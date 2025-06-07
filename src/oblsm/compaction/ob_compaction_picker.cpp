/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/compaction/ob_compaction_picker.h"
#include "common/log/log.h"
#include "oblsm/include/ob_lsm_options.h"
#include "oblsm/table/ob_sstable.h"
#include <memory>
#include <vector>
#include <cmath> // For std::pow

namespace oceanbase {

/**
 * @brief 根据分级合并策略（Tiered Compaction）挑选一个合并任务。
 *
 * 策略逻辑：
 * 1. 从最旧（或索引最小）的层级（Tier）开始遍历。
 * 2. 检查当前层级中的 SSTable 文件数量是否达到了触发合并的阈值（options_->default_run_num）。
 * 3. 如果达到阈值，则选择该层级的所有 SSTable 进行合并。
 * 4. 将这些选中的 SSTable 打包成一个 ObCompaction 任务并返回。
 * 5. 为了避免一次性执行过多的合并任务，函数在找到第一个满足条件的层级后就会立即返回。
 * 6. 如果遍历完所有层级都没有找到需要合并的，则返回 nullptr。
 *
 * @param sstables 指向所有层级及其 SSTable 列表的指针。
 * @return 一个指向具体合并任务的 unique_ptr，如果无需合并则为 nullptr。
 */
// TODO: put it in options
unique_ptr<ObCompaction> TiredCompactionPicker::pick(SSTablesPtr sstables)
{
    if (!sstables || sstables->empty()) {
        return nullptr;
    }
    for (size_t i = 0; i < sstables->size(); ++i) {
        const auto& current_tier = (*sstables)[i];
        // 检查当前层级的文件数是否达到触发合并的阈值
        if (current_tier.size() >= options_->default_run_num) {
            // 找到了需要合并的层级，创建一个合并任务
            // 任务的 level 设置为当前层级的索引 i
            unique_ptr<ObCompaction> compaction(new ObCompaction(i));
            // 将该层级的所有 SSTable 添加到合并任务的输入中
            for (const auto& sstable : current_tier) {
                compaction->inputs_[0].emplace_back(sstable);
            }
            // 返回构建好的合并任务，本次 pick 结束
            // LOG_DEBUG("Picked tier %zu for compaction with %zu sstables.", i, current_tier.size());
            return compaction;
        }
    }
    return nullptr;
}

/**
 * @brief 检查两个SSTable的键范围是否存在重叠。
 * @param a 指向第一个SSTable的共享指针。
 * @param b 指向第二个SSTable的共享指针。
 * @return 如果键范围有重叠，则返回true；否则返回false。
 */
bool DoRangesOverlap(const shared_ptr<ObSSTable>& a, const shared_ptr<ObSSTable>& b) {
    // 处理任一SSTable为空的情况
    if (!a || !b) {
        return false;
    }
    // !(a在b之前 || b在a之前)
    return !(a->last_key() < b->first_key() || b->last_key() < a->first_key());
}

/**
 * @brief 根据分层合并策略（Leveled Compaction）挑选一个合并任务。
 *
 * 策略逻辑：
 * 1.  **L0 -> L1 的合并**:
 * - 这是最高优先级的合并。
 * - 触发条件：当 Level 0 的文件数量达到阈值 (default_l0_file_num)。
 * - 动作：将 L0 的所有文件作为 input[0]，并找出 L1 中所有与 L0 文件键范围有重叠的文件作为 input[1]。
 *
 * 2.  **Li -> L(i+1) 的合并 (i > 0)**:
 * - 触发条件：当 Level i 的总数据大小超过了该层的阈值。
 * - L1阈值：default_l1_level_size。
 * - Li阈值：L(i-1)阈值 * default_level_ratio。
 * - 动作：从 Level i 中选择一个文件（通常是第一个或根据特定策略选择），并找出 Level i+1 中所有与之键范围重叠的文件，分别作为 input[0] 和 input[1]。
 *
 * 3.  函数在找到第一个需要合并的任务后就会立即返回。
 *
 * @param sstables 指向所有层级及其 SSTable 列表的指针。
 * @return 一个指向具体合并任务的 unique_ptr，如果无需合并则为 nullptr。
 */
unique_ptr<ObCompaction> LeveledCompactionPicker::pick(SSTablesPtr sstables)
{
    if (!sstables || sstables->empty()) {
        return nullptr;
    }
    // --- 1. 检查 L0 -> L1 的合并 ---
    const auto& level0_files = (*sstables)[0];
    if (level0_files.size() >= options_->default_l0_file_num) {
        unique_ptr<ObCompaction> compaction(new ObCompaction(0)); // L0的合并
        
        // input[0] 是 L0 的所有文件
        compaction->inputs_[0] = level0_files;

        // input[1] 是 L1 中与 L0 文件有重叠的所有文件
        if (sstables->size() > 1) {
            const auto& level1_files = (*sstables)[1];
            for (const auto& l1_sst : level1_files) {
                for (const auto& l0_sst : level0_files) {
                    if (DoRangesOverlap(l0_sst, l1_sst)) {
                        compaction->inputs_[1].emplace_back(l1_sst);
                        break; // 找到重叠后，这个l1_sst已加入，检查下一个l1_sst
                    }
                }
            }
        }
        return compaction;
    }

    // --- 2. 检查 Li -> L(i+1) 的合并 (i >= 1) ---
    for (size_t i = 1; i < sstables->size() - 1; ++i) {
        const auto& current_level_files = (*sstables)[i];
        if (current_level_files.empty()) {
            continue; // 当前层没有文件，跳过
        }
        
        // 计算当前层的总大小
        uint64_t current_level_size = 0;
        for (const auto& sst : current_level_files) {
            current_level_size += sst->size();
        }
        
        // 计算当前层的大小阈值
        uint64_t level_size_limit = options_->default_l1_level_size * std::pow(options_->default_level_ratio, i - 1);

        if (current_level_size > level_size_limit) {
            // 触发合并，选择当前层的第一个文件作为源
            // 更优的策略是选择修改最少或最老的文件，这里简化为第一个
            shared_ptr<ObSSTable> source_sst = current_level_files[0];
            
            unique_ptr<ObCompaction> compaction(new ObCompaction(i));
            compaction->inputs_[0].emplace_back(source_sst);

            // 找出下一层 (i+1) 中所有与 source_sst 重叠的文件
            const auto& next_level_files = (*sstables)[i + 1];
            for (const auto& next_sst : next_level_files) {
                if (DoRangesOverlap(source_sst, next_sst)) {
                    compaction->inputs_[1].emplace_back(next_sst);
                }
            }
            return compaction;
        }
    }
    return nullptr;
}

ObCompactionPicker *ObCompactionPicker::create(CompactionType type, ObLsmOptions *options)
{

  switch (type) {
    case CompactionType::TIRED: return new TiredCompactionPicker(options);
    case CompactionType::LEVELED: return new LeveledCompactionPicker(options);
    default: return nullptr;
  }
  return nullptr;
}

}  // namespace oceanbase