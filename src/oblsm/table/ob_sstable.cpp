/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/table/ob_sstable.h"
#include "common/sys/rc.h"
#include "oblsm/table/ob_block.h"
#include "oblsm/util/ob_coding.h"
#include "common/log/log.h"
#include "common/lang/filesystem.h"
#include <cstdint>
#include <memory>
#include <string_view>
namespace oceanbase {

// 初始化 SSTable，从文件中加载 footer 和 block meta 信息
void ObSSTable::init()
{
  printf("[init] 加载 SSTable 文件: %s\n", file_name_.c_str());

  file_reader_ = make_unique<ObFileReader>(file_name_);
  file_reader_->open_file();

  uint32_t file_size = file_reader_->file_size();
  if (file_size < sizeof(uint32_t)) {
    printf("[init] 错误：SSTable 文件过小，无法读取 footer\n");
    return;
  }

  string footer_str = file_reader_->read_pos(file_size - sizeof(uint32_t), sizeof(uint32_t));
  uint32_t meta_offset = get_numeric<uint32_t>(footer_str.data());

  if (meta_offset >= file_size) {
    printf("[init] 错误：meta_offset 越界 (meta_offset=%u, file_size=%u)\n", meta_offset, file_size);
    return;
  }

  string meta_size_str = file_reader_->read_pos(meta_offset, sizeof(uint32_t));
  uint32_t meta_size = get_numeric<uint32_t>(meta_size_str.data());

  uint32_t current_pos = meta_offset + sizeof(uint32_t);
  for (uint32_t i = 0; i < meta_size; i++) {
    string block_meta_size_str = file_reader_->read_pos(current_pos, sizeof(uint32_t));
    if (block_meta_size_str.size() != sizeof(uint32_t)) {
      printf("[init] 错误：无法读取 block meta size (pos=%u)\n", current_pos);
      return;
    }
    uint32_t block_meta_size = get_numeric<uint32_t>(block_meta_size_str.data());
    current_pos += sizeof(uint32_t);

    string block_meta_str = file_reader_->read_pos(current_pos, block_meta_size);
    if (block_meta_str.size() != block_meta_size) {
      printf("[init] 错误：无法读取 block meta 数据 (pos=%u)\n", current_pos);
      return;
    }
    current_pos += block_meta_size;

    BlockMeta block_meta;
    RC rc = block_meta.decode(block_meta_str);
    if (rc != RC::SUCCESS) {
      printf("[init] 错误：解析 block meta 失败 (index=%u)\n", i);
      return;
    }

    block_metas_.push_back(block_meta);
  }

  printf("[init] 成功加载 %zu 个 block metas\n", block_metas_.size());
}

// 从缓存或文件中读取指定的 Block
shared_ptr<ObBlock> ObSSTable::read_block_with_cache(uint32_t block_idx) const
{
  if (block_idx >= block_metas_.size()) {
    LOG_ERROR("block index 无效: %u, 总数: %u", block_idx, block_metas_.size());
    return nullptr;
  }

  uint64_t cache_key = (static_cast<uint64_t>(sst_id_) << 32) | static_cast<uint64_t>(block_idx);
  shared_ptr<ObBlock> block = std::make_shared<ObBlock>(comparator_);

  if (block_cache_ != nullptr) {
    if (block_cache_->get(cache_key, block)) {
      return block;
    }
  }

  block = read_block(block_idx);
  if (block && block_cache_ != nullptr) {
    block_cache_->put(cache_key, block);
  }
  return block;
}

// 从文件读取指定的 Block
shared_ptr<ObBlock> ObSSTable::read_block(uint32_t block_idx) const
{
  if (block_idx >= block_metas_.size()) {
    LOG_ERROR("block index 无效: %u, 总数: %u", block_idx, block_metas_.size());
    return nullptr;
  }

  const BlockMeta& meta = block_metas_[block_idx];
  string block_data = file_reader_->read_pos(meta.offset_, meta.size_);

  shared_ptr<ObBlock> block = std::make_shared<ObBlock>(comparator_);
  RC rc = block->decode(block_data);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Block 解码失败: index=%u", block_idx);
    return nullptr;
  }
  return block;
}

void ObSSTable::remove() { filesystem::remove(file_name_); }

ObLsmIterator *ObSSTable::new_iterator() { return new TableIterator(get_shared_ptr()); }

// 读取当前 block 并初始化迭代器
void TableIterator::read_block_with_cache()
{
  block_ = sst_->read_block_with_cache(curr_block_idx_);
  if (block_ == nullptr) {
    LOG_ERROR("加载 block 失败: index=%u", curr_block_idx_);
    block_iterator_ = nullptr;
    return;
  }
  block_iterator_.reset(block_->new_iterator());
}

// 定位到第一个 block 的起始位置
void TableIterator::seek_to_first()
{
  curr_block_idx_ = 0;
  read_block_with_cache();
  if (block_iterator_) {
    block_iterator_->seek_to_first();
  }
}

// 定位到最后一个 block 的最后一条 entry
void TableIterator::seek_to_last()
{
  curr_block_idx_ = block_cnt_ - 1;
  read_block_with_cache();
  if (block_iterator_) {
    block_iterator_->seek_to_last();
  }
}

// 移动到下一条 entry
void TableIterator::next()
{
  block_iterator_->next();
  if (!block_iterator_->valid() && curr_block_idx_ < block_cnt_ - 1) {
    curr_block_idx_++;
    read_block_with_cache();
    if (block_iterator_) {
      block_iterator_->seek_to_first();
    }
  }
}

// 根据 lookup_key 定位到对应的 block 并初始化迭代器
void TableIterator::seek(const string_view &lookup_key)
{
  uint32_t left = 0;
  uint32_t right = block_cnt_ - 1;
  while (left <= right) {
    uint32_t mid = left + (right - left) / 2;
    const auto &block_meta = sst_->block_meta(mid);
    if (sst_->comparator()->compare(
          extract_user_key(block_meta.last_key_),
          extract_user_key_from_lookup_key(lookup_key)) >= 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  if (left == block_cnt_) {
    block_iterator_ = nullptr;
    return;
  }
  curr_block_idx_ = left;
  read_block_with_cache();
  if (block_iterator_) {
    block_iterator_->seek(lookup_key);
  }
}

}  // namespace oceanbase
