/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/table/ob_sstable_builder.h"
#include "oblsm/include/ob_lsm_iterator.h"
#include "oblsm/table/ob_block_builder.h"
#include "oblsm/table/ob_sstable.h"
#include "oblsm/util/ob_coding.h"
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string_view>

namespace oceanbase {

// 构建 SSTable 文件
RC ObSSTableBuilder::build(shared_ptr<ObMemTable> mem_table, const std::string &file_name, uint32_t sst_id)
{
  // printf("[Build] 开始构建 SSTable 文件: %s, sst_id=%u\n", file_name.c_str(), sst_id);
  sst_id_ = sst_id;

  // 删除旧文件，防止残留数据
  if (std::filesystem::exists(file_name)) {
    std::filesystem::remove(file_name);
  }

  // 初始化写入器
  file_writer_ = make_unique<ObFileWriter>(file_name);
  file_writer_->open_file();
  if (!file_writer_->is_open()) {
    LOG_ERROR("无法打开文件进行写入: %s", file_name.c_str());
    return RC::FILE_NOT_OPENED;
  }

  // 构建迭代器并遍历 memtable
  unique_ptr<ObLsmIterator> iter(mem_table->new_iterator());
  iter->seek_to_first();

  if (!iter->valid()) {
    LOG_WARN("MemTable 为空，SSTable 只包含空的元数据");
    write_metadata();
    string footer_buf;
    put_numeric<uint32_t>(&footer_buf, metadata_start_offset_);
    file_writer_->write(footer_buf);
    curr_offset_ += footer_buf.size();
    file_writer_->flush();
    file_writer_->close_file();
    return RC::SUCCESS;
  }

  RC rc = RC::SUCCESS;
  for (; iter->valid(); iter->next()) {
    string_view current_internal_key = iter->key(); // iter->key() IS the internal_key
    string_view current_value = iter->value();

    // 校验 internal_key 的大小
    if (current_internal_key.size() < SEQ_SIZE) {
        LOG_ERROR("ObSSTableBuilder: Corrupted internal key from memtable iterator: size %zu is less than SEQ_SIZE %zu. SST ID: %u. Skipping record.",
                  current_internal_key.size(), static_cast<size_t>(SEQ_SIZE), sst_id_);
        // Or return an error to stop SSTable creation if data is too corrupted
        return RC::CORRUPTED_DATA;
    }

    string_view user_key_for_first_key = extract_user_key(current_internal_key);

    if (block_builder_.appro_size() == 0) {
      // curr_blk_first_key_ 存储的是 user_key
      curr_blk_first_key_.assign(user_key_for_first_key.data(), user_key_for_first_key.length());
    }

    // Pass the full internal_key to block_builder_.add
    rc = block_builder_.add(current_internal_key, current_value);
    if (rc == RC::FULL) {
      finish_build_block(); // last_key in finish_build_block will be based on the last internal_key added
      // curr_blk_first_key_ for the new block
      curr_blk_first_key_.assign(user_key_for_first_key.data(), user_key_for_first_key.length());
      // Add the current entry to the new block
      rc = block_builder_.add(current_internal_key, current_value);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("Block已满后插入失败. InternalKey: %.*s, RC: %d", static_cast<int>(current_internal_key.size()), current_internal_key.data(), rc);
        return rc;
      }
    } else if (rc != RC::SUCCESS) {
      LOG_ERROR("添加Entry失败. InternalKey: %.*s, RC: %d", static_cast<int>(current_internal_key.size()), current_internal_key.data(), rc);
      return rc;
    }
  }

  if (block_builder_.appro_size() > 0) {
    finish_build_block();
  }

  write_metadata();

  // 写入 footer
  string footer_buf;
  put_numeric<uint32_t>(&footer_buf, metadata_start_offset_);
  file_writer_->write(footer_buf);
  curr_offset_ += footer_buf.size();

  file_writer_->flush();
  file_writer_->close_file();

  file_size_ = std::filesystem::file_size(file_name);
  return RC::SUCCESS;
}

// 写入元数据信息
void ObSSTableBuilder::write_metadata()
{
  metadata_start_offset_ = curr_offset_;

  uint32_t meta_count = block_metas_.size();
  string meta_count_buf;
  put_numeric<uint32_t>(&meta_count_buf, meta_count);
  file_writer_->write(meta_count_buf);
  curr_offset_ += meta_count_buf.size();

  for (size_t i = 0; i < block_metas_.size(); ++i) {
    const auto &meta = block_metas_[i];
    string meta_str = meta.encode();
    string size_buf;
    put_numeric<uint32_t>(&size_buf, meta_str.size());
    file_writer_->write(size_buf);
    curr_offset_ += size_buf.size();
    file_writer_->write(meta_str);
    curr_offset_ += meta_str.size();
  }
}

// 写入一个完整的 block 数据和 padding
void ObSSTableBuilder::finish_build_block()
{
  string internal_last_key = block_builder_.last_key();
  // Ensure last_key in BlockMeta is a user_key for consistency
  string user_last_key = string(extract_user_key(internal_last_key)); 
  string_view block_contents = block_builder_.finish();
  uint32_t block_size = block_contents.size();

  file_writer_->write(block_contents);
  block_metas_.push_back(BlockMeta(curr_blk_first_key_, user_last_key, curr_offset_, block_size));
  curr_offset_ += block_size;

  // 添加 padding 以对齐
  uint32_t padding_size = 0;
  if (block_size % BLOCK_SIZE != 0) {
    uint32_t aligned_size = (block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1);
    padding_size = aligned_size - block_size;
    string padding_buf(padding_size, '\0');
    file_writer_->write(padding_buf);
    curr_offset_ += padding_size;
  }

  block_builder_.reset();
}

// 构造已完成的 SSTable 对象
shared_ptr<ObSSTable> ObSSTableBuilder::get_built_table()
{
  shared_ptr<ObSSTable> sstable = make_shared<ObSSTable>(sst_id_, file_writer_->file_name(), comparator_, block_cache_);
  sstable->init();
  return sstable;
}

// 重置构建器状态
void ObSSTableBuilder::reset()
{
  block_builder_.reset();
  curr_blk_first_key_.clear();
  if (file_writer_ != nullptr) {
    file_writer_.reset(nullptr);
  }
  block_metas_.clear();
  curr_offset_ = 0;
  sst_id_ = 0;
  file_size_ = 0;
}

}  // namespace oceanbase
