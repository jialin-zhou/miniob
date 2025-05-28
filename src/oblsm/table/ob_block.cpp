/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/table/ob_block.h"
#include "oblsm/util/ob_coding.h"
#include "common/lang/memory.h"
#include <cstdint>
#include <string_view>
#include <vector>

namespace oceanbase {

// 解码 Block 数据内容（含 offset 区域）
RC ObBlock::decode(const string &data)
{
  uint32_t len = data.size();
  if (len < sizeof(uint32_t)) {
    printf("解码失败：block 长度过小 (len=%u)\n", len);
    return RC::FILE_CORRUPTED;
  }

  uint32_t offset_start = get_numeric<uint32_t>(data.data() + len - sizeof(uint32_t));
  if (offset_start >= len || offset_start + sizeof(uint32_t) > len) {
    printf("解码失败：offset_start 超出范围 (offset_start=%u, len=%u)\n", offset_start, len);
    return RC::FILE_CORRUPTED;
  }

  uint32_t offset_count = get_numeric<uint32_t>(data.data() + offset_start);
  size_t expected_offset_size = offset_start + sizeof(uint32_t) + offset_count * sizeof(uint32_t);
  if (expected_offset_size > len) {
    printf("解码失败：offset 区域超出 block 范围 (offset_count=%u, 期望=%zu, len=%u)\n",
           offset_count, expected_offset_size, len);
    return RC::FILE_CORRUPTED;
  }

  const char *offsets_base = data.data() + offset_start + sizeof(uint32_t);
  offsets_.clear();
  for (uint32_t i = 0; i < offset_count; i++) {
    uint32_t offset = get_numeric<uint32_t>(offsets_base + i * sizeof(uint32_t));
    offsets_.push_back(offset);
  }

  data_ = data.substr(0, offset_start);
  return RC::SUCCESS;
}

// 获取指定 entry 的数据视图
string_view ObBlock::get_entry(uint32_t offset) const
{
  uint32_t curr_begin = offsets_[offset];
  uint32_t curr_end = offset == offsets_.size() - 1 ? data_.size() : offsets_[offset + 1];
  return string_view(data_.data() + curr_begin, curr_end - curr_begin);
}

// 构造一个 Block 内部迭代器
ObLsmIterator *ObBlock::new_iterator() const { return new BlockIterator(comparator_, this, size()); }

// 解析当前 entry 的 key 和 value
void BlockIterator::parse_entry()
{
  curr_entry_ = data_->get_entry(index_);
  uint32_t key_size = get_numeric<uint32_t>(curr_entry_.data());
  key_ = string_view(curr_entry_.data() + sizeof(uint32_t), key_size);
  uint32_t value_size = get_numeric<uint32_t>(curr_entry_.data() + sizeof(uint32_t) + key_size);
  value_ = string_view(curr_entry_.data() + 2 * sizeof(uint32_t) + key_size, value_size);
}

// 编码 BlockMeta
string BlockMeta::encode() const
{
  string ret;
  put_numeric<uint32_t>(&ret, first_key_.size());
  ret.append(first_key_);
  put_numeric<uint32_t>(&ret, last_key_.size());
  ret.append(last_key_);
  put_numeric<uint32_t>(&ret, offset_);
  put_numeric<uint32_t>(&ret, size_);
  return ret;
}

// 解码 BlockMeta
RC BlockMeta::decode(const string &data)
{
  RC rc = RC::SUCCESS;
  const char *data_ptr = data.c_str();
  uint32_t first_key_size = get_numeric<uint32_t>(data_ptr);
  data_ptr += sizeof(uint32_t);
  first_key_.assign(data_ptr, first_key_size);
  data_ptr += first_key_size;

  uint32_t last_key_size = get_numeric<uint32_t>(data_ptr);
  data_ptr += sizeof(uint32_t);
  last_key_.assign(data_ptr, last_key_size);
  data_ptr += last_key_size;

  offset_ = get_numeric<uint32_t>(data_ptr);
  data_ptr += sizeof(uint32_t);
  size_ = get_numeric<uint32_t>(data_ptr);
  return rc;
}

// Block 内查找指定 key 的位置
void BlockIterator::seek(const string_view &lookup_key)
{
  index_ = 0;
  while(valid()) {
    parse_entry();
    if (comparator_->compare(extract_user_key(key_), extract_user_key_from_lookup_key(lookup_key)) >= 0) {
      break;
    }
    index_++;
  }
}

}  // namespace oceanbase
