/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/table/ob_block_builder.h"
#include "oblsm/util/ob_coding.h"
#include "common/log/log.h"

namespace oceanbase {

void ObBlockBuilder::reset()
{
  offsets_.clear();
  data_.clear();
  // first_key_.clear();
}

// [key_size (4B)][key data][value_size (4B)][value data]
RC ObBlockBuilder::add(const string_view &key, const string_view &value)
{
  RC rc = RC::SUCCESS;
  if (appro_size() + key.size() + value.size() + 2 * sizeof(uint32_t) > BLOCK_SIZE) {
    // TODO: support large kv pair.
    if (offsets_.empty()) {
      LOG_ERROR("block is empty, but kv pair is too large, key size: %lu, value size: %lu", key.size(), value.size());
      return RC::UNIMPLEMENTED;
    }
    LOG_TRACE("block is full, can't add more kv pair");
    rc = RC::FULL;
  } else {
    offsets_.push_back(data_.size()); 
    put_numeric<uint32_t>(&data_, key.size());        // 写入 key 长度（4 字节）
    data_.append(key.data(), key.size());             // 写入 key 内容
    put_numeric<uint32_t>(&data_, value.size());      // 写入 value 长度（4 字节）
    data_.append(value.data(), value.size());         // 写入 value 内容
  }
  return rc;
}

string ObBlockBuilder::last_key() const
{
  string_view last_kv(data_.data() + offsets_.back(), data_.size() - offsets_.back());
  uint32_t    key_length = get_numeric<uint32_t>(last_kv.data());
  return string(last_kv.data() + sizeof(uint32_t), key_length);
}

string_view ObBlockBuilder::finish()
{
  uint32_t data_size = data_.size();
  put_numeric<uint32_t>(&data_, offsets_.size());  // 写入 offset 数量，占4字节
  for (size_t i = 0; i < offsets_.size(); i++) {
    put_numeric<uint32_t>(&data_, offsets_[i]);  // 写入 offset，占4字节
  }
  put_numeric<uint32_t>(&data_, data_size);  // 写入 data 大小，占4字节
  return string_view(data_.data(), data_.size());
}

// 模拟：
// 刚开始：data_ = ""， offsets_ = []
// 第一次插入：add("apple", "red") 

// offsets_ = [0]
// data_ =
//   [05 00 00 00]      ← key_len (4B, value 5)   低地址--->高地址  
//   [61 70 70 6C 65]   ← "apple"                   |
//   [03 00 00 00]      ← val_len (4B, value 3)     |
//   [72 65 64]         ← "red"                   高地址

// 第二次插入：add("banana", "yellow")
// offsets_ = [0, 16]
// data_ =
//   [05 00 00 00]         ← key_len (4B, value 5)
//   [61 70 70 6C 65]      ← "apple"
//   [03 00 00 00]         ← val_len (4B, value 3)
//   [72 65 64]            ← "red" 
//   [06 00 00 00]         ← key_len (4B, value 6)
//   [62 61 6E 61 6E 61]   ← "banana"
//   [06 00 00 00]         ← val_len (4B, value 6)
//   [79 65 6C 6C 6F 77]   ← "yellow"

// 调用 finish()
// put_numeric<uint32_t>(&data_, 2)       // offset count = 2
// put_numeric<uint32_t>(&data_, 0)       // offset[0] = 0
// put_numeric<uint32_t>(&data_, 16)      // offset[1] = 16
// put_numeric<uint32_t>(&data_, 36)      // data_size = 36
// offsets_ = [0, 16]
// data_ =
//   [05 00 00 00]         ← key_len (4B, value 5)
//   [61 70 70 6C 65]      ← "apple"
//   [03 00 00 00]         ← val_len (4B, value 3)
//   [72 65 64]            ← "red" 
//   [06 00 00 00]         ← key_len (4B, value 6)
//   [62 61 6E 61 6E 61]   ← "banana"
//   [06 00 00 00]         ← val_len (4B, value 6)
//   [79 65 6C 6C 6F 77]   ← "yellow"
//   [02 00 00 00]         ← offset count = 2 (4B, value 2)
//   [00 00 00 00]         ← offset[0] = 0    (4B, value 0)
//   [10 00 00 00]         ← offset[1] = 16   (4B, value 16)
//   [24 00 00 00]         ← data_size = 36   (4B, value 36)



//  总计  1024KB，一个block块
//     4字节    |  key |    4字节  |  val
// ┌─────────────────────────────────────┐
// │ key1_size | key1 | val1_size | val1 │ ← data_ 的前半段
// ├─────────────────────────────────────┤
// │ key2_size | key2 | val2_size | val2 │
// ├─────────────────────────────────────┤
// │ ...                                 │
// ├─────────────────────────────────────┤
// │         N = offset 数 （4字节）      │ 
// │             offset_1 （4字节）       │
// |              ...     （4字节）       |
// |            offset_N  （4字节）       │ ← 这段从 finish() 追加上来的 offsets_
// │  offset start (data_size) （4字节）  │ ← 用来定位 offset 区域的起点
// └─────────────────────────────────────┘


}  // namespace oceanbase
