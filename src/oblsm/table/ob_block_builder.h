/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/lang/string.h"
#include "common/lang/string_view.h"
#include "common/lang/vector.h"
#include "common/sys/rc.h"

namespace oceanbase {

/**
 * @brief Build a ObBlock in SSTable
 */
class ObBlockBuilder
{

public:
  RC add(const string_view &key, const string_view &value);

  string_view finish();

  void reset();

  string last_key() const;

  uint32_t appro_size() { return data_.size() + offsets_.size() * sizeof(uint32_t); }

private:
  // 表示一个block的大小
  static const uint32_t BLOCK_SIZE = 4 * 1024;  // 4KB
  // Offsets of key-value pairs.
  // 一个vector<uint32_t> 表示当前对象的offset
  vector<uint32_t> offsets_;
  // key-value pairs
  // TODO: use block as data container
  // TODO: add checksum
  // 一个string 表示当前对象的data
  string data_;

  // string first_key_;
};

}  // namespace oceanbase
