/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2024/5/31.
//

#pragma once

#include "sql/expr/tuple.h"
#include <vector>
#include <memory> // For std::unique_ptr

/**
 * @brief 组合的Tuple
 * @ingroup Tuple
 * TODO 单元测试
 */
class CompositeTuple : public Tuple
{
public:
  CompositeTuple()          = default;
  virtual ~CompositeTuple() = default;

      /**
     * @brief 创建并返回当前 CompositeTuple 的一个深拷贝副本。
     * @details
     * CompositeTuple 内部包含一个 Tuple 列表。克隆时，需要对这个列表中的
     * 每一个 Tuple 对象也进行克隆，以创建完全独立的副本。
     * @return 指向新创建的元组副本的 unique_ptr；如果失败则返回 nullptr。
     */
    std::unique_ptr<Tuple> clone() const override {
        // 创建一个新的 CompositeTuple 对象
        auto cloned_composite = std::make_unique<CompositeTuple>();
        
        // CompositeTuple 的核心是其内部的元组列表（假设名为 tuples_）。
        // 假设成员是: std::vector<std::unique_ptr<Tuple>> tuples_;
        cloned_composite->tuples_.reserve(this->tuples_.size());
        for (const auto& child_tuple_ptr : this->tuples_) {
            if (child_tuple_ptr) {
                cloned_composite->add_tuple(child_tuple_ptr->clone());
            } else {
                cloned_composite->add_tuple(nullptr);
            }
        }
        return cloned_composite;
    }

  /// @brief 删除默认构造函数
  CompositeTuple(const CompositeTuple &) = delete;
  /// @brief 删除默认赋值函数
  CompositeTuple &operator=(const CompositeTuple &) = delete;

  /// @brief 保留移动构造函数
  CompositeTuple(CompositeTuple &&) = default;
  /// @brief 保留移动赋值函数
  CompositeTuple &operator=(CompositeTuple &&) = default;

  int cell_num() const override;
  RC  cell_at(int index, Value &cell) const override;
  RC  spec_at(int index, TupleCellSpec &spec) const override;
  RC  find_cell(const TupleCellSpec &spec, Value &cell) const override;

  void   add_tuple(unique_ptr<Tuple> tuple);
  Tuple &tuple_at(size_t index);

private:
  vector<unique_ptr<Tuple>> tuples_;
};
