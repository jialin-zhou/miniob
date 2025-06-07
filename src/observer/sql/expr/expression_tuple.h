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

#include "common/lang/vector.h"
#include "sql/expr/tuple.h"
#include "common/value.h"
#include "common/sys/rc.h"

#include "sql/expr/expression.h" // 确保 Expression 被包含
#include <memory>              // For std::unique_ptr

template <typename ExprPointerType>
class ExpressionTuple : public Tuple
{
public:
  ExpressionTuple(const vector<ExprPointerType> &expressions) : expressions_(expressions) {}
  virtual ~ExpressionTuple() = default;

    /**
     * @brief 创建并返回当前元组的一个深拷贝副本。
     * @details
     * 由于 ExpressionTuple 是一个基于外部表达式列表的“视图”，
     * 克隆它会将其“实体化”：计算所有表达式的当前值，并存入一个新的 ValueListTuple 对象中。
     * 这个新的 ValueListTuple 对象拥有自己独立的数据副本。
     * @return 指向新创建的 ValueListTuple 的 unique_ptr；如果失败则返回 nullptr。
     */
    std::unique_ptr<Tuple> clone() const override
    {
        // 创建一个新的 ValueListTuple，它将拥有自己的数据
        auto value_list_clone = std::make_unique<ValueListTuple>();
        
        // ValueListTuple 有一个静态方法 make，可以方便地从任何 Tuple 构造。
        // 我们利用它来将当前的 ExpressionTuple (它也是一个 Tuple) 转换为 ValueListTuple。
        // ValueListTuple::make 会遍历所有 cell，调用 cell_at，这会触发我们的 get_value 逻辑。
        RC rc = ValueListTuple::make(*this, *value_list_clone);
        if (OB_FAIL(rc)) {
            LOG_WARN("ExpressionTuple::clone 失败: 无法实体化为 ValueListTuple. rc=%s", strrc(rc));
            return nullptr; // 如果转换失败，返回空指针
        }

        return value_list_clone;
    }

  void set_tuple(const Tuple *tuple) { child_tuple_ = tuple; }

  int cell_num() const override { return static_cast<int>(expressions_.size()); }

  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }

    const ExprPointerType &expression = expressions_[index];
    return get_value(expression, cell);
  }

  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }

    const ExprPointerType &expression = expressions_[index];
    spec                              = TupleCellSpec(expression->name());
    return RC::SUCCESS;
  }

  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = child_tuple_->find_cell(spec, cell);
      if (OB_SUCC(rc)) {
        return rc;
      }
    }

    rc = RC::NOTFOUND;
    for (const ExprPointerType &expression : expressions_) {
      if (0 == strcmp(spec.alias(), expression->name())) {
        rc = get_value(expression, cell);
        break;
      }
    }

    return rc;
  }

private:
  RC get_value(const ExprPointerType &expression, Value &value) const
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = expression->get_value(*child_tuple_, value);
    } else {
      rc = expression->try_get_value(value);
    }
    return rc;
  }

private:
  const vector<ExprPointerType> &expressions_;
  const Tuple                   *child_tuple_ = nullptr;
};
