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
// Created by WangYunlai on 2024/05/29.
//

#pragma once

#include "sql/operator/logical_operator.h"

// 不仅需要存储排序列 Expression expr_，还需要存储排序方向 bool is_asc_
struct BoundOrderByItem {
    std::unique_ptr<Expression> expr_;
    bool is_asc_;
    // 可以有构造函数等
    BoundOrderByItem(std::unique_ptr<Expression> e, bool asc)
        : expr_(std::move(e)), is_asc_(asc) {}
};

class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator(std::vector<BoundOrderByItem>&& items);

  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ORDER_BY; }
  OpType              get_op_type() const override { return OpType::LOGICALORDERBY; }

  const std::vector<BoundOrderByItem>& get_order_by_items() const { return order_by_items_; }

private:
  std::vector<BoundOrderByItem> order_by_items_;
};
