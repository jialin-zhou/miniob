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

#include "sql/operator/physical_operator.h"
#include "sql/operator/order_by_logical_operator.h" // For BoundOrderByItem
#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include <vector>
#include <memory> // For unique_ptr

class OrderByPhysicalOperator : public PhysicalOperator {
public:
    // Constructor takes the child operator and the sorting criteria
    OrderByPhysicalOperator(std::unique_ptr<PhysicalOperator> child,
                            std::vector<BoundOrderByItem>&& order_by_items);

    ~OrderByPhysicalOperator() override = default;

    PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY_OP; } // Add ORDER_BY_OP to your enum if not present
    OpType get_op_type() const override { return OpType::ORDERBY; } // Matches your existing OpType enum for physical sort

    string name() const override { return "ORDER_BY"; } // Or physical_operator_type_name(type())
    string param() const override; // Will list sorting columns and directions

    RC open(Trx *trx) override;
    RC next() override; // For row-at-a-time
    // RC next(Chunk &chunk) override; // If you plan to support vectorized execution for sort
    RC close() override;

    Tuple *current_tuple() override { return current_output_tuple_.get(); }
    RC tuple_schema(TupleSchema &schema) const override;

private:
    // 此辅助类将封装基于 order_by_items_ 中定义的多个表达式及其排序方向比较两个元组的逻辑。
    class TupleComparator {
        public:
            TupleComparator(const std::vector<BoundOrderByItem>& order_by_items, const TupleSchema& schema);
            bool operator()(const std::unique_ptr<Tuple>& lhs, const std::unique_ptr<Tuple>& rhs) const;
        private:
            const std::vector<BoundOrderByItem>& order_by_items_;
            const TupleSchema& input_schema_; // Schema to help evaluate expressions if needed, or get column indices
    };

    std::unique_ptr<PhysicalOperator> child_operator_;
    std::vector<BoundOrderByItem> order_by_items_; // 存储所有 排序列 和 排序方式

    std::vector<std::unique_ptr<Tuple>> all_tuples_; // 存储所有子节点的元组
    size_t current_tuple_idx_ = 0;                  // Index for iterating over sorted tuples
    std::unique_ptr<Tuple> current_output_tuple_;    // The current tuple to be returned by next()
    TupleSchema child_schema_;                      // Schema from the child operator
    bool opened_ = false;
    Trx* trx_ = nullptr;
};
