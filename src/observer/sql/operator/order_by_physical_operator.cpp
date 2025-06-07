#include "sql/operator/order_by_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include <algorithm>

using namespace std;
using namespace common;

// Constructor for OrderByPhysicalOperator
OrderByPhysicalOperator::OrderByPhysicalOperator(
    std::unique_ptr<PhysicalOperator> child,
    std::vector<BoundOrderByItem>&& order_by_items)
    : child_operator_(std::move(child)),
      order_by_items_(std::move(order_by_items)) {
    ASSERT(child_operator_ != nullptr, "Child operator cannot be null for OrderBy");
}

// 生成操作符参数的字符串表示，通常用于 EXPLAIN 输出
string OrderByPhysicalOperator::param() const {
    string params_str;
    for (size_t i = 0; i < order_by_items_.size(); ++i) {
        if (order_by_items_[i].expr_) {
            // 假设 Expression 类有 name() 方法返回其名称或描述
            params_str += order_by_items_[i].expr_->name();
        } else {
            params_str += "未知表达式"; // 防御性代码，正常情况下 expr_ 不应为空
        }
        params_str += (order_by_items_[i].is_asc_ ? " ASC" : " DESC"); // 添加排序方向
        if (i < order_by_items_.size() - 1) {
            params_str += ", "; // 如果不是最后一项，则添加逗号分隔符
        }
    }
    return params_str;
}

RC OrderByPhysicalOperator::tuple_schema(TupleSchema &schema) const {
    if (!opened_ && child_operator_ == nullptr) {
         LOG_WARN("Operator not opened or child is null, cannot get schema");
         return RC::SCHEMA_CANNOT_GET;
    }
    // 排序操作不改变元组的模式，因此直接返回（或代理获取）子算子的模式
    return child_operator_->tuple_schema(schema);
}


// 构造函数，接收排序项列表和元组的模式 (Schema)
OrderByPhysicalOperator::TupleComparator::TupleComparator(
    const std::vector<BoundOrderByItem>& items,
    const TupleSchema& schema)
    : order_by_items_(items), input_schema_(schema) {}

// 比较操作符，用于 std::sort
bool OrderByPhysicalOperator::TupleComparator::operator()(
    const std::unique_ptr<Tuple>& lhs, // 左侧比较元组的智能指针
    const std::unique_ptr<Tuple>& rhs) const { // 右侧比较元组的智能指针

    // 遍历所有排序项（例如 ORDER BY col1 ASC, col2 DESC 中的 col1 ASC 和 col2 DESC）
    for (const auto& item : order_by_items_) {
        // 检查表达式、左元组或右元组是否为空指针
        if (!item.expr_ || !lhs || !rhs) {
            LOG_TRACE("比较器中遇到空的表达式或元组，无法比较");
            // 定义对空值的统一处理行为，例如视为相等或抛出异常
            return false; // 或者其他默认行为，当前行为可能导致排序不稳定
        }

        Value lhs_val; // 用于存储左侧元组当前排序键的表达式计算结果
        Value rhs_val; // 用于存储右侧元组当前排序键的表达式计算结果
        RC rc;

        // --- 对左侧元组计算当前排序键的表达式值 ---
        // 注意：Expression::get_value 方法签名是 get_value(const Tuple &tuple, Value &value) const
        // lhs.get() 返回 Tuple*，*lhs.get() 得到 Tuple&，可以传递给 const Tuple&
        rc = item.expr_->get_value(*lhs.get(), lhs_val); // 使用 get_value 替换 evaluate
        if (OB_FAIL(rc)) {
            // 使用 expr_->name() 获取表达式名称是正确的
            LOG_WARN("计算左侧元组表达式失败: %s, rc=%s", item.expr_->name(), strrc(rc));
            return false; // 比较中发生错误，可能导致排序结果不确定
        }

        // --- 对右侧元组计算当前排序键的表达式值 ---
        rc = item.expr_->get_value(*rhs.get(), rhs_val); // 使用 get_value 替换 evaluate
        if (OB_FAIL(rc)) {
            LOG_WARN("计算右侧元组表达式失败: %s, rc=%s", item.expr_->name(), strrc(rc));
            return false; // 比较中发生错误，可能导致排序结果不确定
        }

        // --- 处理 NULL 值比较 ---
        // 假设 Value 类实现了 is_null() 方法。如果 Value 类中没有 is_null()，
        // 你需要根据 Value 的设计来实现一个等效的判断，例如检查特定的 attr_type_。
        bool lhs_is_null = lhs_val.is_null(); // 假设的 is_null() 方法
        bool rhs_is_null = rhs_val.is_null(); // 假设的 is_null() 方法

        if (lhs_is_null && rhs_is_null) {
            continue; // 当前排序键两者都为NULL，视为相等，继续比较下一个排序键
        }
        if (lhs_is_null) {
            // 左值为NULL，右值非NULL
            // SQL标准中NULL的排序行为：通常认为NULL大于任何非NULL值。
            // ASC排序时，NULLS LAST (NULL在后) -> lhs > rhs -> 返回 false (lhs不应该排在rhs前面)
            // DESC排序时，NULLS FIRST (NULL在前) -> lhs < rhs -> 返回 true (lhs应该排在rhs前面)
            return item.is_asc_ ? false : true;
        }
        if (rhs_is_null) {
            // 左值非NULL，右值为NULL
            // ASC排序时，NULLS LAST (NULL在后) -> lhs < rhs -> 返回 true (lhs应该排在rhs前面)
            // DESC排序时，NULLS FIRST (NULL在前) -> lhs > rhs -> 返回 false (lhs不应该排在rhs前面)
            return item.is_asc_ ? true : false;
        }

        // --- 非 NULL 值比较 ---
        // Value::compare(const Value &other) const 方法用于比较
        int cmp = lhs_val.compare(rhs_val); // 调用 Value 类中定义的 compare 方法

        if (cmp < 0) { // lhs_val < rhs_val
            return item.is_asc_ ? true : false; // ASC: lhs应在前 (true)；DESC: lhs应在后 (false)
        }
        if (cmp > 0) { // lhs_val > rhs_val
            return item.is_asc_ ? false : true; // ASC: lhs应在后 (false)；DESC: lhs应在前 (true)
        }
        // 如果当前排序键的值相等 (cmp == 0)，则继续比较下一个排序键
    }
    return false; // 所有排序键都相等，保持原有相对顺序（对于std::sort，这意味着它们等价）
}

// 打开 OrderByPhysicalOperator 算子
RC OrderByPhysicalOperator::open(Trx *trx) {
    if (opened_) {
        LOG_INFO("OrderByPhysicalOperator already opened");
        return RC::SUCCESS;
    }
    trx_ = trx;
    LOG_TRACE("Opening OrderByPhysicalOperator");
    RC rc = child_operator_->open(trx_);
    if (OB_FAIL(rc)) {
        LOG_WARN("Failed to open child operator. rc=%s", strrc(rc));
        return rc;
    }

    // 获取并存储子算子的元组模式 (schema)
    rc = child_operator_->tuple_schema(child_schema_);
    if (OB_FAIL(rc)) {
        LOG_WARN("Failed to get child tuple schema. rc=%s", strrc(rc));
        child_operator_->close();
        return rc;
    }

    // 清空内部元组缓冲区，准备接收新数据
    all_tuples_.clear();
    while (true) {
        rc = child_operator_->next();
        if (rc == RC::RECORD_NOT_EXIST) {
            rc = RC::SUCCESS;
            break;
        }
        if (OB_FAIL(rc)) {
            LOG_WARN("Child operator failed during next(). rc=%s", strrc(rc));
            child_operator_->close();
            return rc;
        }
        Tuple *child_tuple_ptr = child_operator_->current_tuple();
        if (child_tuple_ptr == nullptr) {
            LOG_WARN("Child operator returned success but null tuple.");
            child_operator_->close();
            return RC::OPERATOR_NULL_PTR;
        }
        // **重要**: 需要对从子算子获取的元组进行深拷贝。
        // 因为子算子在下一次调用 next() 时可能会重用其内部的元组内存。
        // 必须确保 all_tuples_ 中存储的是独立的元组副本。
        // 你已经在 Tuple 基类和所有子类中声明和实现了 clone() 方法。
        std::unique_ptr<Tuple> cloned_tuple = child_tuple_ptr->clone(); 
        if (!cloned_tuple) {
             LOG_WARN("Failed to clone tuple from child operator.");
             child_operator_->close();
             return RC::OPERATOR_FAILED_CLONE;
        }
        all_tuples_.emplace_back(std::move(cloned_tuple));
    }

    // 对收集到的所有元组进行排序
    if (!all_tuples_.empty()) {
        LOG_TRACE("Sorting %zu tuples", all_tuples_.size());
        TupleComparator comparator(order_by_items_, child_schema_);
        std::sort(all_tuples_.begin(), all_tuples_.end(), comparator);
    }

    current_tuple_idx_ = 0;
    opened_ = true;
    LOG_TRACE("OrderByPhysicalOperator opened successfully with %zu tuples buffered and sorted.", all_tuples_.size());
    return RC::SUCCESS;
}

// 获取下一条排序后的元组
RC OrderByPhysicalOperator::next() {
    if (!opened_) {
        LOG_WARN("Operator not opened");
        return RC::OPERATOR_NOT_OPENNED;
    }

    if (current_tuple_idx_ >= all_tuples_.size()) {
        current_output_tuple_.reset(); // No more tuples
        return RC::NO_MORE_TUPLE;
    }

    // 从 all_tuples_ 缓冲区中获取当前索引指向的元组，并为其创建一个克隆副本
    // 作为本次 next() 调用的输出。
    // PhysicalOperator::current_tuple() 返回 Tuple*，因此我们需要一个内部的
    // unique_ptr (current_output_tuple_) 来管理这个将要返回的元组的生命周期。
    // 通过克隆，可以确保 current_tuple() 返回的指针指向的 Tuple 对象是独立的，
    // 其生命周期与对 next() 的单次调用相关联（由 current_output_tuple_ 管理）。
    current_output_tuple_ = all_tuples_[current_tuple_idx_]->clone();
    if (!current_output_tuple_) {
         LOG_WARN("Failed to clone tuple for output.");
         return RC::OPERATOR_FAILED_CLONE;
    }


    current_tuple_idx_++;
    return RC::SUCCESS;
}

// 关闭 OrderByPhysicalOperator 算子
RC OrderByPhysicalOperator::close() {
    if (!opened_) {
        LOG_INFO("OrderByPhysicalOperator already closed or not opened");
        return RC::SUCCESS;
    }
    LOG_TRACE("Closing OrderByPhysicalOperator");
    all_tuples_.clear();
    current_output_tuple_.reset();
    RC rc = RC::SUCCESS;
    if (child_operator_) {
        rc = child_operator_->close();
    }
    opened_ = false;
    trx_ = nullptr;
    return rc;
}