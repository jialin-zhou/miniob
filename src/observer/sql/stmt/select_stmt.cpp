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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"
#include <memory>

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }

  // query_expressions_ (vector<unique_ptr<Expression>>) 会自动释放其管理的 Expression 对象
  // group_by_ (vector<unique_ptr<Expression>>) 也会自动释放其管理的 Expression 对象

  // 清理 order_by_ 中的 Expression 对象，因为它们是裸指针
  // order_by_ 本身是 vector 对象，其内存在 SelectStmt 对象销毁时自动回收，
  // 但它包含的裸指针指向的内存需要如上所示手动管理。
  for (OrderBySqlNode &item : order_by_) {
    if (item.expr != nullptr) {
      delete item.expr;
      item.expr = nullptr;
    }
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);
  
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // collect and bind fields in 'order by'
  vector<OrderBySqlNode> bound_order_by_items;
  for (OrderBySqlNode &unbound_order_by_item : select_sql.orderbys) {
    if (nullptr == unbound_order_by_item .expr) {
      LOG_WARN("Order by item has a null expression pointer.");
      return RC::INVALID_ARGUMENT;
    }
    // ExpressionBinder::bind_expression 期望一个 unique_ptr<Expression>&作为输入。
    // 从 parse_defs.h 和你的 yacc 文件来看，OrderBySqlNode::expr 是一个由解析器 new 出来的裸指针。
    // 我们用一个 unique_ptr 临时接管这个裸指针的所有权，以便传递给 binder。
    std::unique_ptr<Expression> expr_to_bind(unbound_order_by_item.expr);
    // （可选）为了安全，可以将原始解析节点中的指针置空，防止意外使用，
    // 尽管 SelectSqlNode 通常是临时的。
    // unbound_order_by_item.expr = nullptr; 

    std::vector<std::unique_ptr<Expression>> single_bound_expr_list; // binder 的输出是一个 vector
    RC rc_bind = expression_binder.bind_expression(expr_to_bind, single_bound_expr_list);

    if (OB_FAIL(rc_bind)) {
      LOG_WARN("Failed to bind order by expression. rc=%s", strrc(rc_bind));
      // 如果绑定失败，expr_to_bind (如果它没有被移动/释放) 会自动删除其管理的表达式。
      return rc_bind;
    }

    if (single_bound_expr_list.empty() || !single_bound_expr_list[0]) {
      LOG_WARN("Binding order by expression resulted in no output expression.");
      return RC::INVALID_ARGUMENT;
    }
    
    // 通常 ORDER BY 的一项应该只解析为一个表达式
    if (single_bound_expr_list.size() > 1) {
        LOG_WARN("Order by expression component resolved to multiple expressions, which is not typical.");
        return RC::SYNTAX_ERROR;
    }

    OrderBySqlNode bound_item_for_stmt;
    bound_item_for_stmt.expr = single_bound_expr_list[0].release(); 
    bound_item_for_stmt.is_asc = unbound_order_by_item.is_asc;
    bound_order_by_items.push_back(bound_item_for_stmt);
  }


  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->order_by_.swap(bound_order_by_items);
  stmt                      = select_stmt;
  return RC::SUCCESS;
}
