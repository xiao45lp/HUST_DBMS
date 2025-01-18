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
// Created by Wangyunlai on 2022/12/14.
//

#include <utility>

#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/aggregate_vec_physical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/calc_physical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/delete_physical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/explain_physical_operator.h"
#include "sql/operator/expr_vec_physical_operator.h"
#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/operator/index_scan_physical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/insert_physical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/join_physical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/project_vec_physical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/group_by_logical_operator.h"
#include "sql/operator/group_by_physical_operator.h"
#include "sql/operator/hash_group_by_physical_operator.h"
#include "sql/operator/scalar_group_by_physical_operator.h"
#include "sql/operator/table_scan_vec_physical_operator.h"
#include "sql/operator/update_logical_opeator.h"
#include "sql/operator/update_physical_opeator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/operator/order_by_logical_operator.h"
#include "sql/operator/order_by_physical_operator.h"
#include "sql/operator/vector_index_scan_physical_operator.h"
using namespace std;

RC PhysicalPlanGenerator::create(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;

  switch (logical_operator.type()) {
    case LogicalOperatorType::CALC: {
      return create_plan(static_cast<CalcLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::INSERT: {
      return create_plan(static_cast<InsertLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::DELETE: {
      return create_plan(static_cast<DeleteLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::JOIN: {
      return create_plan(static_cast<JoinLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::GROUP_BY: {
      return create_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalOperator &>(logical_operator), oper);
    } break;

    case LogicalOperatorType::ORDER_BY: {
      return create_plan(static_cast<OrderByLogicalOperator &>(logical_operator), oper);
    } break;

    default: {
      ASSERT(false, "unknown logical operator type");
      return RC::INVALID_ARGUMENT;
    }
  }
  return rc;
}

RC PhysicalPlanGenerator::create_vec(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;

  switch (logical_operator.type()) {
    case LogicalOperatorType::TABLE_GET: {
      return create_vec_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);
    } break;
    case LogicalOperatorType::PROJECTION: {
      return create_vec_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);
    } break;
    case LogicalOperatorType::GROUP_BY: {
      return create_vec_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);
    } break;
    case LogicalOperatorType::EXPLAIN: {
      return create_vec_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);
    } break;
    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
  return rc;
}



RC PhysicalPlanGenerator::create_plan(TableGetLogicalOperator &table_get_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();
  Table *table = table_get_oper.table();
  Index                          *index      = nullptr;
  ValueExpr                      *value_expr = nullptr;
  // 简单处理，就找等值查询
  std::vector<const char *> index_field_names;
  vector<Value>             values;
  LOG_DEBUG("table get predicate exprs length: %d", predicates.size());
  for (auto &expr : predicates) {
    if (expr->type() == ExprType::COMPARISON) {
      auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());

      // 创建子查询
      if (comparison_expr->left()->type() == ExprType::SUB_QUERY) {
        auto sub_query_expr = static_cast<SubqueryExpr *>(comparison_expr->left().get());
        if (sub_query_expr->physical_operator() != nullptr) {
          LOG_WARN("[UNEXPECTED] subquery physical operator is not null!");
        }
        unique_ptr<PhysicalOperator> subquery_phy_oper = nullptr;
        RC rc = create(*sub_query_expr->logical_operator(), subquery_phy_oper);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to create subquery physical operator. rc=%s", strrc(rc));
          return rc;
        }
        sub_query_expr->set_physical_operator(std::move(subquery_phy_oper));
      } 
      if (comparison_expr->right()->type() == ExprType::SUB_QUERY) {
        auto sub_query_expr = static_cast<SubqueryExpr *>(comparison_expr->right().get());
        if (sub_query_expr->physical_operator() != nullptr) {
          LOG_WARN("[UNEXPECTED] subquery physical operator is not null!");
        }
        unique_ptr<PhysicalOperator> subquery_phy_oper = nullptr;
        RC rc = create(*sub_query_expr->logical_operator(), subquery_phy_oper);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to create subquery physical operator. rc=%s", strrc(rc));
          return rc;
        }
        sub_query_expr->set_physical_operator(std::move(subquery_phy_oper));
      }

      if (comparison_expr->comp() != CompOp::EQUAL_TO) {
        continue;
      }

      unique_ptr<Expression> &left_expr  = comparison_expr->left();
      unique_ptr<Expression> &right_expr = comparison_expr->right();
      // 左右比较的一边最少是一个值
      if (left_expr->type() != ExprType::VALUE && right_expr->type() != ExprType::VALUE) {
        continue;
      }

      FieldExpr *field_expr = nullptr;
      if (left_expr->type() == ExprType::FIELD) {
        ASSERT(right_expr->type() == ExprType::VALUE, "right expr should be a value expr while left is field expr");
        field_expr = static_cast<FieldExpr *>(left_expr.get());
        value_expr = static_cast<ValueExpr *>(right_expr.get());
      } else if (right_expr->type() == ExprType::FIELD) {
        ASSERT(left_expr->type() == ExprType::VALUE, "left expr should be a value expr while right is a field expr");
        field_expr = static_cast<FieldExpr *>(right_expr.get());
        value_expr = static_cast<ValueExpr *>(left_expr.get());
      }

      if (field_expr == nullptr) {
        continue;
      }

      const Field &field = field_expr->field();
      index_field_names.push_back(field.field_name());
      values.push_back(value_expr->get_value());
    }
  }

  if (!table_get_oper.not_use_index()) {
    index = table->find_index_by_fields(index_field_names);
  }

  if (index != nullptr) {
    ASSERT(value_expr != nullptr, "got an index but value expr is null ?");

    IndexScanPhysicalOperator *index_scan_oper = new IndexScanPhysicalOperator(table,
        index,
        table_get_oper.read_write_mode(),
        values,
        true /*left_inclusive*/,
        values,
        true /*right_inclusive*/);

    index_scan_oper->set_predicates(std::move(predicates));
    index_scan_oper->is_or_conjunction = table_get_oper.is_or_conjunction;
    oper                               = unique_ptr<PhysicalOperator>(index_scan_oper);
    LOG_INFO("use index scan");
    return RC::SUCCESS;
  }
  auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.read_write_mode());
  table_scan_oper->set_table_alias(table_get_oper.table_alias());
  table_scan_oper->set_predicates(std::move(predicates));
  table_scan_oper->is_or_conjunction = table_get_oper.is_or_conjunction;
  oper                               = unique_ptr<PhysicalOperator>(table_scan_oper);
  LOG_INFO("use table scan");

  return RC::SUCCESS;
}

RC PhysicalPlanGenerator::create_plan(PredicateLogicalOperator &pred_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalOperator>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalOperator &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC                           rc = create(child_oper, child_phy_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;
  }

  // 检查是否只有一个表达式(ConjunctionExpr)
  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  // 取出子查询的逻辑算子，创建物理算子
  std::vector<ComparisonExpr *> comparison_exprs;
  // std::vector<unique_ptr<ComparisonExpr>> comparison_exprs;
  if (expression->type() == ExprType::CONJUNCTION) {
    auto conjunction_expr = static_cast<ConjunctionExpr *>(expression.get());
    vector<unique_ptr<Expression>> &children = conjunction_expr->children();
    for (auto &child_expr : children) {
      if (child_expr->type() == ExprType::COMPARISON) {
        comparison_exprs.push_back(static_cast<ComparisonExpr *>(child_expr.get()));
      }
    }
  } else if (expression->type() == ExprType::COMPARISON) {
    comparison_exprs.push_back(static_cast<ComparisonExpr *>(expression.get()));
  }

  for (auto &comparison_expr : comparison_exprs) {
    if (comparison_expr->left()->type() == ExprType::SUB_QUERY) {
      auto sub_query_expr = static_cast<SubqueryExpr *>(comparison_expr->left().get());
      if (sub_query_expr->physical_operator() != nullptr) {
        LOG_WARN("[UNEXPECTED] subquery physical operator is not null!");
      }
      unique_ptr<PhysicalOperator> subquery_phy_oper = nullptr;
      RC rc = create(*sub_query_expr->logical_operator(), subquery_phy_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create subquery physical operator. rc=%s", strrc(rc));
        return rc;
      }
      sub_query_expr->set_physical_operator(std::move(subquery_phy_oper));
    }
    if (comparison_expr->right()->type() == ExprType::SUB_QUERY) {
      auto sub_query_expr = static_cast<SubqueryExpr *>(comparison_expr->right().get());
      if (sub_query_expr->physical_operator() != nullptr) {
        LOG_WARN("[UNEXPECTED] subquery physical operator is not null!");
      }
      unique_ptr<PhysicalOperator> subquery_phy_oper = nullptr;
      RC rc = create(*sub_query_expr->logical_operator(), subquery_phy_oper);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create subquery physical operator. rc=%s", strrc(rc));
        return rc;
      }
      sub_query_expr->set_physical_operator(std::move(subquery_phy_oper));
    }
  }

  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  return rc;
}

RC PhysicalPlanGenerator::create_plan(ProjectLogicalOperator &project_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalOperator>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  do {
    // 将orderby + limit 转化为向量索引查询计划
    // condition: limit, order by
    int limit = project_oper.limit();
    if (limit != -1 && child_opers.size() == 1 && child_opers[0]->type() == LogicalOperatorType::ORDER_BY) {
      const auto order_by_oper = static_cast<OrderByLogicalOperator *>(child_opers[0].get());
      const std::vector<std::unique_ptr<LogicalOperator>> &order_by_children = order_by_oper->children();
      const std::vector<std::unique_ptr<Expression>>      &order_by_exprs    = order_by_oper->expressions();
      // condition: order by L2_Distance(列，向量字面量)
      if (order_by_exprs.size() == 1 && order_by_exprs[0]->type() == ExprType::VECTOR_DISTANCE_EXPR) {
        const auto   order_by_expr = static_cast<VectorDistanceExpr *>(order_by_exprs[0].get());
        DistanceType distance_type;
        // 两个枚举类重复了，以下是非常糟糕的做法
        switch (order_by_expr->distance_type()) {
          case VectorDistanceExpr::Type::COSINE_DISTANCE: {
            distance_type = DistanceType::COSINE_DISTANCE;
            break;
          }
          case VectorDistanceExpr::Type::L2_DISTANCE: {
            distance_type = DistanceType::L2_DISTANCE;
            break;
          }
          case VectorDistanceExpr::Type::INNER_PRODUCT: {
            distance_type = DistanceType::INNER_PRODUCT;
            break;
          }
          default: {
            LOG_WARN("unsupported distance type: %d", static_cast<int>(order_by_expr->distance_type()));
            return RC::INTERNAL;
          };
        }
        FieldExpr* left_expr;
        ValueExpr* right_expr;
        // 只支持向量距离函数中，一边是列一边是值的情况
        if (order_by_expr->left()->type() == ExprType::FIELD && order_by_expr->right()->type() == ExprType::VALUE &&
            order_by_expr->right()->value_type() == AttrType::VECTORS) {
           left_expr   = static_cast<FieldExpr *>(order_by_expr->left().get());
           right_expr  = static_cast<ValueExpr *>(order_by_expr->right().get());
            }else if (order_by_expr->right()->type() == ExprType::FIELD && order_by_expr->left()->type() == ExprType::VALUE &&
                order_by_expr->left()->value_type() == AttrType::VECTORS) {
               left_expr   = static_cast<FieldExpr *>(order_by_expr->right().get());
               right_expr  = static_cast<ValueExpr *>(order_by_expr->left().get());
            }else {
              break;
            }
          Value      right_value = right_expr->get_value();
          if (order_by_children.size() == 1 && order_by_children[0]->type() == LogicalOperatorType::TABLE_GET) {
            const auto   table_get_oper = static_cast<TableGetLogicalOperator *>(order_by_children[0].get());
            Table       *table          = table_get_oper->table();
            VectorIndex *vector_index   = table->find_vector_index_by_fields(left_expr->field_name());
            // 检查查询的距离类型和向量索引的距离类型是否相同
            if (vector_index != nullptr && distance_type == vector_index->meta().distance_type()) {
              child_phy_oper = std::make_unique<VectorIndexScanPhysicalOperator>(table, vector_index, right_value, limit);
            }
          }
      }
    }
  } while (false);

  RC rc = RC::SUCCESS;
  if (!child_opers.empty() && child_phy_oper == nullptr) {
    LogicalOperator *child_oper = child_opers.front().get();

    rc = create(*child_oper, child_phy_oper);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto project_operator =
      make_unique<ProjectPhysicalOperator>(std::move(project_oper.expressions()), project_oper.limit());
  if (child_phy_oper) {
    project_operator->add_child(std::move(child_phy_oper));
  }

  oper = std::move(project_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalPlanGenerator::create_plan(InsertLogicalOperator &insert_oper, unique_ptr<PhysicalOperator> &oper)
{
  Table                  *table           = insert_oper.table();
  vector<Value>          &values          = insert_oper.values();
  InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(values));
  insert_phy_oper->set_attrs_name(insert_oper.attrs_name());
  oper.reset(insert_phy_oper);
  return RC::SUCCESS;
}

RC PhysicalPlanGenerator::create_plan(DeleteLogicalOperator &delete_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalOperator>> &child_opers = delete_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalOperator *child_oper = child_opers.front().get();

    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalPlanGenerator::create_plan(ExplainLogicalOperator &explain_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalOperator>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;

  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  return rc;
}

RC PhysicalPlanGenerator::create_plan(JoinLogicalOperator &join_oper, unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;

  vector<unique_ptr<LogicalOperator>> &child_opers = join_oper.children();
  if (child_opers.size() != 2) {
    LOG_WARN("join operator should have 2 children, but have %d", child_opers.size());
    return RC::INTERNAL;
  }

  unique_ptr<PhysicalOperator> join_physical_oper(new NestedLoopJoinPhysicalOperator);
  for (auto &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical child oper. rc=%s", strrc(rc));
      return rc;
    }

    join_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(join_physical_oper);
  return rc;
}

RC PhysicalPlanGenerator::create_plan(CalcLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;

  CalcPhysicalOperator *calc_oper = new CalcPhysicalOperator(std::move(logical_oper.expressions()));
  oper.reset(calc_oper);
  return rc;
}

RC PhysicalPlanGenerator::create_plan(GroupByLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;

  vector<unique_ptr<Expression>> &group_by_expressions = logical_oper.group_by_expressions();
  unique_ptr<GroupByPhysicalOperator> group_by_oper;
  if (group_by_expressions.empty()) {
    // 没有 group by，只有聚合函数的情况
    group_by_oper = make_unique<ScalarGroupByPhysicalOperator>(std::move(logical_oper.aggregate_expressions()));
  } else {
    group_by_oper = make_unique<HashGroupByPhysicalOperator>(std::move(logical_oper.group_by_expressions()),
        std::move(logical_oper.aggregate_expressions()));
  }

  ASSERT(logical_oper.children().size() == 1, "group by operator should have 1 child");

  LogicalOperator             &child_oper = *logical_oper.children().front();
  unique_ptr<PhysicalOperator> child_physical_oper;
  rc = create(child_oper, child_physical_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create child physical operator of group by operator. rc=%s", strrc(rc));
    return rc;
  }

  group_by_oper->add_child(std::move(child_physical_oper));

  oper = std::move(group_by_oper);
  return rc;
}

RC PhysicalPlanGenerator::create_vec_plan(TableGetLogicalOperator &table_get_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();
  Table *table = table_get_oper.table();
  TableScanVecPhysicalOperator *table_scan_oper = new TableScanVecPhysicalOperator(table, table_get_oper.read_write_mode());
  table_scan_oper->set_predicates(std::move(predicates));
  oper = unique_ptr<PhysicalOperator>(table_scan_oper);
  LOG_TRACE("use vectorized table scan");

  return RC::SUCCESS;
}

RC PhysicalPlanGenerator::create_vec_plan(GroupByLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> physical_oper = nullptr;
  if (logical_oper.group_by_expressions().empty()) {
    physical_oper = make_unique<AggregateVecPhysicalOperator>(std::move(logical_oper.aggregate_expressions()));
  } else {
    physical_oper = make_unique<GroupByVecPhysicalOperator>(
      std::move(logical_oper.group_by_expressions()), std::move(logical_oper.aggregate_expressions()));

  }

  ASSERT(logical_oper.children().size() == 1, "group by operator should have 1 child");

  LogicalOperator             &child_oper = *logical_oper.children().front();
  unique_ptr<PhysicalOperator> child_physical_oper;
  rc = create_vec(child_oper, child_physical_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create child physical operator of group by(vec) operator. rc=%s", strrc(rc));
    return rc;
  }

  physical_oper->add_child(std::move(child_physical_oper));

  oper = std::move(physical_oper);
  return rc;

  return RC::SUCCESS;
}

RC PhysicalPlanGenerator::create_vec_plan(ProjectLogicalOperator &project_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalOperator>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalOperator *child_oper = child_opers.front().get();
    rc                          = create_vec(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto project_operator = make_unique<ProjectVecPhysicalOperator>(std::move(project_oper.expressions()));

  if (child_phy_oper != nullptr) {
    std::vector<Expression *> expressions;
    for (auto &expr : project_operator->expressions()) {
      expressions.push_back(expr.get());
    }
    auto expr_operator = make_unique<ExprVecPhysicalOperator>(std::move(expressions));
    expr_operator->add_child(std::move(child_phy_oper));
    project_operator->add_child(std::move(expr_operator));
  }

  oper = std::move(project_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}


RC PhysicalPlanGenerator::create_vec_plan(ExplainLogicalOperator &explain_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalOperator>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;
  // reuse `ExplainPhysicalOperator` in explain vectorized physical plan
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create_vec(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  return rc;
}

RC PhysicalPlanGenerator::create_plan(UpdateLogicalOperator &logical_operator, std::unique_ptr<PhysicalOperator> &oper)
{
  RC rc = RC::SUCCESS;

  // 取出可能的 sub query 创建物理计划
  auto logical_update_stmt_exprs = std::move(logical_operator.exprs());
  for (auto &expr: logical_update_stmt_exprs) {
    if (expr->type() == ExprType::SUB_QUERY) {
      auto sub_query_expr = static_cast<SubqueryExpr *>(expr.get());
      unique_ptr<PhysicalOperator> subquery_phy_oper = nullptr;
      rc = create(*sub_query_expr->logical_operator(), subquery_phy_oper);
      if (rc != RC::SUCCESS) {
        LOG_PANIC("update_physical_generator: failed to create sub query physical plan. rc=%s", strrc(rc));
        return rc;
      }
      sub_query_expr->set_physical_operator(std::move(subquery_phy_oper));
    }
  }

  oper = std::make_unique<UpdatePhysicalOperator>(
      logical_operator.table(), logical_operator.field_metas(), std::move(logical_update_stmt_exprs));
  auto children = std::move(logical_operator.children());
  if (!children.empty()) {
    std::unique_ptr<PhysicalOperator> child_oper;
    if (rc = create(*children[0], child_oper); OB_FAIL(rc)) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }
    oper->add_child(std::move(child_oper));
  }
  return rc;
}

RC PhysicalPlanGenerator::create_plan(OrderByLogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper)
{
  oper = std::make_unique<OrderByPhysicalOperator>(
      std::move(logical_operator.expressions()), std::move(logical_operator.order_by_descs()));
  auto children = std::move(logical_operator.children());
  if (!children.empty()) {
    std::unique_ptr<PhysicalOperator> child_oper;
    if (RC rc = create(*children[0], child_oper); OB_FAIL(rc)) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }
    oper->add_child(std::move(child_oper));
  }
  return RC::SUCCESS;
}
