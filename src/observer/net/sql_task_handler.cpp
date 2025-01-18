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
// Created by Wangyunlai on 2024/01/10.
//

#include "net/sql_task_handler.h"
#include "net/communicator.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/parser/parse_defs.h"
#include "storage/db/db.h"

RC SqlTaskHandler::handle_event(Communicator *communicator)
{
  SessionEvent *event = nullptr;
  RC rc = communicator->read_event(event);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (nullptr == event) {
    return RC::SUCCESS;
  }

  session_stage_.handle_request2(event);

  SQLStageEvent sql_event(event, event->query());

  rc = handle_sql(&sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to handle sql. rc=%s", strrc(rc));
    event->sql_result()->set_return_code(rc);
  }

  bool need_disconnect = false;

  rc = communicator->write_result(event, need_disconnect);
  LOG_INFO("write result return %s", strrc(rc));
  event->session()->set_current_request(nullptr);
  Session::set_current_session(nullptr);

  delete event;

  if (need_disconnect) {
    return RC::INTERNAL;
  }
  return RC::SUCCESS;
}

RC SqlTaskHandler::handle_sql(SQLStageEvent *sql_event)
{
  RC rc = query_cache_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do query cache. rc=%s", strrc(rc));
    return rc;
  }

  // 解析 sql 语句，创建 sqlnode
  rc = parse_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
    return rc;
  }

  // check views
  // 逻辑暂时放在这里，做可行性验证
  if (sql_event->sql_node()->flag == SCF_SELECT || 
    sql_event->sql_node()->flag == SCF_INSERT ||
    sql_event->sql_node()->flag == SCF_UPDATE) {
    auto *db = sql_event->session_event()->session()->get_current_db();
    if (db == nullptr) return RC::INTERNAL;

    std::vector<std::string> view_names;
    switch (sql_event->sql_node()->flag)
    {
    case SCF_SELECT:
      for (auto &relation : sql_event->sql_node()->selection.relations) view_names.push_back(relation.name);
      break;
    case SCF_INSERT:
      view_names.push_back(sql_event->sql_node()->insertion.relation_name);
      break;
    case SCF_UPDATE:
      view_names.push_back(sql_event->sql_node()->update.relation_name);
    default:
      break;
    }

    for (auto &view_name : view_names) {
      View *view = db->find_view(view_name.c_str());
      if (view == nullptr) continue;
      sql_event->add_view_sql(view->view_definition());
      sql_event->add_view_name(view->view_name());
    }
    LOG_DEBUG("found %d views in your sql", sql_event->sql_views().size());
    // ... cache stage
    // 解析 View 的 SQL
    rc = parse_stage_.handle_view_request(sql_event);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
      return rc;
    }
  }


  // 从 sqlnode 中创建 stmt
  rc = resolve_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do resolve. rc=%s", strrc(rc));
    return rc;
  }

  // 下推算子、生成 logical 和 physical operator
  rc = optimize_stage_.handle_request(sql_event);
  if (rc != RC::UNIMPLEMENTED && rc != RC::SUCCESS) {
    LOG_TRACE("failed to do optimize. rc=%s", strrc(rc));
    return rc;
  }

  rc = execute_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do execute. rc=%s", strrc(rc));
    return rc;
  }

  return rc;
}