/*
 * Copyright (c) 2026 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/ob_ai_column_worker.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/utility.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_column_execution.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "query/vector/ob_vector_index_util.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include <charconv>
#include <limits>

namespace oceanbase::observer
{
using namespace common;
using namespace share::schema;
using namespace sql;

namespace
{
enum class RequestState : int64_t { PENDING = 0, STAGED = 2, RETRY_WAIT = 3, FAILED = 4 };

struct ColumnRequest
{
  uint64_t column_id_ = OB_INVALID_ID;
  int64_t version_ = 0;
  int64_t retries_ = 0;
  const ObColumnSchemaV2 *column_ = nullptr;
  int64_t state_ = static_cast<int64_t>(RequestState::PENDING);
  ObString result_;
  bool succeeded_ = false;
  bool retry_ = false;
};

int validate_ai_integer(const ObString &value, const ObObjType type)
{
  const ObString integer = value.trim();
  if (integer.empty()) {
    return OB_INVALID_DATA;
  }
  const char *begin = integer.ptr();
  const char *end = begin + integer.length();
  if (*begin == '+') {
    ++begin;
  }
  if (begin == end) {
    return OB_INVALID_DATA;
  }
  static const int widths[] = {8, 16, 24, 32, 64};
  if (ob_is_uint_tc(type)) {
    uint64_t parsed = 0;
    const auto result = std::from_chars(begin, end, parsed);
    const int bits = widths[type - ObUTinyIntType];
    const uint64_t maximum = bits == 64 ? std::numeric_limits<uint64_t>::max()
                                      : (uint64_t{1} << bits) - 1;
    return result.ec == std::errc() && result.ptr == end && parsed <= maximum
        ? OB_SUCCESS : OB_INVALID_DATA;
  }
  int64_t parsed = 0;
  const auto result = std::from_chars(begin, end, parsed);
  const int bits = widths[type - ObTinyIntType];
  const int64_t maximum = bits == 64 ? std::numeric_limits<int64_t>::max()
                                    : (int64_t{1} << (bits - 1)) - 1;
  return result.ec == std::errc() && result.ptr == end && parsed >= -maximum - 1 && parsed <= maximum
      ? OB_SUCCESS : OB_INVALID_DATA;
}

int read_ai_column_result(ObMySQLProxy &proxy, ObIAllocator &allocator,
                          const ObSqlString &table, const ObString &key,
                          const ObString &request, ColumnRequest &job)
{
  int ret = OB_SUCCESS;
  int64_t dimension = 0;
  ObSqlString query;
  const ObColumnSchemaV2 &column = *job.column_;
  if (column.is_collection()) {
    OZ (share::ObVectorIndexUtil::get_vector_dim_from_extend_type_info(column.get_extended_type_info(), dimension));
  }
  OZ (query.append("SELECT /*+ USE_PLAN_CACHE(NONE) */ "));
  OZ (query.append("CAST("));
  if (column.is_json()) {
    OZ (query.append("CAST("));
  }
  OZ (query.append(column.get_ai_column_expr()));
  if (column.is_json()) {
    OZ (query.append(" AS JSON)"));
  }
  OZ (query.append(" AS CHAR)"));
  OZ (query.append(" FROM "));
  OZ (query.append(table.string()));
  OZ (query.append(" WHERE "));
  OZ (query.append(key));
  OZ (query.append(" AND "));
  OZ (append_ai_companion(query, "__ai_request$", column.get_column_name_str()));
  OZ (query.append("="));
  OZ (sql_append_hex_escape_str(request, query));
  if (OB_SUCC(ret)) {
    ObMySQLTransaction read_transaction;
    OZ (read_transaction.start(&proxy));
    ObSQLSessionInfo *session = nullptr;
    if (OB_SUCC(ret)) {
      session = query::ObInnerSQLConnectionAccess::get_session(read_transaction.get_connection());
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_FAIL(ret)) {
      return ret;
    }
    AiColumnExecution inference(session, false, column.get_ai_column_type_prompt(), dimension);
    ObISQLClient::ReadResult result;
    ObString value;
    if (OB_FAIL(read_transaction.read(result, query.ptr()))) {
    } else if (OB_FAIL(result.get_result()->next())) {
    } else if (OB_FAIL(result.get_result()->get_varchar(int64_t{0}, value))) {
    } else if (column.get_meta_type().is_integer_type()) {
      ret = validate_ai_integer(value, column.get_data_type());
    } else if (column.is_string_type()
               && ObCharset::strlen_char(column.get_collation_type(), value.ptr(), value.length())
                   > static_cast<uint64_t>(column.get_data_length())) {
      ret = OB_ERR_DATA_TOO_LONG;
    }
    OZ (ob_write_string(allocator, value, job.result_));
  }
  return ret;
}

int append_ai_result_literal(ObSqlString &query, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (value.empty()) {
    ret = query.append("NULL");
  } else {
    OZ (query.append("CONVERT("));
    OZ (sql_append_hex_escape_str(value, query));
    OZ (query.append(" USING utf8mb4)"));
  }
  return ret;
}
} // namespace

int AiColumnWorker::schedule(ObTimer &timer, ObMySQLProxy &proxy,
                             ObMultiVersionSchemaService &schema_service)
{
  proxy_ = &proxy;
  schema_service_ = &schema_service;
  return timer.schedule(*this, 200 * 1000L, true);
}

void AiColumnWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (recover_leases_) {
    int64_t affected = 0;
    // A seekdb data directory is opened by a single server process. A lease
    // persisted by the previous process cannot have a live owner on startup.
    if (OB_FAIL(proxy_->write("UPDATE oceanbase.__all_ai_column_task "
                             "SET lease_id=NULL, lease_expires=NULL WHERE lease_id IS NOT NULL",
                             affected))) {
    } else {
      recover_leases_ = false;
    }
  }
  OZ (process_request());
  if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
    LOG_WARN("AI column request processing failed", K(ret));
  }
}

int AiColumnWorker::process_request()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColumnWorker");
  ObString request;
  ObString key;
  int64_t table_id = 0;
  {
    ObMySQLProxy::MySQLResult result;
    ObString request_value;
    ObString rowkey_value;
    if (OB_FAIL(proxy_->read(result, "SELECT request_id, table_id, rowkey_predicate "
                       "FROM oceanbase.__all_ai_column_task WHERE state IN (0,3) "
                       "AND eligible_ts <= NOW(6) AND (lease_expires IS NULL OR lease_expires < NOW(6)) "
                       "ORDER BY eligible_ts,table_id,column_id LIMIT 1"))) {
    } else if (OB_FAIL(result.get_result()->next())) {
      if (OB_ITER_END == ret) {
        return OB_SUCCESS;
      }
    } else if (OB_FAIL(result.get_result()->get_varchar(int64_t{0}, request_value))) {
    } else if (OB_FAIL(result.get_result()->get_int(1, table_id))) {
    } else if (OB_FAIL(result.get_result()->get_varchar(2, rowkey_value))) {
    } else if (OB_FAIL(ob_write_string(allocator, request_value, request))) {
    } else if (OB_FAIL(ob_write_string(allocator, rowkey_value, key))) {
    }
  }

  char lease[UuidCommon::LENGTH_UUID + 1] = {};
  ObSqlString request_filter;
  ObSqlString query;
  int64_t affected = 0;
  OZ (ObExprUuid::gen_server_uuid(lease, UuidCommon::LENGTH_UUID));
  OZ (request_filter.append("request_id = "));
  OZ (sql_append_hex_escape_str(request, request_filter));
  OZ (query.append_fmt("UPDATE oceanbase.__all_ai_column_task SET lease_id='%s', "
                      "lease_expires=DATE_ADD(NOW(6), INTERVAL 5 MINUTE) WHERE ", lease));
  OZ (query.append(request_filter.string()));
  OZ (query.append(" AND (lease_expires IS NULL OR lease_expires < NOW(6))"));
  OZ (proxy_->write(query.ptr(), affected));
  if (OB_FAIL(ret) || affected == 0) {
    return ret;
  }
  OZ (request_filter.append_fmt(" AND lease_id='%s'", lease));

  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  ObSqlString qualified_table;
  OZ (schema_service_->get_runtime_schema_guard(guard));
  OZ (guard.get_table_schema(static_cast<uint64_t>(table_id), table));
  if (OB_SUCC(ret) && OB_NOT_NULL(table) && !table->is_in_recyclebin()) {
    OZ (guard.get_database_schema(table->get_database_id(), database));
    if (OB_SUCC(ret) && OB_NOT_NULL(database)) {
      OZ (append_ai_identifier(qualified_table, database->get_database_name_str()));
      OZ (qualified_table.append("."));
      OZ (append_ai_identifier(qualified_table, table->get_table_name_str()));
    }
  }
  ObSEArray<ColumnRequest, 4> jobs;
  if (OB_SUCC(ret) && !qualified_table.empty()) {
    query.reset();
    OZ (query.append("SELECT column_id,definition_version,retry_count,state,result_payload "
                     "FROM oceanbase.__all_ai_column_task WHERE "));
    OZ (query.append(request_filter.string()));
    ObMySQLProxy::MySQLResult result;
    OZ (proxy_->read(result, query.ptr()));
    int next = OB_SUCCESS;
    while (OB_SUCC(ret) && OB_SUCCESS == (next = result.get_result()->next())) {
      ColumnRequest job;
      int64_t column_id = 0;
      ObString value;
      OZ (result.get_result()->get_int(int64_t{0}, column_id));
      OZ (result.get_result()->get_int(1, job.version_));
      OZ (result.get_result()->get_int(2, job.retries_));
      OZ (result.get_result()->get_int(3, job.state_));
      if (OB_SUCC(ret) && job.state_ == static_cast<int64_t>(RequestState::STAGED)) {
        OZ (result.get_result()->get_varchar(4, value));
        OZ (ob_write_string(allocator, value, job.result_));
        job.succeeded_ = true;
      }
      job.column_id_ = static_cast<uint64_t>(column_id);
      job.column_ = table->get_column_schema(job.column_id_);
      if (OB_SUCC(ret) && OB_NOT_NULL(job.column_) && job.column_->is_ai_column()
          && job.column_->get_schema_version() == job.version_) {
        OZ (jobs.push_back(job));
      }
    }
    if (OB_SUCC(ret) && OB_ITER_END != next) {
      ret = next;
    }
  }
  bool retry_group = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    ColumnRequest &job = jobs.at(i);
    if (job.state_ == static_cast<int64_t>(RequestState::PENDING)
        || job.state_ == static_cast<int64_t>(RequestState::RETRY_WAIT)) {
      const int model_ret = read_ai_column_result(*proxy_, allocator, qualified_table, key, request, job);
      job.succeeded_ = OB_SUCCESS == model_ret;
      job.retry_ = !job.succeeded_ && model_ret != OB_ITER_END
          && job.retries_ < job.column_->get_ai_column_max_retries();
      retry_group = retry_group || job.retry_;
    }
  }

  ObMySQLTransaction trans;
  OZ (trans.start(proxy_));
  if (OB_SUCC(ret) && retry_group) {
    for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
      const ColumnRequest &job = jobs.at(i);
      query.reset();
      OZ (query.append_fmt("UPDATE oceanbase.__all_ai_column_task SET state=%ld, "
                           "retry_count=%ld, eligible_ts=DATE_ADD(NOW(6), INTERVAL 1 SECOND), "
                           "lease_id=NULL, lease_expires=NULL, result_payload=",
                           static_cast<int64_t>(job.retry_ ? RequestState::RETRY_WAIT
                               : job.succeeded_ ? RequestState::STAGED : RequestState::FAILED),
                           job.retries_ + (job.retry_ ? 1 : 0)));
      OZ (append_ai_result_literal(query, job.result_));
      OZ (query.append(" WHERE "));
      OZ (query.append(request_filter.string()));
      OZ (query.append_fmt(" AND column_id=%lu", job.column_id_));
      OZ (trans.write(query.ptr(), affected));
    }
  } else if (OB_SUCC(ret)) {
    if (!jobs.empty()) {
      query.reset();
      OZ (query.append("SELECT 1 FROM "));
      OZ (query.append(qualified_table.string()));
      OZ (query.append(" WHERE "));
      OZ (query.append(key));
      OZ (query.append(" FOR UPDATE"));
      bool row_exists = false;
      {
        ObMySQLProxy::MySQLResult result;
        OZ (trans.read(result, query.ptr()));
        if (OB_SUCC(ret)) {
          const int next = result.get_result()->next();
          if (OB_SUCCESS == next) {
            row_exists = true;
          } else if (OB_ITER_END != next) {
            ret = next;
          }
        }
      }
      ObSEArray<int64_t, 4> active_columns;
      if (OB_SUCC(ret) && row_exists) {
        // The business DML locks the row before it updates the outbox. Keep the
        // same order here and verify the lease after acquiring the row lock.
        query.reset();
        OZ (query.append("SELECT column_id FROM oceanbase.__all_ai_column_task WHERE "));
        OZ (query.append(request_filter.string()));
        OZ (query.append(" FOR UPDATE"));
        ObMySQLProxy::MySQLResult result;
        OZ (trans.read(result, query.ptr()));
        int next = OB_SUCCESS;
        while (OB_SUCC(ret) && OB_SUCCESS == (next = result.get_result()->next())) {
          int64_t column_id = 0;
          OZ (result.get_result()->get_int(int64_t{0}, column_id));
          OZ (active_columns.push_back(column_id));
        }
        if (OB_SUCC(ret) && OB_ITER_END != next) {
          ret = next;
        }
      }
      if (OB_SUCC(ret) && !active_columns.empty()) {
        ObSchemaGetterGuard latest_guard;
        const ObTableSchema *latest_table = nullptr;
        OZ (schema_service_->get_runtime_schema_guard(latest_guard));
        OZ (latest_guard.get_table_schema(static_cast<uint64_t>(table_id), latest_table));
        query.reset();
        OZ (query.append("UPDATE /*+ USE_PLAN_CACHE(NONE) */ "));
        OZ (query.append(qualified_table.string()));
        OZ (query.append(" SET "));
        ObSqlString current_requests;
        bool has_assignments = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count() && OB_NOT_NULL(latest_table); ++i) {
          const ColumnRequest &job = jobs.at(i);
          const ObColumnSchemaV2 *latest = latest_table->get_column_schema(job.column_id_);
          if (!has_exist_in_array(active_columns, static_cast<int64_t>(job.column_id_))
              || OB_ISNULL(latest) || latest->get_schema_version() != job.version_) {
            continue;
          }
          const ObString &name = latest->get_column_name_str();
          if (has_assignments) {
            OZ (query.append(","));
          }
          has_assignments = true;
          OZ (append_ai_identifier(query, name));
          OZ (query.append("="));
          if (job.succeeded_) {
            OZ (append_ai_result_literal(query, job.result_));
          } else {
            OZ (query.append("NULL"));
          }
          OZ (query.append(","));
          OZ (append_ai_companion(query, "__ai_state$", name));
          OZ (query.append_fmt("='%s'", job.succeeded_ ? "READY" : "FAILED"));
          OZ (current_requests.append(" AND "));
          OZ (append_ai_companion(current_requests, "__ai_request$", name));
          OZ (current_requests.append("="));
          OZ (sql_append_hex_escape_str(request, current_requests));
          OZ (current_requests.append(" AND "));
          OZ (append_ai_companion(current_requests, "__ai_result_spec$", name));
          OZ (current_requests.append_fmt("='%ld'", job.version_));
        }
        if (OB_SUCC(ret) && has_assignments) {
          OZ (query.append(" WHERE "));
          OZ (query.append(key));
          OZ (query.append(current_requests.string()));
          ObSQLSessionInfo *session =
              query::ObInnerSQLConnectionAccess::get_session(trans.get_connection());
          if (OB_ISNULL(session)) {
            ret = OB_ERR_UNEXPECTED;
          }
          if (OB_SUCC(ret)) {
            AiColumnExecution writeback(session, true);
            OZ (trans.write(query.ptr(), affected));
          }
        }
      }
    }
    query.reset();
    OZ (query.append("DELETE FROM oceanbase.__all_ai_column_task WHERE "));
    OZ (query.append(request_filter.string()));
    OZ (trans.write(query.ptr(), affected));
  }
  if (trans.is_started()) {
    const int end_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCC(ret)) {
      ret = end_ret;
    }
  }
  return ret;
}
} // namespace oceanbase::observer
