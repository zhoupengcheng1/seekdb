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

#pragma once

#include "lib/string/ob_sql_string.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase::sql
{

inline int append_ai_identifier(common::ObSqlString &sql, const common::ObString &name)
{
  int ret = sql.append("`");
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (name.ptr()[i] == '`') {
      OZ (sql.append("`"));
    }
    OZ (sql.append(name.ptr() + i, 1));
  }
  OZ (sql.append("`"));
  return ret;
}

inline int append_ai_companion(common::ObSqlString &sql, const char *prefix,
                               const common::ObString &column)
{
  common::ObSqlString name;
  int ret = name.append(prefix);
  OZ (name.append(column));
  OZ (append_ai_identifier(sql, name.string()));
  return ret;
}

// Scoped to one SQL execution on the background worker or to one engine-generated
// mutation in the originating transaction. It cannot be enabled by a session variable.
class AiColumnExecution
{
public:
  AiColumnExecution(ObSQLSessionInfo *session, const bool write, const common::ObString &prompt = {},
                    const int64_t dimension = 0)
    : session_(session), previous_(session->get_ai_column_execution()),
      write_(write), prompt_(prompt), dimension_(dimension)
  {
    session_->set_ai_column_execution(this);
  }

  ~AiColumnExecution() { session_->set_ai_column_execution(previous_); }

  static bool writing(const ObSQLSessionInfo *session)
  { return active(session) && session->get_ai_column_execution()->write_; }
  static bool active(const ObSQLSessionInfo *session)
  { return nullptr != session && nullptr != session->get_ai_column_execution(); }
  static common::ObString prompt(const ObSQLSessionInfo *session)
  { return active(session) ? session->get_ai_column_execution()->prompt_ : common::ObString(); }
  static int64_t dimension(const ObSQLSessionInfo *session)
  { return active(session) ? session->get_ai_column_execution()->dimension_ : 0; }

private:
  ObSQLSessionInfo *session_;
  const AiColumnExecution *previous_;
  bool write_;
  common::ObString prompt_;
  int64_t dimension_;
  DISALLOW_COPY_AND_ASSIGN(AiColumnExecution);
};

} // namespace oceanbase::sql
