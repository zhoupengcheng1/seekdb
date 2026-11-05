/*
 * Copyright (c) 2025 OceanBase.
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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/engine/basic/ob_file_sql.h"
#include "sql/parser/ob_parser.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;

static int resolve_file_description(ObResolverParams &params, const ObSelectStmt &source,
                                    ObStmt *&stmt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  OZ(sql.append("SELECT column_name, column_type, `null`, `key`, `default`, extra FROM (VALUES "));
  for (int64_t i = 0; OB_SUCC(ret) && i < source.get_select_item_size(); ++i) {
    const SelectItem &item = source.get_select_item(i);
    const ObRawExpr &expr = *item.expr_;
    const ObString &name = item.alias_name_.empty() ? expr.get_expr_name() : item.alias_name_;
    const char *type = ob_obj_type_str(expr.get_data_type());
    bool nullable = !(expr.get_result_flag() & NOT_NULL_FLAG);
    if (expr.is_column_ref_expr()) {
      const auto &ref = static_cast<const ObColumnRefRawExpr &>(expr);
      const TableItem *table = source.get_table_item_by_id(ref.get_table_id());
      const ObString descriptor = table == nullptr ? ObString() : file_sql_descriptor(table->function_table_expr_);
      if (!descriptor.empty()) {
        FileSqlBound *bound = seekdb_file_decode(FileSqlSpan::from(descriptor));
        const size_t index = ref.get_column_id() - 1;
        if (bound == nullptr || index >= seekdb_file_column_count(bound)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          FileSqlKind kind = FileSqlKind::NULL_VALUE;
          uint8_t scale = 0;
          seekdb_file_column(bound, index, kind, nullable, scale);
          static constexpr const char *names[] = {
            "NULL", "BIGINT", "DOUBLE", "BOOLEAN", "VARCHAR", "DATE", "DATETIME"
          };
          type = names[static_cast<size_t>(kind)];
        }
        seekdb_file_bound_close(bound);
      }
    }
    OZ(sql.append(i == 0 ? "ROW(CONVERT(X'" : ",ROW(CONVERT(X'"));
    for (int32_t j = 0; OB_SUCC(ret) && j < name.length(); ++j) {
      OZ(sql.append_fmt("%02x", static_cast<unsigned char>(name.ptr()[j])));
    }
    OZ(sql.append_fmt("' USING utf8mb4),'%s','%s',NULL,NULL,NULL)", type, nullable ? "YES" : "NO"));
  }
  OZ(sql.append(") AS file_schema(column_name,column_type,`null`,`key`,`default`,extra)"));
  ObParser parser(*params.allocator_, params.session_info_->get_sql_mode());
  ParseResult result;
  OZ(parser.parse(sql.string(), result));
  if (OB_SUCC(ret) && (result.result_tree_ == nullptr || result.result_tree_->num_child_ != 1
                      || result.result_tree_->children_[0] == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    ObResolver resolver(params);
    ret = resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *result.result_tree_->children_[0], stmt);
  }
  return ret;
}

int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else if (parse_tree.children_[EXPLAIN_FORMAT] != nullptr
                 && parse_tree.children_[EXPLAIN_FORMAT]->type_ == T_INT && child_stmt->is_select_stmt()
                 && params_.query_ctx_->has_file_sql_) {
        ObStmt *description = nullptr;
        if (OB_SUCC(ret = resolve_file_description(params_, *static_cast<ObSelectStmt *>(child_stmt), description))) {
          set_basic_stmt(description);
        }
        return ret;
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format && format->type_ != T_INT) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}
