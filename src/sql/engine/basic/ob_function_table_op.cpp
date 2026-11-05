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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/session/ob_sql_session_info.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_,
                    file_descriptor_, file_columns_);

ObString file_sql_descriptor(const ObRawExpr *expr)
{
  ObString descriptor;
  if (expr != nullptr && expr->get_expr_type() == T_VARCHAR && expr->is_const_expr()) {
    const ObObj &value = static_cast<const ObConstRawExpr *>(expr)->get_value();
    const ObString prefix(FILE_SQL_DESCRIPTOR_PREFIX);
    if (value.is_varchar() && value.get_varchar().prefix_match(prefix)) {
      const ObString data = value.get_varchar();
      descriptor.assign_ptr(data.ptr() + prefix.length(), data.length() - prefix.length());
    }
  }
  return descriptor;
}

int file_sql_error(const char *message)
{
  LOG_USER_ERROR(OB_NOT_SUPPORTED, message);
  return OB_NOT_SUPPORTED;
}

int get_file_sql_root(const ObSQLSessionInfo *session, ObString &root)
{
  int ret = OB_SUCCESS;
  bool enabled = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (!enabled) {
    ret = file_sql_error("file SQL is disabled (enable_file_sql is OFF)");
  } else if (OB_FAIL(session->get_secure_file_priv(root))) {
  }
  return ret;
}

int ObFunctionTableOp::open_file_scan()
{
  int ret = OB_SUCCESS;
  ObString root;
  char error[512] = {};
  seekdb_file_close(file_scan_);
  file_scan_ = nullptr;
  if (OB_FAIL(get_file_sql_root(ctx_.get_my_session(), root))) {
  } else if (OB_ISNULL(file_scan_ = seekdb_file_open(
                 FileSqlSpan::from(MY_SPEC.file_descriptor_), FileSqlSpan::from(root),
                 MY_SPEC.file_columns_.count() == 0 ? nullptr : &MY_SPEC.file_columns_.at(0),
                 MY_SPEC.file_columns_.count(), error, sizeof(error)))) {
    ret = file_sql_error(error);
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_file_row()
{
  clear_evaluated_flag();
  return read_file_row();
}

int ObFunctionTableOp::read_file_row()
{
  int ret = OB_SUCCESS;
  char error[512] = {};
  if (OB_ISNULL(file_scan_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    const int found = seekdb_file_next(file_scan_, error, sizeof(error));
    if (found < 0) {
      ret = file_sql_error(error);
    } else if (found == 0) {
      ret = OB_ITER_END;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
    ObExpr &expr = *MY_SPEC.column_exprs_.at(i);
    ObDatum &datum = expr.locate_datum_for_write(eval_ctx_);
    int64_t integer = 0;
    double real = 0;
    FileSqlSpan text{};
    const FileSqlKind kind = seekdb_file_cell(file_scan_, i, integer, real, text);
    if (kind == FileSqlKind::NULL_VALUE) {
      datum.set_null();
    } else if (kind == FileSqlKind::DOUBLE) {
      datum.set_double(real);
    } else if (kind == FileSqlKind::VARCHAR) {
      char *buffer = expr.get_str_res_mem(eval_ctx_, text.len_ + 1);
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (text.len_ > 0) {
          MEMCPY(buffer, text.data_, text.len_);
        }
        datum.set_string(ObString(static_cast<int32_t>(text.len_), buffer));
      }
    } else if (expr.datum_meta_.type_ == ObDateType) {
      datum.set_date(static_cast<int32_t>(integer));
    } else if (expr.datum_meta_.type_ == ObDateTimeType) {
      datum.set_datetime(integer);
    } else {
      datum.set_int(integer);
    }
    if (OB_SUCC(ret)) {
      expr.set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int FileTableOp::inner_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  brs_.size_ = 0;
  brs_.skip_->reset(max_row_cnt);
  ObEvalCtx::BatchInfoScopeGuard batch(eval_ctx_);
  batch.set_batch_size(max_row_cnt);
  while (OB_SUCC(ret) && brs_.size_ < max_row_cnt) {
    batch.set_batch_idx(brs_.size_);
    if (OB_FAIL(read_file_row())) {
    } else {
      ++brs_.size_;
    }
  }
  if (ret == OB_ITER_END) {
    brs_.end_ = brs_.size_ == 0;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (!MY_SPEC.file_descriptor_.empty()) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_file_row;
    ret = open_file_scan();
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (!MY_SPEC.file_descriptor_.empty()) {
    ret = open_file_scan();
  } else {
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  seekdb_file_close(file_scan_);
  file_scan_ = nullptr;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  seekdb_file_close(file_scan_);
  file_scan_ = nullptr;
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
