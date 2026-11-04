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
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (!ob_is_text_tc(types_stack[CONTENT_IDX].get_type())) {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    }
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}


static int embed_input_batch(ObIAllocator &allocator, ObString &model_id, int64_t dimension,
                             ObArray<ObString> &contents, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo endpoint;
  query::ObIAiEndpointResolver *resolver =
      ::oceanbase::share::server_service<query::ObIAiEndpointResolver>();
  ObJsonObject *config = nullptr;
  if (dimension > 0) {
    ObJsonInt *value = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dimension, value))) {
    } else if (OB_FAIL(config->add("dimensions", value))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {
  } else if (OB_ISNULL(resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(resolver->resolve_by_model_name(model_id, allocator, endpoint))) {
  } else {
    ObAIFuncModel model(allocator, *info, endpoint);
    ret = model.call_dense_embedding_vector_v2(contents, config, results);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *model = nullptr;
  ObDatum *content = nullptr;
  ObDatum *dim = nullptr;
  ObEvalCtx::TempAllocGuard guard(ctx);
  MultimodeAlloctor allocator(guard.get_allocator());
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, model, content, dim))
                         : OB_FAIL(expr.eval_param_value(ctx, model, content))) {
  } else if (model->is_null() || content->is_null()) {
    res.set_null();
  } else if (dim != nullptr && (dim->is_null() || dim->get_int() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString model_id = model->get_string();
    ObString text;
    ObArray<ObString> contents;
    ObArray<ObString> results;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator,
                   *content, expr.args_[CONTENT_IDX]->datum_meta_,
                   expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), text))) {
    } else if (model_id.empty() || text.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(contents.push_back(text))) {
    } else if (OB_FAIL(embed_input_batch(allocator, model_id, dim == nullptr ? 0 : dim->get_int(), contents, results))) {
    } else if (results.count() != 1) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, results.at(0)))) {
    }
  }
  return ret;
}



int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector models;
  ObDatumVector inputs;
  ObDatumVector dimensions;
  ObDatumVector output = expr.locate_expr_datumvector(ctx);
  ObBitVector &evaluated = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard guard(ctx);
  MultimodeAlloctor allocator(guard.get_allocator());
  ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
  ObArray<ObString> model_ids;
  ObArray<ObString> texts;
  ObArray<int64_t> dims;
  ObArray<int64_t> row_ids;
  ObArray<bool> done;
  if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, batch_size, models, inputs, dimensions))) {
  } else {
    // Only live, non-NULL rows enter requests. Keep original positions separate
    // from provider-local indices, including across multiple request batches.
    for (int64_t row = 0; OB_SUCC(ret) && row < batch_size; ++row) {
      if (skip.at(row) || evaluated.at(row)) { continue; }
      if (models.at(row)->is_null() || inputs.at(row)->is_null()) {
        output.at(row)->set_null();
        evaluated.set(row);
      } else {
        ObString model_id = models.at(row)->get_string();
        ObString text;
        int64_t dimension = 0;
        batch_guard.set_batch_idx(row);
        if (expr.arg_cnt_ == 3) {
          if (dimensions.at(row)->is_null() || dimensions.at(row)->get_int() <= 0) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            dimension = dimensions.at(row)->get_int();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator,
                       *inputs.at(row), expr.args_[CONTENT_IDX]->datum_meta_,
                       expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), text))) {
        } else if (model_id.empty() || text.empty()) {
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(model_ids.push_back(model_id))) {
        } else if (OB_FAIL(texts.push_back(text))) {
        } else if (OB_FAIL(dims.push_back(dimension))) {
        } else if (OB_FAIL(row_ids.push_back(row))) {
        } else if (OB_FAIL(done.push_back(false))) {
        }
      }
    }
    const int64_t request_batch_size = 64;
    for (int64_t first = 0; OB_SUCC(ret) && first < row_ids.count(); ++first) {
      if (done.at(first)) { continue; }
      ObArray<ObString> contents;
      ObArray<ObString> results;
      ObArray<int64_t> positions;
      for (int64_t i = first; OB_SUCC(ret) && i < row_ids.count()
                            && contents.count() < request_batch_size; ++i) {
        if (!done.at(i) && model_ids.at(i) == model_ids.at(first) && dims.at(i) == dims.at(first)) {
          if (OB_FAIL(contents.push_back(texts.at(i)))) {
          } else if (OB_FAIL(positions.push_back(row_ids.at(i)))) {
          } else {
            done.at(i) = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(embed_input_batch(allocator, model_ids.at(first), dims.at(first), contents, results))) {
      } else if (results.count() != positions.count()) {
        ret = OB_INVALID_DATA;
      } else {
        // Publish no data from a failed provider batch.
        for (int64_t i = 0; OB_SUCC(ret) && i < positions.count(); ++i) {
          int64_t row = positions.at(i);
          batch_guard.set_batch_idx(row);
          if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, *output.at(row), results.at(i)))) {
          } else {
            evaluated.set(row);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   ObObj const_data;
  //   bool got_data = false;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
