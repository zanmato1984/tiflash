// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Core/NamesAndTypes.h>

#include <arrow/result.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <vector>

namespace tiforth
{
struct Expr;
using ExprPtr = std::shared_ptr<Expr>;
} // namespace tiforth

namespace DB::TiForth
{

arrow::Result<tiforth::ExprPtr> TipbExprToTiForthExpr(
    const tipb::Expr & expr,
    const std::vector<NameAndTypePair> & input_columns);

arrow::Result<tiforth::ExprPtr> TipbSelectionConditionsToTiForthPredicate(
    const std::vector<tipb::Expr> & conditions,
    const std::vector<NameAndTypePair> & input_columns);

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)

