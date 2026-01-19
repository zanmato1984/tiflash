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

#include <Flash/TiForth/TipbExprToTiForthExpr.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>

#include <arrow/scalar.h>
#include <arrow/status.h>

#include <string_view>
#include <unordered_set>

#include "tiforth/expr.h"

namespace DB::TiForth
{

namespace
{

bool isSupportedScalarFunctionName(std::string_view name)
{
    static const std::unordered_set<std::string_view> supported{
        "and",
        "or",
        "not",
        "equals",
        "notEquals",
        "less",
        "lessOrEquals",
        "greater",
        "greaterOrEquals",
        "plus",
        "minus",
        "multiply",
    };
    return supported.find(name) != supported.end();
}

arrow::Result<std::shared_ptr<arrow::Scalar>> tipbLiteralToArrowScalar(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
    case tipb::ExprType::Null:
        return std::make_shared<arrow::NullScalar>();
    case tipb::ExprType::Int64:
        return std::make_shared<arrow::Int64Scalar>(decodeDAGInt64(expr.val()));
    case tipb::ExprType::Uint64:
        return std::make_shared<arrow::UInt64Scalar>(decodeDAGUInt64(expr.val()));
    case tipb::ExprType::Float32:
        return std::make_shared<arrow::FloatScalar>(decodeDAGFloat32(expr.val()));
    case tipb::ExprType::Float64:
        return std::make_shared<arrow::DoubleScalar>(decodeDAGFloat64(expr.val()));
    case tipb::ExprType::String:
        return std::make_shared<arrow::StringScalar>(decodeDAGString(expr.val()));
    case tipb::ExprType::Bytes:
        return std::make_shared<arrow::BinaryScalar>(decodeDAGBytes(expr.val()));
    default:
        return arrow::Status::NotImplemented("unsupported literal expr type: ", tipb::ExprType_Name(expr.tp()));
    }
}

} // namespace

arrow::Result<tiforth::ExprPtr> TipbExprToTiForthExpr(
    const tipb::Expr & expr,
    const std::vector<NameAndTypePair> & input_columns)
{
    switch (expr.tp())
    {
    case tipb::ExprType::ColumnRef:
    {
        const auto name = getColumnNameForColumnExpr(expr, input_columns);
        return tiforth::MakeFieldRef(std::move(name));
    }
    case tipb::ExprType::ScalarFunc:
    {
        const auto & func = getFunctionName(expr);
        if (!isSupportedScalarFunctionName(func))
            return arrow::Status::NotImplemented("unsupported scalar function: ", func);

        std::vector<tiforth::ExprPtr> args;
        args.reserve(static_cast<size_t>(expr.children_size()));
        for (const auto & child : expr.children())
        {
            ARROW_ASSIGN_OR_RAISE(auto arg, TipbExprToTiForthExpr(child, input_columns));
            args.push_back(std::move(arg));
        }
        return tiforth::MakeCall(func, std::move(args));
    }
    default:
        break;
    }

    if (isLiteralExpr(expr))
    {
        ARROW_ASSIGN_OR_RAISE(auto scalar, tipbLiteralToArrowScalar(expr));
        return tiforth::MakeLiteral(std::move(scalar));
    }

    return arrow::Status::NotImplemented("unsupported expr type: ", tipb::ExprType_Name(expr.tp()));
}

arrow::Result<tiforth::ExprPtr> TipbSelectionConditionsToTiForthPredicate(
    const std::vector<tipb::Expr> & conditions,
    const std::vector<NameAndTypePair> & input_columns)
{
    if (conditions.empty())
        return arrow::Status::Invalid("selection conditions must not be empty");

    ARROW_ASSIGN_OR_RAISE(auto predicate, TipbExprToTiForthExpr(conditions.front(), input_columns));
    for (size_t i = 1; i < conditions.size(); ++i)
    {
        ARROW_ASSIGN_OR_RAISE(auto rhs, TipbExprToTiForthExpr(conditions[i], input_columns));
        predicate = tiforth::MakeCall("and", {std::move(predicate), std::move(rhs)});
    }

    return predicate;
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)

