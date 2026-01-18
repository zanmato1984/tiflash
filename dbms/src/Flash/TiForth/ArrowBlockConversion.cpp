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

#include <Flash/TiForth/ArrowBlockConversion.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <arrow/array.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <boost/multiprecision/cpp_int.hpp>

#include <cstring>

#include "tiforth/type_metadata.h"

namespace DB::TiForth
{

namespace
{

arrow::Result<DataTypePtr> fieldToDataType(const arrow::Field & field, ColumnOptions * out_options)
{
    ARROW_ASSIGN_OR_RAISE(const auto logical_type, tiforth::GetLogicalType(field));

    DataTypePtr nested;
    switch (logical_type.id)
    {
    case tiforth::LogicalTypeId::kDecimal:
        if (logical_type.decimal_precision <= 0)
            return arrow::Status::Invalid("decimal precision must be positive");
        if (logical_type.decimal_scale < 0)
            return arrow::Status::Invalid("decimal scale must not be negative");
        nested = createDecimal(static_cast<UInt64>(logical_type.decimal_precision), static_cast<UInt64>(logical_type.decimal_scale));
        break;
    case tiforth::LogicalTypeId::kMyDate:
        nested = std::make_shared<DataTypeMyDate>();
        break;
    case tiforth::LogicalTypeId::kMyDateTime:
        nested = std::make_shared<DataTypeMyDateTime>(logical_type.datetime_fsp >= 0 ? logical_type.datetime_fsp : 0);
        break;
    case tiforth::LogicalTypeId::kString:
    {
        nested = std::make_shared<DataTypeString>();
        if (out_options != nullptr)
        {
            ColumnOptions opt;
            opt.collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
            *out_options = opt;
        }
        break;
    }
    case tiforth::LogicalTypeId::kUnknown:
    default:
        break;
    }

    if (nested == nullptr)
    {
        // Fallback to Arrow physical type only.
        switch (field.type()->id())
        {
        case arrow::Type::INT8:
            nested = std::make_shared<DataTypeInt8>();
            break;
        case arrow::Type::INT16:
            nested = std::make_shared<DataTypeInt16>();
            break;
        case arrow::Type::INT32:
            nested = std::make_shared<DataTypeInt32>();
            break;
        case arrow::Type::INT64:
            nested = std::make_shared<DataTypeInt64>();
            break;
        case arrow::Type::UINT8:
            nested = std::make_shared<DataTypeUInt8>();
            break;
        case arrow::Type::UINT16:
            nested = std::make_shared<DataTypeUInt16>();
            break;
        case arrow::Type::UINT32:
            nested = std::make_shared<DataTypeUInt32>();
            break;
        case arrow::Type::UINT64:
            nested = std::make_shared<DataTypeUInt64>();
            break;
        case arrow::Type::FLOAT:
            nested = std::make_shared<DataTypeFloat32>();
            break;
        case arrow::Type::DOUBLE:
            nested = std::make_shared<DataTypeFloat64>();
            break;
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
        {
            const auto & dec = static_cast<const arrow::DecimalType &>(*field.type());
            nested = createDecimal(static_cast<UInt64>(dec.precision()), static_cast<UInt64>(dec.scale()));
            break;
        }
        case arrow::Type::BINARY:
        case arrow::Type::STRING:
            nested = std::make_shared<DataTypeString>();
            break;
        default:
            return arrow::Status::NotImplemented("unsupported Arrow field type: ", field.type()->ToString());
        }
    }

    if (field.nullable())
        return makeNullable(nested);
    return nested;
}

template <typename ArrowArray, typename ColumnT, typename ValueT>
arrow::Result<MutableColumnPtr> buildPODColumn(const arrow::Array & array)
{
    const auto & arr = static_cast<const ArrowArray &>(array);
    auto col = ColumnT::create();
    auto & data = col->getData();
    data.reserve(static_cast<size_t>(arr.length()));
    for (int64_t i = 0; i < arr.length(); ++i)
    {
        if (arr.IsNull(i))
            data.push_back(ValueT());
        else
            data.push_back(static_cast<ValueT>(arr.Value(i)));
    }
    return col;
}

arrow::Result<MutableColumnPtr> buildStringColumn(const arrow::Array & array)
{
    auto col = ColumnString::create();
    if (array.type_id() == arrow::Type::BINARY)
    {
        const auto & arr = static_cast<const arrow::BinaryArray &>(array);
        for (int64_t i = 0; i < arr.length(); ++i)
        {
            if (arr.IsNull(i))
            {
                col->insertDefault();
                continue;
            }
            const std::string_view view = arr.GetView(i);
            col->insertData(view.data(), view.size());
        }
        return col;
    }
    if (array.type_id() == arrow::Type::STRING)
    {
        const auto & arr = static_cast<const arrow::StringArray &>(array);
        for (int64_t i = 0; i < arr.length(); ++i)
        {
            if (arr.IsNull(i))
            {
                col->insertDefault();
                continue;
            }
            const std::string_view view = arr.GetView(i);
            col->insertData(view.data(), view.size());
        }
        return col;
    }
    return arrow::Status::NotImplemented("expected Arrow binary/string array");
}

arrow::Result<MutableColumnPtr> buildDecimalColumn(const arrow::Array & array, const DataTypePtr & nested_type)
{
    if (nested_type == nullptr)
        return arrow::Status::Invalid("decimal type must not be null");

    const auto type_id = nested_type->getTypeId();
    if (array.type_id() != arrow::Type::DECIMAL128 && array.type_id() != arrow::Type::DECIMAL256)
        return arrow::Status::NotImplemented("expected Arrow decimal128/decimal256 array");

    const auto & fixed = static_cast<const arrow::FixedSizeBinaryArray &>(array);
    const int64_t rows = fixed.length();

    auto out = nested_type->createColumn();

    const auto decode128 = [&](int64_t i) -> DB::Int128 {
        const auto * ptr = fixed.GetValue(i);
        const arrow::Decimal128 dec(reinterpret_cast<const uint8_t *>(ptr));
        // Avoid UB: left-shifting a negative signed integer is undefined.
        const uint64_t high = static_cast<uint64_t>(dec.high_bits());
        const uint64_t low = static_cast<uint64_t>(dec.low_bits());
        const unsigned __int128 u = (static_cast<unsigned __int128>(high) << 64) | low;
        return static_cast<DB::Int128>(u);
    };

    const auto decode256 = [&](int64_t i) -> DB::Int256 {
        const auto * ptr = fixed.GetValue(i);
        const arrow::Decimal256 dec(reinterpret_cast<const uint8_t *>(ptr));
        const auto words = dec.little_endian_array(); // low->high

        boost::multiprecision::cpp_int unsigned_val = 0;
        for (int wi = static_cast<int>(words.size()) - 1; wi >= 0; --wi)
        {
            unsigned_val <<= 64;
            unsigned_val += words[static_cast<size_t>(wi)];
        }

        const bool negative = (words.back() & (uint64_t(1) << 63)) != 0;
        if (negative)
        {
            unsigned_val -= (boost::multiprecision::cpp_int(1) << 256);
        }
        return static_cast<DB::Int256>(unsigned_val);
    };

    // We only support Arrow<->DB roundtrip for the canonical widths:
    // - Decimal32/64/128 use Arrow decimal128 physical storage.
    // - Decimal256 uses Arrow decimal256 physical storage.
    if (type_id != TypeIndex::Decimal256 && array.type_id() != arrow::Type::DECIMAL128)
        return arrow::Status::NotImplemented("expected Arrow decimal128 array for non-decimal256 type");
    if (type_id == TypeIndex::Decimal256 && array.type_id() != arrow::Type::DECIMAL256)
        return arrow::Status::NotImplemented("expected Arrow decimal256 array for decimal256 type");

    if (type_id == TypeIndex::Decimal32)
    {
        auto * col = typeid_cast<ColumnDecimal<Decimal32> *>(out.get());
        if (col == nullptr)
            return arrow::Status::Invalid("expected Decimal32 column");
        for (int64_t i = 0; i < rows; ++i)
        {
            if (fixed.IsNull(i))
                col->insert(Decimal32(Int32(0)));
            else
                col->insert(Decimal32(static_cast<Int32>(decode128(i))));
        }
        return out;
    }
    if (type_id == TypeIndex::Decimal64)
    {
        auto * col = typeid_cast<ColumnDecimal<Decimal64> *>(out.get());
        if (col == nullptr)
            return arrow::Status::Invalid("expected Decimal64 column");
        for (int64_t i = 0; i < rows; ++i)
        {
            if (fixed.IsNull(i))
                col->insert(Decimal64(Int64(0)));
            else
                col->insert(Decimal64(static_cast<Int64>(decode128(i))));
        }
        return out;
    }
    if (type_id == TypeIndex::Decimal128)
    {
        auto * col = typeid_cast<ColumnDecimal<Decimal128> *>(out.get());
        if (col == nullptr)
            return arrow::Status::Invalid("expected Decimal128 column");
        for (int64_t i = 0; i < rows; ++i)
        {
            if (fixed.IsNull(i))
                col->insert(Decimal128(Int128(0)));
            else
                col->insert(Decimal128(decode128(i)));
        }
        return out;
    }
    if (type_id == TypeIndex::Decimal256)
    {
        auto * col = typeid_cast<ColumnDecimal<Decimal256> *>(out.get());
        if (col == nullptr)
            return arrow::Status::Invalid("expected Decimal256 column");
        for (int64_t i = 0; i < rows; ++i)
        {
            if (fixed.IsNull(i))
                col->insert(Decimal256(Int256(0)));
            else
                col->insert(Decimal256(decode256(i)));
        }
        return out;
    }

    return arrow::Status::NotImplemented("unsupported decimal type id: ", nested_type->getName());
}

arrow::Result<ColumnPtr> arrayToColumn(const arrow::Array & array, const DataTypePtr & type)
{
    if (type == nullptr)
        return arrow::Status::Invalid("type must not be null");

    const bool nullable = type->isNullable();
    const auto nested_type = removeNullable(type);
    if (nested_type == nullptr)
        return arrow::Status::Invalid("nested type must not be null");

    MutableColumnPtr nested_col;
    if (nested_type->isDecimal())
    {
        ARROW_ASSIGN_OR_RAISE(nested_col, buildDecimalColumn(array, nested_type));
    }
    else
    {
        switch (nested_type->getTypeId())
        {
        case TypeIndex::Int8:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::Int8Array, ColumnInt8, Int8>(array)));
            break;
        }
        case TypeIndex::Int16:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::Int16Array, ColumnInt16, Int16>(array)));
            break;
        }
        case TypeIndex::Int32:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::Int32Array, ColumnInt32, Int32>(array)));
            break;
        }
        case TypeIndex::Int64:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::Int64Array, ColumnInt64, Int64>(array)));
            break;
        }
        case TypeIndex::UInt8:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::UInt8Array, ColumnUInt8, UInt8>(array)));
            break;
        }
        case TypeIndex::UInt16:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::UInt16Array, ColumnUInt16, UInt16>(array)));
            break;
        }
        case TypeIndex::UInt32:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::UInt32Array, ColumnUInt32, UInt32>(array)));
            break;
        }
        case TypeIndex::UInt64:
        case TypeIndex::MyDate:
        case TypeIndex::MyDateTime:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::UInt64Array, ColumnUInt64, UInt64>(array)));
            break;
        }
        case TypeIndex::Float32:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::FloatArray, ColumnFloat32, Float32>(array)));
            break;
        }
        case TypeIndex::Float64:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, (buildPODColumn<arrow::DoubleArray, ColumnFloat64, Float64>(array)));
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString:
        {
            ARROW_ASSIGN_OR_RAISE(nested_col, buildStringColumn(array));
            break;
        }
        default:
            return arrow::Status::NotImplemented("unsupported nested type: ", nested_type->getName());
        }
    }

    if (!nullable)
        return std::move(nested_col);

    // Wrap with null map.
    auto null_map = ColumnUInt8::create();
    auto & data = null_map->getData();
    data.reserve(static_cast<size_t>(array.length()));
    for (int64_t i = 0; i < array.length(); ++i)
        data.push_back(array.IsNull(i) ? 1 : 0);

    return ColumnNullable::create(std::move(nested_col), std::move(null_map));
}

} // namespace

arrow::Result<BlockConversionResult> fromArrowRecordBatch(const std::shared_ptr<arrow::RecordBatch> & batch)
{
    if (batch == nullptr)
        return arrow::Status::Invalid("batch must not be null");
    const auto schema = batch->schema();
    if (schema == nullptr)
        return arrow::Status::Invalid("schema must not be null");

    BlockConversionResult result;
    ColumnsWithTypeAndName cols;
    cols.reserve(static_cast<size_t>(batch->num_columns()));

    for (int i = 0; i < batch->num_columns(); ++i)
    {
        const auto & field = schema->field(i);
        const auto & array = batch->column(i);
        if (field == nullptr)
            return arrow::Status::Invalid("field must not be null");
        if (array == nullptr)
            return arrow::Status::Invalid("array must not be null");

        ColumnOptions options;
        ARROW_ASSIGN_OR_RAISE(auto type, fieldToDataType(*field, &options));
        ARROW_ASSIGN_OR_RAISE(auto column, arrayToColumn(*array, type));

        if (options.collation_id.has_value())
            result.options_by_name[field->name()] = options;

        cols.emplace_back(std::move(column), std::move(type), field->name());
    }

    result.block = Block(std::move(cols));
    return result;
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
