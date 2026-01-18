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

#include <Flash/TiForth/ArrowTypeMapping.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <boost/multiprecision/cpp_int.hpp>

#include <array>
#include <vector>

#include "tiforth/type_metadata.h"

namespace DB
{
namespace TiForth
{

namespace
{

arrow::Status ensurePool(arrow::MemoryPool * pool)
{
    if (pool == nullptr)
        return arrow::Status::Invalid("memory pool must not be null");
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::DataType>> toArrowDataType(const IDataType & type)
{
    switch (type.getTypeId())
    {
    case TypeIndex::Int8:
        return arrow::int8();
    case TypeIndex::Int16:
        return arrow::int16();
    case TypeIndex::Int32:
        return arrow::int32();
    case TypeIndex::Int64:
        return arrow::int64();
    case TypeIndex::UInt8:
        return arrow::uint8();
    case TypeIndex::UInt16:
        return arrow::uint16();
    case TypeIndex::UInt32:
        return arrow::uint32();
    case TypeIndex::UInt64:
        return arrow::uint64();
    case TypeIndex::Float32:
        return arrow::float32();
    case TypeIndex::Float64:
        return arrow::float64();
    case TypeIndex::MyDate:
        return arrow::uint64();
    case TypeIndex::MyDateTime:
        return arrow::uint64();
    case TypeIndex::String:
    case TypeIndex::FixedString:
        return arrow::binary();
    default:
        break;
    }

    if (type.isDecimal())
    {
        const auto prec = getDecimalPrecision(type, /*default_value=*/0);
        const auto scale = getDecimalScale(type, /*default_value=*/0);
        if (prec <= 0)
            return arrow::Status::Invalid("decimal precision must be positive");

        if (prec <= arrow::Decimal128Type::kMaxPrecision)
            return arrow::decimal128(static_cast<int32_t>(prec), static_cast<int32_t>(scale));
        return arrow::decimal256(static_cast<int32_t>(prec), static_cast<int32_t>(scale));
    }

    return arrow::Status::NotImplemented("unsupported type id: ", type.getName());
}

arrow::Result<arrow::Decimal128> toArrowDecimal128(const Decimal128 & value)
{
    const Int128 v = value.value;
    const int64_t high = static_cast<int64_t>(v >> 64);
    const uint64_t low = static_cast<uint64_t>(v);
    return arrow::Decimal128(high, low);
}

arrow::Result<arrow::Decimal256> toArrowDecimal256(const Decimal256 & value)
{
    // Convert DB::Int256 (boost::multiprecision) to a fixed-width 32-byte big-endian
    // two's complement representation, then let Arrow decode it.
    boost::multiprecision::cpp_int tmp = value.value;
    const bool negative = tmp < 0;
    if (negative)
    {
        tmp += (boost::multiprecision::cpp_int(1) << 256);
    }

    std::vector<uint8_t> bytes;
    bytes.reserve(32);
    boost::multiprecision::export_bits(tmp, std::back_inserter(bytes), /*bits_per_chunk=*/8, /*msb_first=*/true);

    if (bytes.size() > 32)
        return arrow::Status::Invalid("unexpected decimal256 width");

    std::array<uint8_t, 32> padded{};
    const size_t offset = 32 - bytes.size();
    for (size_t i = 0; i < bytes.size(); ++i)
        padded[offset + i] = bytes[i];

    // Preserve sign-extension if the export lost leading 0xFF bytes (should be rare).
    if (negative && offset > 0)
    {
        for (size_t i = 0; i < offset; ++i)
            padded[i] = 0xFF;
    }

    return arrow::Decimal256::FromBigEndian(padded.data(), static_cast<int32_t>(padded.size()));
}

arrow::Result<std::shared_ptr<arrow::Array>> buildUInt64Array(
    const IColumn & column,
    const ColumnNullable * nullable,
    arrow::MemoryPool * pool)
{
    arrow::UInt64Builder builder(pool);
    const auto & col = nullable != nullptr ? nullable->getNestedColumn() : column;
    const auto & data = static_cast<const ColumnUInt64 &>(col).getData();
    const size_t rows = data.size();

    for (size_t i = 0; i < rows; ++i)
    {
        if (nullable != nullptr && nullable->isNullAt(i))
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        else
            ARROW_RETURN_NOT_OK(builder.Append(data[i]));
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return out;
}

template <typename Builder, typename Col>
arrow::Result<std::shared_ptr<arrow::Array>> buildPODArray(
    const IColumn & column,
    const ColumnNullable * nullable,
    arrow::MemoryPool * pool)
{
    Builder builder(pool);
    const auto & col = nullable != nullptr ? nullable->getNestedColumn() : column;
    const auto & data = static_cast<const Col &>(col).getData();
    const size_t rows = data.size();

    for (size_t i = 0; i < rows; ++i)
    {
        if (nullable != nullptr && nullable->isNullAt(i))
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        else
            ARROW_RETURN_NOT_OK(builder.Append(data[i]));
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> buildBinaryArray(
    const IColumn & column,
    const ColumnNullable * nullable,
    arrow::MemoryPool * pool)
{
    arrow::BinaryBuilder builder(pool);
    const auto & col = nullable != nullptr ? nullable->getNestedColumn() : column;
    const size_t rows = col.size();

    if (const auto * str = typeid_cast<const ColumnString *>(&col))
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (nullable != nullptr && nullable->isNullAt(i))
            {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto ref = str->getDataAt(i);
            ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t *>(ref.data), static_cast<int32_t>(ref.size)));
        }
    }
    else if (const auto * fixed = typeid_cast<const ColumnFixedString *>(&col))
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (nullable != nullptr && nullable->isNullAt(i))
            {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto ref = fixed->getDataAt(i);
            ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t *>(ref.data), static_cast<int32_t>(ref.size)));
        }
    }
    else
    {
        return arrow::Status::NotImplemented("unsupported string column kind");
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> buildDecimalArray(
    const IColumn & column,
    const ColumnNullable * nullable,
    const std::shared_ptr<arrow::DataType> & arrow_type,
    arrow::MemoryPool * pool)
{
    if (arrow_type == nullptr)
        return arrow::Status::Invalid("arrow_type must not be null");

    const auto & col = nullable != nullptr ? nullable->getNestedColumn() : column;
    const size_t rows = col.size();

    if (arrow_type->id() == arrow::Type::DECIMAL128)
    {
        arrow::Decimal128Builder builder(arrow_type, pool);

        if (const auto * dec = typeid_cast<const ColumnDecimal<Decimal128> *>(&col))
        {
            const auto & data = dec->getData();
            for (size_t i = 0; i < rows; ++i)
            {
                if (nullable != nullptr && nullable->isNullAt(i))
                {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                ARROW_ASSIGN_OR_RAISE(const auto v, toArrowDecimal128(data[i]));
                ARROW_RETURN_NOT_OK(builder.Append(v));
            }
        }
        else if (const auto * dec64 = typeid_cast<const ColumnDecimal<Decimal64> *>(&col))
        {
            const auto & data = dec64->getData();
            for (size_t i = 0; i < rows; ++i)
            {
                if (nullable != nullptr && nullable->isNullAt(i))
                {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                // Promote to 128-bit.
                const Int64 raw = data[i].value;
                ARROW_RETURN_NOT_OK(builder.Append(arrow::Decimal128(/*high=*/raw < 0 ? -1 : 0, static_cast<uint64_t>(raw))));
            }
        }
        else if (const auto * dec32 = typeid_cast<const ColumnDecimal<Decimal32> *>(&col))
        {
            const auto & data = dec32->getData();
            for (size_t i = 0; i < rows; ++i)
            {
                if (nullable != nullptr && nullable->isNullAt(i))
                {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                const Int32 raw = data[i].value;
                ARROW_RETURN_NOT_OK(builder.Append(arrow::Decimal128(/*high=*/raw < 0 ? -1 : 0, static_cast<uint64_t>(static_cast<int64_t>(raw)))));
            }
        }
        else
        {
            return arrow::Status::NotImplemented("unsupported decimal column for decimal128");
        }

        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        return out;
    }

    if (arrow_type->id() == arrow::Type::DECIMAL256)
    {
        arrow::Decimal256Builder builder(arrow_type, pool);

        if (const auto * dec = typeid_cast<const ColumnDecimal<Decimal256> *>(&col))
        {
            const auto & data = dec->getData();
            for (size_t i = 0; i < rows; ++i)
            {
                if (nullable != nullptr && nullable->isNullAt(i))
                {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                ARROW_ASSIGN_OR_RAISE(const auto v, toArrowDecimal256(data[i]));
                ARROW_RETURN_NOT_OK(builder.Append(v));
            }
        }
        else
        {
            return arrow::Status::NotImplemented("unsupported decimal column for decimal256");
        }

        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        return out;
    }

    return arrow::Status::NotImplemented("expected Arrow decimal128/decimal256 type");
}

} // namespace

arrow::Result<std::shared_ptr<arrow::Field>> toArrowField(
    const DataTypePtr & type,
    const String & name,
    const ColumnOptions & options)
{
    if (type == nullptr)
        return arrow::Status::Invalid("type must not be null");

    const bool nullable = type->isNullable();
    const auto nested = removeNullable(type);
    if (nested == nullptr)
        return arrow::Status::Invalid("nested type must not be null");

    ARROW_ASSIGN_OR_RAISE(auto arrow_type, toArrowDataType(*nested));
    auto field = arrow::field(name, arrow_type, /*nullable=*/nullable);

    tiforth::LogicalType meta;
    if (nested->isDecimal())
    {
        meta.id = tiforth::LogicalTypeId::kDecimal;
        meta.decimal_precision = static_cast<int32_t>(getDecimalPrecision(*nested, 0));
        meta.decimal_scale = static_cast<int32_t>(getDecimalScale(*nested, 0));
    }
    else
    {
        switch (nested->getTypeId())
        {
        case TypeIndex::MyDate:
            meta.id = tiforth::LogicalTypeId::kMyDate;
            break;
        case TypeIndex::MyDateTime:
        {
            meta.id = tiforth::LogicalTypeId::kMyDateTime;
            const auto * dt = typeid_cast<const DataTypeMyDateTime *>(nested.get());
            if (dt != nullptr)
                meta.datetime_fsp = dt->getFraction();
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString:
            meta.id = tiforth::LogicalTypeId::kString;
            meta.collation_id = options.collation_id.has_value() ? *options.collation_id : 63;
            break;
        default:
            break;
        }
    }

    ARROW_ASSIGN_OR_RAISE(field, tiforth::WithLogicalTypeMetadata(field, meta));
    return field;
}

arrow::Result<std::shared_ptr<arrow::Array>> toArrowArray(
    const IColumn & column,
    const DataTypePtr & type,
    const std::shared_ptr<arrow::DataType> & arrow_type,
    arrow::MemoryPool * pool)
{
    ARROW_RETURN_NOT_OK(ensurePool(pool));
    if (type == nullptr)
        return arrow::Status::Invalid("type must not be null");
    if (arrow_type == nullptr)
        return arrow::Status::Invalid("arrow_type must not be null");

    const ColumnNullable * nullable = nullptr;
    const DataTypePtr nested = removeNullable(type);
    if (type->isNullable())
    {
        nullable = typeid_cast<const ColumnNullable *>(&column);
        if (nullable == nullptr)
            return arrow::Status::Invalid("expected ColumnNullable for nullable type");
    }

    if (nested->isDecimal())
        return buildDecimalArray(column, nullable, arrow_type, pool);

    switch (nested->getTypeId())
    {
    case TypeIndex::Int8:
        return buildPODArray<arrow::Int8Builder, ColumnInt8>(column, nullable, pool);
    case TypeIndex::Int16:
        return buildPODArray<arrow::Int16Builder, ColumnInt16>(column, nullable, pool);
    case TypeIndex::Int32:
        return buildPODArray<arrow::Int32Builder, ColumnInt32>(column, nullable, pool);
    case TypeIndex::Int64:
        return buildPODArray<arrow::Int64Builder, ColumnInt64>(column, nullable, pool);
    case TypeIndex::UInt8:
        return buildPODArray<arrow::UInt8Builder, ColumnUInt8>(column, nullable, pool);
    case TypeIndex::UInt16:
        return buildPODArray<arrow::UInt16Builder, ColumnUInt16>(column, nullable, pool);
    case TypeIndex::UInt32:
        return buildPODArray<arrow::UInt32Builder, ColumnUInt32>(column, nullable, pool);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::UInt64:
        return buildUInt64Array(column, nullable, pool);
    case TypeIndex::Float32:
        return buildPODArray<arrow::FloatBuilder, ColumnFloat32>(column, nullable, pool);
    case TypeIndex::Float64:
        return buildPODArray<arrow::DoubleBuilder, ColumnFloat64>(column, nullable, pool);
    case TypeIndex::String:
    case TypeIndex::FixedString:
        return buildBinaryArray(column, nullable, pool);
    default:
        break;
    }

    return arrow::Status::NotImplemented("unsupported column conversion for type: ", nested->getName());
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> toArrowRecordBatch(
    const Block & block,
    const std::unordered_map<String, ColumnOptions> & options_by_name,
    arrow::MemoryPool * pool)
{
    ARROW_RETURN_NOT_OK(ensurePool(pool));

    const size_t num_cols = block.columns();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    fields.reserve(num_cols);
    arrays.reserve(num_cols);

    const auto rows = static_cast<int64_t>(block.rows());

    for (size_t i = 0; i < num_cols; ++i)
    {
        const auto & elem = block.getByPosition(i);
        auto column = elem.column;
        if (column == nullptr)
            return arrow::Status::Invalid("block column must not be null");
        if (column->isColumnConst())
            column = column->convertToFullColumnIfConst();

        ColumnOptions options;
        auto it = options_by_name.find(elem.name);
        if (it != options_by_name.end())
            options = it->second;

        ARROW_ASSIGN_OR_RAISE(auto field, toArrowField(elem.type, elem.name, options));
        if (field == nullptr)
            return arrow::Status::Invalid("arrow field must not be null");

        ARROW_ASSIGN_OR_RAISE(auto arrow_type, toArrowDataType(*removeNullable(elem.type)));
        ARROW_ASSIGN_OR_RAISE(auto array, toArrowArray(*column, elem.type, arrow_type, pool));
        if (array == nullptr)
            return arrow::Status::Invalid("arrow array must not be null");
        if (array->length() != rows)
            return arrow::Status::Invalid("arrow array length mismatch");

        fields.push_back(std::move(field));
        arrays.push_back(std::move(array));
    }

    auto schema = arrow::schema(std::move(fields));
    return arrow::RecordBatch::Make(schema, rows, std::move(arrays));
}

} // namespace TiForth
} // namespace DB

#endif // defined(TIFLASH_ENABLE_TIFORTH)
