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

#include <Core/Types.h>
#include <DataTypes/IDataType.h>

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include <optional>
#include <unordered_map>

namespace arrow
{
class Array;
class DataType;
class Field;
class Schema;
} // namespace arrow

namespace DB
{

class Block;

namespace TiForth
{

struct ColumnOptions
{
    std::optional<Int32> collation_id;
};

arrow::Result<std::shared_ptr<arrow::Field>> toArrowField(
    const DataTypePtr & type,
    const String & name,
    const ColumnOptions & options);

arrow::Result<std::shared_ptr<arrow::Array>> toArrowArray(
    const IColumn & column,
    const DataTypePtr & type,
    const std::shared_ptr<arrow::DataType> & arrow_type,
    arrow::MemoryPool * pool);

arrow::Result<std::shared_ptr<arrow::RecordBatch>> toArrowRecordBatch(
    const Block & block,
    const std::unordered_map<String, ColumnOptions> & options_by_name,
    arrow::MemoryPool * pool);

} // namespace TiForth

} // namespace DB

#endif // defined(TIFLASH_ENABLE_TIFORTH)

