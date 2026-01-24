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

#include <Core/Block.h>
#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/record_batch.h>
#include <arrow/result.h>

#include <unordered_map>

namespace DB::TiForth
{

struct BlockConversionResult
{
    Block block;

    // Side-channel metadata that is not representable in DB::Block today.
    // Currently used for string collation id roundtrip.
    std::unordered_map<String, ColumnOptions> options_by_name;
};

arrow::Result<BlockConversionResult> fromArrowRecordBatch(const std::shared_ptr<arrow::RecordBatch> & batch);

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)

