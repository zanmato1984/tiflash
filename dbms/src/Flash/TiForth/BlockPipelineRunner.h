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
#include <Flash/TiForth/ArrowBlockConversion.h>

#include <arrow/memory_pool.h>
#include <arrow/result.h>

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline/op/op.h"

namespace DB::TiForth
{

arrow::Result<std::vector<BlockConversionResult>> RunTiForthPipeOpsOnBlocks(
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block = nullptr,
    std::size_t dop = 1);

arrow::Result<std::vector<BlockConversionResult>> RunTiForthHashAggOnBlocks(
    const tiforth::Engine * engine,
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> build_pipe_ops,
    std::vector<tiforth::AggKey> keys,
    std::vector<tiforth::AggFunc> aggs,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block = nullptr,
    std::size_t dop = 1);

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
