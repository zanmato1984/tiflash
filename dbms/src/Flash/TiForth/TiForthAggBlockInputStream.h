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
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/memory_pool.h>

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/pipeline/task_groups.h"
#include "tiforth/task/resumer.h"
#include "tiforth/task/task_context.h"

namespace tiforth
{
class Engine;
} // namespace tiforth

namespace DB::TiForth
{

class TiForthAggBlockInputStream final : public IProfilingBlockInputStream
{
public:
    TiForthAggBlockInputStream(
        const BlockInputStreamPtr & input_stream_,
        std::unique_ptr<tiforth::Engine> engine_,
        std::vector<tiforth::AggKey> keys_,
        std::vector<tiforth::AggFunc> aggs_,
        const NamesAndTypesList & output_columns_,
        const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
        std::shared_ptr<arrow::MemoryPool> pool_holder_,
        const Block & sample_input_block_);

    String getName() const override { return "TiForthAgg"; }

    Block getHeader() const override;

protected:
    void readPrefixImpl() override;

    Block readImpl() override;

    void readSuffixImpl() override;

private:
    void initOnce();
    void DriveUntilOutputOrFinished();

    BlockInputStreamPtr input_stream;
    std::unique_ptr<tiforth::Engine> engine;
    std::vector<tiforth::AggKey> keys;
    std::vector<tiforth::AggFunc> aggs;
    std::shared_ptr<tiforth::HashAggState> agg_state;
    std::unique_ptr<tiforth::pipeline::SourceOp> result_source_op;
    std::unique_ptr<tiforth::pipeline::SinkOp> sink_op;
    tiforth::task::TaskGroups task_groups;
    tiforth::task::TaskContext task_ctx;
    std::size_t next_group = 0;
    std::optional<Block> next_output;
    tiforth::task::ResumerPtr output_resumer;
    NamesAndTypesList output_columns;
    std::unordered_map<String, ColumnOptions> input_options_by_name;
    std::shared_ptr<arrow::MemoryPool> pool_holder;
    Block sample_input_block;

    bool initialized = false;
    bool prefix_called = false;
    bool suffix_called = false;
    bool finished = false;
};

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
