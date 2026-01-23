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

#include <Flash/Executor/QueryExecutor.h>
#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/memory_pool.h>

#include <memory>
#include <vector>

#include "tiforth/pipeline/op/op.h"

namespace tiforth
{
class Engine;
} // namespace tiforth

namespace DB
{
class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

namespace TiForth
{

/// QueryExecutor implementation that runs a TiForth pipeline on Blocks produced by a BlockInputStream.
/// Currently only used for gtest integration; the initial supported shape is a pass-through pipeline.
class TiForthQueryExecutor final : public DB::QueryExecutor
{
public:
    TiForthQueryExecutor(
        const MemoryTrackerPtr & memory_tracker_,
        Context & context_,
        const String & req_id,
        const BlockInputStreamPtr & input_stream_,
        std::unique_ptr<tiforth::Engine> engine_,
        std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops_,
        std::unordered_map<String, ColumnOptions> input_options_by_name_,
        std::shared_ptr<arrow::MemoryPool> pool_holder_);

    ~TiForthQueryExecutor() override;

    static arrow::Result<std::unique_ptr<TiForthQueryExecutor>> CreatePassThrough(
        const MemoryTrackerPtr & memory_tracker,
        Context & context,
        const String & req_id,
        const BlockInputStreamPtr & input_stream,
        const std::unordered_map<String, ColumnOptions> & input_options_by_name,
        std::shared_ptr<arrow::MemoryPool> pool);

    String toString() const override;

    void cancel() override;

    int estimateNewThreadCount() override;

    UInt64 collectCPUTimeNs() override;

    Block getSampleBlock() const override;

    BaseRuntimeStatistics getRuntimeStatistics() const override;

protected:
    ExecutionResult execute(ResultHandler && result_handler) override;

private:
    BlockInputStreamPtr input_stream;
    std::unique_ptr<tiforth::Engine> engine;
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
    std::unordered_map<String, ColumnOptions> input_options_by_name;
    std::shared_ptr<arrow::MemoryPool> pool_holder;
    Block sample_block;
};

} // namespace TiForth

} // namespace DB

#endif // defined(TIFLASH_ENABLE_TIFORTH)
