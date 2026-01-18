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

#include <Flash/TiForth/TiForthQueryExecutor.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/engine.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/pipeline.h"

namespace DB::TiForth
{

namespace
{

arrow::Status ensurePool(arrow::MemoryPool * pool)
{
    if (pool == nullptr)
        return arrow::Status::Invalid("memory pool must not be null");
    return arrow::Status::OK();
}

} // namespace

TiForthQueryExecutor::TiForthQueryExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    Context & context_,
    const String & req_id,
    const BlockInputStreamPtr & input_stream_,
    std::unique_ptr<tiforth::Engine> engine_,
    std::unique_ptr<tiforth::Pipeline> pipeline_,
    std::unordered_map<String, ColumnOptions> input_options_by_name_)
    : DB::QueryExecutor(memory_tracker_, context_, req_id)
    , input_stream(input_stream_)
    , engine(std::move(engine_))
    , pipeline(std::move(pipeline_))
    , input_options_by_name(std::move(input_options_by_name_))
{
    RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
    RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
    RUNTIME_CHECK_MSG(pipeline != nullptr, "tiforth pipeline must not be null");
    sample_block = input_stream->getHeader();
}

TiForthQueryExecutor::~TiForthQueryExecutor() = default;

arrow::Result<std::unique_ptr<TiForthQueryExecutor>> TiForthQueryExecutor::CreatePassThrough(
    const MemoryTrackerPtr & memory_tracker,
    Context & context,
    const String & req_id,
    const BlockInputStreamPtr & input_stream,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool)
{
    ARROW_RETURN_NOT_OK(ensurePool(pool));
    if (input_stream == nullptr)
        return arrow::Status::Invalid("input stream must not be null");

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool;
    ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(engine_options));
    if (engine == nullptr)
        return arrow::Status::Invalid("tiforth engine must not be null");

    ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
    if (builder == nullptr)
        return arrow::Status::Invalid("tiforth pipeline builder must not be null");

    // Minimal integration: run a pass-through TiForth pipeline on Block streams.
    ARROW_RETURN_NOT_OK(builder->AppendTransform([]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::PassThroughTransformOp>();
    }));

    ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
    if (pipeline == nullptr)
        return arrow::Status::Invalid("tiforth pipeline must not be null");

    return std::make_unique<TiForthQueryExecutor>(
        memory_tracker,
        context,
        req_id,
        input_stream,
        std::move(engine),
        std::move(pipeline),
        input_options_by_name);
}

ExecutionResult TiForthQueryExecutor::execute(ResultHandler && result_handler)
{
    try
    {
        RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
        RUNTIME_CHECK_MSG(pipeline != nullptr, "tiforth pipeline must not be null");
        RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");

        std::vector<Block> input_blocks;
        input_stream->readPrefix();
        while (Block block = input_stream->read())
            input_blocks.push_back(std::move(block));
        input_stream->readSuffix();

        auto outputs_res
            = RunTiForthPipelineOnBlocks(*pipeline, input_blocks, input_options_by_name, engine->memory_pool());
        if (!outputs_res.ok())
            throw Exception(outputs_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
        auto outputs = std::move(outputs_res).ValueOrDie();

        if (result_handler)
        {
            for (auto & out : outputs)
                result_handler(out.block);
        }

        return ExecutionResult::success();
    }
    catch (...)
    {
        return ExecutionResult::fail(std::current_exception());
    }
}

void TiForthQueryExecutor::cancel()
{
    if (input_stream == nullptr)
        return;
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get()); p_stream)
        p_stream->cancel(/*kill=*/false);
}

String TiForthQueryExecutor::toString() const
{
    if (input_stream == nullptr)
        return "TiForthQueryExecutor (null input stream)";

    FmtBuffer fb;
    input_stream->dumpTree(fb);
    return fmt::format("TiForthQueryExecutor (pass-through)\n{}", fb.toString());
}

int TiForthQueryExecutor::estimateNewThreadCount()
{
    if (input_stream == nullptr)
        return 1;
    return std::max(input_stream->estimateNewThreadCount(), 1);
}

UInt64 TiForthQueryExecutor::collectCPUTimeNs()
{
    // Not wired: TiForth operator-level profiling is not exposed through TiFlash yet.
    return 0;
}

Block TiForthQueryExecutor::getSampleBlock() const
{
    return sample_block;
}

BaseRuntimeStatistics TiForthQueryExecutor::getRuntimeStatistics() const
{
    BaseRuntimeStatistics runtime_statistics;
    if (input_stream == nullptr)
        return runtime_statistics;
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get()))
        runtime_statistics.append(p_stream->getProfileInfo());
    return runtime_statistics;
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
