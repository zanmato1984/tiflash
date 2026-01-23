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
#include <Flash/TiForth/ArrowBlockConversion.h>
#include <Flash/TiForth/TaskGroupRunner.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include <ext/scope_guard.h>

#include <thread>
#include <utility>

#include "tiforth/engine.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/pipeline_context.h"
#include "tiforth/pipeline/task_groups.h"

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

class BlockInputStreamSourceOp final : public tiforth::pipeline::SourceOp
{
public:
    BlockInputStreamSourceOp(
        BlockInputStreamPtr input_stream_,
        std::unordered_map<String, ColumnOptions> input_options_by_name_,
        arrow::MemoryPool * pool_,
        Block sample_input_block_)
        : input_stream(std::move(input_stream_))
        , input_options_by_name(std::move(input_options_by_name_))
        , pool(pool_)
        , sample_input_block(std::move(sample_input_block_))
    {}

    tiforth::pipeline::PipelineSource Source(const tiforth::pipeline::PipelineContext &) override
    {
        return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &, tiforth::pipeline::ThreadId thread_id) -> tiforth::pipeline::OpResult {
            if (thread_id != 0)
                return arrow::Status::Invalid("BlockInputStreamSourceOp only supports thread_id=0");
            if (input_stream == nullptr)
                return arrow::Status::Invalid("input stream must not be null");
            if (pool == nullptr)
                return arrow::Status::Invalid("arrow memory pool must not be null");

            if (eos)
                return tiforth::pipeline::OpOutput::Finished();

            if (Block block = input_stream->read())
            {
                ARROW_ASSIGN_OR_RAISE(auto batch, toArrowRecordBatch(block, input_options_by_name, pool));
                pushed_any_input = true;
                return tiforth::pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
            }

            eos = true;
            if (!pushed_any_input)
            {
                ARROW_ASSIGN_OR_RAISE(auto batch, toArrowRecordBatch(sample_input_block, input_options_by_name, pool));
                pushed_any_input = true;
                return tiforth::pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
            }
            return tiforth::pipeline::OpOutput::Finished();
        };
    }

private:
    BlockInputStreamPtr input_stream;
    std::unordered_map<String, ColumnOptions> input_options_by_name;
    arrow::MemoryPool * pool = nullptr;
    Block sample_input_block;

    bool eos = false;
    bool pushed_any_input = false;
};

class CallbackSinkOp final : public tiforth::pipeline::SinkOp
{
public:
    CallbackSinkOp(Block output_header_, DB::ResultHandler * result_handler_)
        : output_header(std::move(output_header_))
        , result_handler(result_handler_)
    {}

    tiforth::pipeline::PipelineSink Sink(const tiforth::pipeline::PipelineContext &) override
    {
        return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &, tiforth::pipeline::ThreadId thread_id, std::optional<tiforth::pipeline::Batch> input) -> tiforth::pipeline::OpResult {
            if (thread_id != 0)
                return arrow::Status::Invalid("CallbackSinkOp only supports thread_id=0");
            if (!input.has_value())
                return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();

            auto batch = std::move(*input);
            if (batch == nullptr)
                return arrow::Status::Invalid("sink input batch must not be null");

            ARROW_ASSIGN_OR_RAISE(auto out, fromArrowRecordBatch(batch));
            Block reordered;
            for (const auto & elem : output_header)
            {
                if (!out.block.has(elem.name))
                    return arrow::Status::Invalid("output is missing required column: ", elem.name);
                reordered.insert(out.block.getByName(elem.name));
            }

            if (result_handler != nullptr && *result_handler)
                (*result_handler)(reordered);
            return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
        };
    }

private:
    Block output_header;
    DB::ResultHandler * result_handler = nullptr;
};

} // namespace

TiForthQueryExecutor::TiForthQueryExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    Context & context_,
    const String & req_id,
    const BlockInputStreamPtr & input_stream_,
    std::unique_ptr<tiforth::Engine> engine_,
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops_,
    std::unordered_map<String, ColumnOptions> input_options_by_name_,
    std::shared_ptr<arrow::MemoryPool> pool_holder_)
    : DB::QueryExecutor(memory_tracker_, context_, req_id)
    , input_stream(input_stream_)
    , engine(std::move(engine_))
    , pipe_ops(std::move(pipe_ops_))
    , input_options_by_name(std::move(input_options_by_name_))
    , pool_holder(std::move(pool_holder_))
{
    RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
    RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
    RUNTIME_CHECK_MSG(pool_holder != nullptr, "arrow memory pool must not be null");
    sample_block = input_stream->getHeader();
}

TiForthQueryExecutor::~TiForthQueryExecutor() = default;

arrow::Result<std::unique_ptr<TiForthQueryExecutor>> TiForthQueryExecutor::CreatePassThrough(
    const MemoryTrackerPtr & memory_tracker,
    Context & context,
    const String & req_id,
    const BlockInputStreamPtr & input_stream,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    std::shared_ptr<arrow::MemoryPool> pool)
{
    if (pool == nullptr)
        return arrow::Status::Invalid("memory pool must not be null");
    ARROW_RETURN_NOT_OK(ensurePool(pool.get()));
    if (input_stream == nullptr)
        return arrow::Status::Invalid("input stream must not be null");

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool.get();
    ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(engine_options));
    if (engine == nullptr)
        return arrow::Status::Invalid("tiforth engine must not be null");

    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
    pipe_ops.push_back(std::make_unique<tiforth::PassThroughPipeOp>());

    return std::make_unique<TiForthQueryExecutor>(
        memory_tracker,
        context,
        req_id,
        input_stream,
        std::move(engine),
        std::move(pipe_ops),
        input_options_by_name,
        std::move(pool));
}

ExecutionResult TiForthQueryExecutor::execute(ResultHandler && result_handler)
{
    try
    {
        RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
        RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
        RUNTIME_CHECK_MSG(pool_holder != nullptr, "arrow memory pool must not be null");

        input_stream->readPrefix();
        SCOPE_EXIT({ input_stream->readSuffix(); });

        auto source_op = std::make_unique<BlockInputStreamSourceOp>(input_stream, input_options_by_name, pool_holder.get(), sample_block);
        auto output_header = sample_block;
        auto sink_op = std::make_unique<CallbackSinkOp>(std::move(output_header), &result_handler);

        tiforth::pipeline::LogicalPipeline::Channel channel;
        channel.source_op = source_op.get();
        channel.pipe_ops.reserve(pipe_ops.size());
        for (const auto & op : pipe_ops)
        {
            if (op == nullptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthQueryExecutor pipe op must not be null");
            channel.pipe_ops.push_back(op.get());
        }

        tiforth::pipeline::LogicalPipeline logical_pipeline{"QueryExecutor", {std::move(channel)}, sink_op.get()};

        auto groups_res = tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1);
        if (!groups_res.ok())
            throw Exception(groups_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);

        const auto task_ctx = MakeTaskContext();
        const auto st = RunTaskGroupsToCompletion(std::move(groups_res).ValueOrDie(), task_ctx);
        if (!st.ok())
            throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);

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
    return fmt::format("TiForthQueryExecutor (task groups)\n{}", fb.toString());
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
