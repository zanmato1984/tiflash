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
#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include <ext/scope_guard.h>

#include "tiforth/engine.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

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

tiforth::TaskState driveBlockedTaskOrThrow(tiforth::Task & task, tiforth::TaskState state)
{
    while (true)
    {
        switch (state)
        {
        case tiforth::TaskState::kIOIn:
        case tiforth::TaskState::kIOOut:
        {
            auto st = task.ExecuteIO();
            if (!st.ok())
                throw Exception(st.status().ToString(), ErrorCodes::LOGICAL_ERROR);
            state = st.ValueOrDie();
            break;
        }
        case tiforth::TaskState::kWaiting:
        {
            auto st = task.Await();
            if (!st.ok())
                throw Exception(st.status().ToString(), ErrorCodes::LOGICAL_ERROR);
            state = st.ValueOrDie();
            break;
        }
        case tiforth::TaskState::kWaitForNotify:
        {
            auto st = task.Notify();
            if (!st.ok())
                throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);

            auto step = task.Step();
            if (!step.ok())
                throw Exception(step.status().ToString(), ErrorCodes::LOGICAL_ERROR);
            state = step.ValueOrDie();
            break;
        }
        default:
            return state;
        }
    }
}

} // namespace

TiForthQueryExecutor::TiForthQueryExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    Context & context_,
    const String & req_id,
    const BlockInputStreamPtr & input_stream_,
    std::unique_ptr<tiforth::Engine> engine_,
    std::unique_ptr<tiforth::Pipeline> pipeline_,
    std::unordered_map<String, ColumnOptions> input_options_by_name_,
    std::shared_ptr<arrow::MemoryPool> pool_holder_)
    : DB::QueryExecutor(memory_tracker_, context_, req_id)
    , input_stream(input_stream_)
    , engine(std::move(engine_))
    , pipeline(std::move(pipeline_))
    , input_options_by_name(std::move(input_options_by_name_))
    , pool_holder(std::move(pool_holder_))
{
    RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
    RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
    RUNTIME_CHECK_MSG(pipeline != nullptr, "tiforth pipeline must not be null");
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
        input_options_by_name,
        std::move(pool));
}

ExecutionResult TiForthQueryExecutor::execute(ResultHandler && result_handler)
{
    try
    {
        RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
        RUNTIME_CHECK_MSG(pipeline != nullptr, "tiforth pipeline must not be null");
        RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");

        input_stream->readPrefix();
        SCOPE_EXIT({ input_stream->readSuffix(); });

        auto task_res = pipeline->CreateTask();
        if (!task_res.ok())
            throw Exception(task_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
        auto task = std::move(task_res).ValueOrDie();
        if (task == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth task must not be null");

        bool input_closed = false;
        bool pushed_any_input = false;

        while (true)
        {
            auto state_res = task->Step();
            if (!state_res.ok())
                throw Exception(state_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);

            auto state = state_res.ValueOrDie();
            state = driveBlockedTaskOrThrow(*task, state);
            switch (state)
            {
            case tiforth::TaskState::kNeedInput:
            {
                if (input_closed)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth task asked for input after CloseInput()");

                if (Block block = input_stream->read())
                {
                    auto batch_res = toArrowRecordBatch(block, input_options_by_name, pool_holder.get());
                    if (!batch_res.ok())
                        throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                    pushed_any_input = true;

                    auto st = task->PushInput(batch_res.ValueOrDie());
                    if (!st.ok())
                        throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);
                }
                else
                {
                    if (!pushed_any_input)
                    {
                        auto batch_res = toArrowRecordBatch(sample_block, input_options_by_name, pool_holder.get());
                        if (!batch_res.ok())
                            throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                        pushed_any_input = true;

                        auto st = task->PushInput(batch_res.ValueOrDie());
                        if (!st.ok())
                            throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);
                    }

                    auto st = task->CloseInput();
                    if (!st.ok())
                        throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);
                    input_closed = true;
                }
                break;
            }
            case tiforth::TaskState::kHasOutput:
            {
                auto batch_res = task->PullOutput();
                if (!batch_res.ok())
                    throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                const auto out_batch = std::move(batch_res).ValueOrDie();
                if (out_batch == nullptr)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "expected non-null output batch");

                auto out_res = fromArrowRecordBatch(out_batch);
                if (!out_res.ok())
                    throw Exception(out_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                const auto out = std::move(out_res).ValueOrDie();
                if (result_handler)
                    result_handler(out.block);
                break;
            }
            case tiforth::TaskState::kFinished:
                return ExecutionResult::success();
            case tiforth::TaskState::kCancelled:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth task is cancelled");
            case tiforth::TaskState::kWaiting:
            case tiforth::TaskState::kWaitForNotify:
            case tiforth::TaskState::kIOIn:
            case tiforth::TaskState::kIOOut:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected blocked task state after driveBlockedTaskOrThrow");
            }
        }
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
