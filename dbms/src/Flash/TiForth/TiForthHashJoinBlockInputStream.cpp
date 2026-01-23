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

#include <Flash/TiForth/TiForthHashJoinBlockInputStream.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/Exception.h>
#include <Flash/TiForth/ArrowBlockConversion.h>

#include <arrow/status.h>

#include <algorithm>
#include <string>

#include "tiforth/engine.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/projection.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

namespace DB::TiForth
{

namespace
{

Block makeHeader(const NamesAndTypesList & columns)
{
    Block header;
    for (const auto & col : columns)
    {
        if (col.type == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin output column type must not be null");
        header.insert({col.type->createColumn(), col.type, col.name});
    }
    return header;
}

const char * taskStateName(const tiforth::TaskState state)
{
    switch (state)
    {
    case tiforth::TaskState::kNeedInput:
        return "NeedInput";
    case tiforth::TaskState::kHasOutput:
        return "HasOutput";
    case tiforth::TaskState::kFinished:
        return "Finished";
    case tiforth::TaskState::kCancelled:
        return "Cancelled";
    case tiforth::TaskState::kWaiting:
        return "Waiting";
    case tiforth::TaskState::kWaitForNotify:
        return "WaitForNotify";
    case tiforth::TaskState::kIOIn:
        return "IOIn";
    case tiforth::TaskState::kIOOut:
        return "IOOut";
    }
    return "Unknown";
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

TiForthHashJoinBlockInputStream::TiForthHashJoinBlockInputStream(
    const BlockInputStreamPtr & probe_stream_,
    const BlockInputStreamPtr & build_stream_,
    std::vector<String> probe_key_names_,
    std::vector<String> build_key_names_,
    const NamesAndTypesList & output_columns_,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
    std::shared_ptr<arrow::MemoryPool> pool_holder_,
    const Block & sample_probe_block_,
    const Block & sample_build_block_)
    : probe_stream(probe_stream_)
    , build_stream(build_stream_)
    , probe_key_names(std::move(probe_key_names_))
    , build_key_names(std::move(build_key_names_))
    , output_columns(output_columns_)
    , input_options_by_name(input_options_by_name_)
    , pool_holder(std::move(pool_holder_))
    , sample_probe_block(sample_probe_block_)
    , sample_build_block(sample_build_block_)
{
    RUNTIME_CHECK_MSG(probe_stream != nullptr, "probe stream must not be null");
    RUNTIME_CHECK_MSG(build_stream != nullptr, "build stream must not be null");
    RUNTIME_CHECK_MSG(pool_holder != nullptr, "arrow memory pool must not be null");
    RUNTIME_CHECK_MSG(!probe_key_names.empty(), "probe join keys must not be empty");
    RUNTIME_CHECK_MSG(probe_key_names.size() == build_key_names.size(), "join key arity mismatch");

    children.push_back(build_stream);
    children.push_back(probe_stream);
}

TiForthHashJoinBlockInputStream::~TiForthHashJoinBlockInputStream() = default;

Block TiForthHashJoinBlockInputStream::getHeader() const
{
    return makeHeader(output_columns);
}

void TiForthHashJoinBlockInputStream::readPrefixImpl()
{
    if (prefix_called)
        return;
    prefix_called = true;
}

void TiForthHashJoinBlockInputStream::readSuffixImpl()
{
    if (suffix_called)
        return;
    suffix_called = true;
}

void TiForthHashJoinBlockInputStream::initOnce()
{
    if (initialized)
        return;
    initialized = true;

    std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches;
    bool built_any = false;
    while (Block block = build_stream->read())
    {
        auto batch_res = toArrowRecordBatch(block, input_options_by_name, pool_holder.get());
        if (!batch_res.ok())
            throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
        build_batches.push_back(batch_res.ValueOrDie());
        built_any = true;
    }
    if (!built_any)
    {
        auto batch_res = toArrowRecordBatch(sample_build_block, input_options_by_name, pool_holder.get());
        if (!batch_res.ok())
            throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
        build_batches.push_back(batch_res.ValueOrDie());
    }

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool_holder.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    if (!engine_res.ok())
        throw Exception(engine_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    engine = std::move(engine_res).ValueOrDie();
    if (engine == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth engine must not be null");

    auto builder_res = tiforth::PipelineBuilder::Create(engine.get());
    if (!builder_res.ok())
        throw Exception(builder_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    auto builder = std::move(builder_res).ValueOrDie();
    if (builder == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth pipeline builder must not be null");

    tiforth::JoinKey join_key;
    join_key.left.reserve(probe_key_names.size());
    join_key.right.reserve(build_key_names.size());
    for (size_t i = 0; i < probe_key_names.size(); ++i)
    {
        join_key.left.push_back(probe_key_names[i]);
        join_key.right.push_back(build_key_names[i]);
    }

    auto append_status = builder->AppendPipe(
        [engine = engine.get(), build_batches, join_key, pool = pool_holder.get()]() mutable
            -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
            return std::make_unique<tiforth::HashJoinPipeOp>(engine, std::move(build_batches), std::move(join_key), pool);
        });
    if (!append_status.ok())
        throw Exception(append_status.ToString(), ErrorCodes::LOGICAL_ERROR);

    if (output_columns.size() != sample_probe_block.columns() + sample_build_block.columns())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TiForthHashJoin output schema mismatch: cannot build stable projection");

    std::vector<tiforth::ProjectionExpr> projection;
    projection.reserve(output_columns.size());
    int field_index = 0;
    for (const auto & col : output_columns)
    {
        projection.push_back({col.name, tiforth::MakeFieldRef(field_index)});
        ++field_index;
    }

    append_status = builder->AppendPipe(
        [engine = engine.get(), projection]() -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
            return std::make_unique<tiforth::ProjectionPipeOp>(engine, projection);
        });
    if (!append_status.ok())
        throw Exception(append_status.ToString(), ErrorCodes::LOGICAL_ERROR);

    auto pipeline_res = builder->Finalize();
    if (!pipeline_res.ok())
        throw Exception(pipeline_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    pipeline = std::move(pipeline_res).ValueOrDie();
    if (pipeline == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth pipeline must not be null");

    auto task_res = pipeline->CreateTask();
    if (!task_res.ok())
        throw Exception(task_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    task = std::move(task_res).ValueOrDie();
    if (task == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "tiforth task must not be null");
}

Block TiForthHashJoinBlockInputStream::readImpl()
{
    initOnce();
    if (finished)
        return Block();

    while (true)
    {
        RUNTIME_CHECK_MSG(task != nullptr, "tiforth task must not be null");
        auto state_res = task->Step();
        if (!state_res.ok())
            throw Exception(state_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);

        auto state = state_res.ValueOrDie();
        state = driveBlockedTaskOrThrow(*task, state);
        switch (state)
        {
        case tiforth::TaskState::kNeedInput:
        {
            if (probe_input_closed)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "TiForthHashJoin task asked for input after CloseInput() (state={})",
                    taskStateName(state));

            if (Block block = probe_stream->read())
            {
                auto batch_res = toArrowRecordBatch(block, input_options_by_name, pool_holder.get());
                if (!batch_res.ok())
                    throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                pushed_any_probe_input = true;

                auto st = task->PushInput(batch_res.ValueOrDie());
                if (!st.ok())
                    throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);
            }
            else
            {
                if (!pushed_any_probe_input)
                {
                    auto batch_res = toArrowRecordBatch(sample_probe_block, input_options_by_name, pool_holder.get());
                    if (!batch_res.ok())
                        throw Exception(batch_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                    pushed_any_probe_input = true;

                    auto st = task->PushInput(batch_res.ValueOrDie());
                    if (!st.ok())
                        throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);
                }

                auto st = task->CloseInput();
                if (!st.ok())
                    throw Exception(st.ToString(), ErrorCodes::LOGICAL_ERROR);
                probe_input_closed = true;
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin expected non-null output batch");

            auto out_res = fromArrowRecordBatch(out_batch);
            if (!out_res.ok())
                throw Exception(out_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
            auto out = std::move(out_res).ValueOrDie();

            Block reordered;
            const auto header = getHeader();
            for (const auto & elem : header)
            {
                if (!out.block.has(elem.name))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "TiForthHashJoin output is missing required column: {}",
                        elem.name);
                reordered.insert(out.block.getByName(elem.name));
            }
            return reordered;
        }
        case tiforth::TaskState::kFinished:
            finished = true;
            return Block();
        case tiforth::TaskState::kCancelled:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin task is cancelled");
        case tiforth::TaskState::kWaiting:
        case tiforth::TaskState::kWaitForNotify:
        case tiforth::TaskState::kIOIn:
        case tiforth::TaskState::kIOOut:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected blocked task state after driveBlockedTaskOrThrow (state={})",
                taskStateName(state));
        }
    }
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
