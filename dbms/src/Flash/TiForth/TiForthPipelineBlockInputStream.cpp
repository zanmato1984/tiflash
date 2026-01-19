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

#include <Flash/TiForth/TiForthPipelineBlockInputStream.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/Exception.h>
#include <Flash/TiForth/ArrowBlockConversion.h>

#include <arrow/status.h>

#include "tiforth/engine.h"
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline output column type must not be null");
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

TiForthPipelineBlockInputStream::TiForthPipelineBlockInputStream(
    const String & name_,
    const BlockInputStreamPtr & input_stream_,
    std::unique_ptr<tiforth::Engine> engine_,
    std::unique_ptr<tiforth::Pipeline> pipeline_,
    const NamesAndTypesList & output_columns_,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
    std::shared_ptr<arrow::MemoryPool> pool_holder_,
    const Block & sample_input_block_)
    : name(name_)
    , input_stream(input_stream_)
    , engine(std::move(engine_))
    , pipeline(std::move(pipeline_))
    , output_columns(output_columns_)
    , input_options_by_name(input_options_by_name_)
    , pool_holder(std::move(pool_holder_))
    , sample_input_block(sample_input_block_)
{
    RUNTIME_CHECK_MSG(!name.empty(), "name must not be empty");
    RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
    RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
    RUNTIME_CHECK_MSG(pipeline != nullptr, "tiforth pipeline must not be null");
    RUNTIME_CHECK_MSG(pool_holder != nullptr, "arrow memory pool must not be null");
    children.push_back(input_stream);
}

TiForthPipelineBlockInputStream::~TiForthPipelineBlockInputStream() = default;

Block TiForthPipelineBlockInputStream::getHeader() const
{
    return makeHeader(output_columns);
}

void TiForthPipelineBlockInputStream::readPrefixImpl()
{
    if (prefix_called)
        return;
    prefix_called = true;
}

void TiForthPipelineBlockInputStream::readSuffixImpl()
{
    if (suffix_called)
        return;
    suffix_called = true;
}

void TiForthPipelineBlockInputStream::initOnce()
{
    if (initialized)
        return;
    initialized = true;

    auto task_res = pipeline->CreateTask();
    if (!task_res.ok())
        throw Exception(task_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    task = std::move(task_res).ValueOrDie();
    if (task == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline task must not be null");
}

Block TiForthPipelineBlockInputStream::readImpl()
{
    initOnce();
    if (finished)
        return Block();

    while (true)
    {
        RUNTIME_CHECK_MSG(task != nullptr, "TiForthPipeline task must not be null");
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
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "TiForthPipeline task asked for input after CloseInput() (state={})",
                    taskStateName(state));

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
                    auto batch_res = toArrowRecordBatch(sample_input_block, input_options_by_name, pool_holder.get());
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline expected non-null output batch");

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
                        "TiForthPipeline output is missing required column: {}",
                        elem.name);
                reordered.insert(out.block.getByName(elem.name));
            }
            return reordered;
        }
        case tiforth::TaskState::kFinished:
            finished = true;
            return Block();
        case tiforth::TaskState::kCancelled:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline task is cancelled");
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
