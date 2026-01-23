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
#include <Flash/TiForth/TaskGroupRunner.h>

#include <arrow/status.h>

#include <thread>
#include <utility>

#include "tiforth/engine.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/pipeline_context.h"

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

class SingleOutputSinkOp final : public tiforth::pipeline::SinkOp
{
public:
    SingleOutputSinkOp(std::optional<Block> * next_output_, tiforth::task::ResumerPtr * output_resumer_, Block output_header_)
        : next_output(next_output_)
        , output_resumer(output_resumer_)
        , output_header(std::move(output_header_))
    {}

    tiforth::pipeline::PipelineSink Sink(const tiforth::pipeline::PipelineContext &) override
    {
        return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &, tiforth::pipeline::ThreadId thread_id, std::optional<tiforth::pipeline::Batch> input) -> tiforth::pipeline::OpResult {
            if (thread_id != 0)
                return arrow::Status::Invalid("SingleOutputSinkOp only supports thread_id=0");
            if (next_output == nullptr || output_resumer == nullptr)
                return arrow::Status::Invalid("sink output pointers must not be null");

            if (!input.has_value())
            {
                if (*output_resumer != nullptr && !(*output_resumer)->IsResumed())
                    return tiforth::pipeline::OpOutput::Blocked(*output_resumer);
                output_resumer->reset();
                return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
            }

            if (next_output->has_value())
                return arrow::Status::Invalid("SingleOutputSinkOp output buffer is full");

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

            *next_output = std::move(reordered);
            *output_resumer = std::make_shared<SimpleResumer>();
            return tiforth::pipeline::OpOutput::Blocked(*output_resumer);
        };
    }

private:
    std::optional<Block> * next_output = nullptr;
    tiforth::task::ResumerPtr * output_resumer = nullptr;
    Block output_header;
};

} // namespace

TiForthPipelineBlockInputStream::TiForthPipelineBlockInputStream(
    const String & name_,
    const BlockInputStreamPtr & input_stream_,
    std::unique_ptr<tiforth::Engine> engine_,
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops_,
    const NamesAndTypesList & output_columns_,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
    std::shared_ptr<arrow::MemoryPool> pool_holder_,
    const Block & sample_input_block_)
    : name(name_)
    , input_stream(input_stream_)
    , engine(std::move(engine_))
    , pipe_ops(std::move(pipe_ops_))
    , output_columns(output_columns_)
    , input_options_by_name(input_options_by_name_)
    , pool_holder(std::move(pool_holder_))
    , sample_input_block(sample_input_block_)
{
    RUNTIME_CHECK_MSG(!name.empty(), "name must not be empty");
    RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
    RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
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

    const auto output_header = getHeader();
    source_op = std::make_unique<BlockInputStreamSourceOp>(input_stream, input_options_by_name, pool_holder.get(), sample_input_block);
    sink_op = std::make_unique<SingleOutputSinkOp>(&next_output, &output_resumer, output_header);

    tiforth::pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops.reserve(pipe_ops.size());
    for (const auto & op : pipe_ops)
    {
        if (op == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline pipe op must not be null");
        channel.pipe_ops.push_back(op.get());
    }

    tiforth::pipeline::LogicalPipeline logical_pipeline{name, {std::move(channel)}, sink_op.get()};

    auto groups_res = tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1);
    if (!groups_res.ok())
        throw Exception(groups_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    task_groups = std::move(groups_res).ValueOrDie();
    task_ctx = MakeTaskContext();
}

void TiForthPipelineBlockInputStream::DriveUntilOutputOrFinished()
{
    while (!finished && !next_output.has_value())
    {
        if (next_group >= task_groups.size())
        {
            finished = true;
            return;
        }

        const auto & group = task_groups[next_group];
        if (group.NumTasks() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline only supports NumTasks=1 for now");

        auto st_res = group.GetTask()(task_ctx, /*task_id=*/0);
        if (!st_res.ok())
            throw Exception(st_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
        auto st = std::move(st_res).ValueOrDie();

        if (st.IsContinue())
            continue;
        if (st.IsYield())
        {
            std::this_thread::yield();
            continue;
        }
        if (st.IsCancelled())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline task is cancelled");
        if (st.IsBlocked())
        {
            auto drive_res = DriveAwaiter(st.GetAwaiter());
            if (!drive_res.ok())
                throw Exception(drive_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);

            if (*drive_res == AwaiterDriveResult::kWaitingExternal)
            {
                if (!next_output.has_value())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline blocked on external resumer without output");
                return;
            }
            continue;
        }
        if (!st.IsFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected task status");

        if (group.GetContinuation().has_value())
        {
            const auto & cont = *group.GetContinuation();
            while (true)
            {
                auto cont_res = cont(task_ctx);
                if (!cont_res.ok())
                    throw Exception(cont_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                auto cont_st = std::move(cont_res).ValueOrDie();

                if (cont_st.IsFinished())
                    break;
                if (cont_st.IsContinue())
                    continue;
                if (cont_st.IsYield())
                {
                    std::this_thread::yield();
                    continue;
                }
                if (cont_st.IsCancelled())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline continuation is cancelled");
                if (cont_st.IsBlocked())
                {
                    auto drive_cont = DriveAwaiter(cont_st.GetAwaiter());
                    if (!drive_cont.ok())
                        throw Exception(drive_cont.status().ToString(), ErrorCodes::LOGICAL_ERROR);
                    if (*drive_cont == AwaiterDriveResult::kWaitingExternal)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthPipeline continuation blocked on external resumer");
                    continue;
                }
                throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected continuation status");
            }
        }

        auto notify_st = group.NotifyFinish(task_ctx);
        if (!notify_st.ok())
            throw Exception(notify_st.ToString(), ErrorCodes::LOGICAL_ERROR);
        ++next_group;
    }
}

Block TiForthPipelineBlockInputStream::readImpl()
{
    initOnce();
    if (finished)
        return {};

    DriveUntilOutputOrFinished();

    if (!next_output.has_value())
        return {};

    Block out = std::move(*next_output);
    next_output.reset();
    if (output_resumer != nullptr && !output_resumer->IsResumed())
        output_resumer->Resume();
    return out;
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
