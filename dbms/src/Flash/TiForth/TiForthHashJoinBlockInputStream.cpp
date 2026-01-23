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
#include <Flash/TiForth/TaskGroupRunner.h>

#include <arrow/status.h>

#include <thread>
#include <utility>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/projection.h"
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin output column type must not be null");
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

    tiforth::JoinKey join_key;
    join_key.left.reserve(probe_key_names.size());
    join_key.right.reserve(build_key_names.size());
    for (size_t i = 0; i < probe_key_names.size(); ++i)
    {
        join_key.left.push_back(probe_key_names[i]);
        join_key.right.push_back(build_key_names[i]);
    }

    pipe_ops.clear();
    pipe_ops.push_back(std::make_unique<tiforth::HashJoinPipeOp>(engine.get(), std::move(build_batches), std::move(join_key), pool_holder.get()));

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
    pipe_ops.push_back(std::make_unique<tiforth::ProjectionPipeOp>(engine.get(), projection));

    const auto output_header = getHeader();
    source_op = std::make_unique<BlockInputStreamSourceOp>(probe_stream, input_options_by_name, pool_holder.get(), sample_probe_block);
    sink_op = std::make_unique<SingleOutputSinkOp>(&next_output, &output_resumer, output_header);

    tiforth::pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops.reserve(pipe_ops.size());
    for (const auto & op : pipe_ops)
    {
        if (op == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin pipe op must not be null");
        channel.pipe_ops.push_back(op.get());
    }

    tiforth::pipeline::LogicalPipeline logical_pipeline{"HashJoin", {std::move(channel)}, sink_op.get()};

    auto groups_res = tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1);
    if (!groups_res.ok())
        throw Exception(groups_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);
    task_groups = std::move(groups_res).ValueOrDie();
    task_ctx = MakeTaskContext();
}

void TiForthHashJoinBlockInputStream::DriveUntilOutputOrFinished()
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin only supports NumTasks=1 for now");

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin task is cancelled");
        if (st.IsBlocked())
        {
            auto drive_res = DriveAwaiter(st.GetAwaiter());
            if (!drive_res.ok())
                throw Exception(drive_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);

            if (*drive_res == AwaiterDriveResult::kWaitingExternal)
            {
                if (!next_output.has_value())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthHashJoin blocked on external resumer without output");
                return;
            }
            continue;
        }
        if (!st.IsFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected task status");

        auto notify_st = group.NotifyFinish(task_ctx);
        if (!notify_st.ok())
            throw Exception(notify_st.ToString(), ErrorCodes::LOGICAL_ERROR);
        ++next_group;
    }
}

Block TiForthHashJoinBlockInputStream::readImpl()
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

