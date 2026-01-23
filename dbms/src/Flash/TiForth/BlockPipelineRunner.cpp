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

#include <Flash/TiForth/BlockPipelineRunner.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Flash/TiForth/TaskGroupRunner.h>

#include <arrow/status.h>

#include <memory>
#include <utility>

#include "tiforth/operators/hash_agg.h"
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

class VectorSourceOp final : public tiforth::pipeline::SourceOp
{
public:
    explicit VectorSourceOp(std::vector<std::shared_ptr<arrow::RecordBatch>> batches_)
        : batches(std::move(batches_))
    {}

    tiforth::pipeline::PipelineSource Source(const tiforth::pipeline::PipelineContext &) override
    {
        return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &, tiforth::pipeline::ThreadId thread_id) -> tiforth::pipeline::OpResult {
            if (thread_id != 0)
                return arrow::Status::Invalid("VectorSourceOp only supports thread_id=0");
            if (next >= batches.size())
                return tiforth::pipeline::OpOutput::Finished();
            auto batch = batches[next++];
            if (batch == nullptr)
                return arrow::Status::Invalid("source batch must not be null");
            return tiforth::pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
        };
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::size_t next = 0;
};

class CollectSinkOp final : public tiforth::pipeline::SinkOp
{
public:
    explicit CollectSinkOp(std::vector<std::shared_ptr<arrow::RecordBatch>> * outputs_)
        : outputs(outputs_)
    {}

    tiforth::pipeline::PipelineSink Sink(const tiforth::pipeline::PipelineContext &) override
    {
        return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &, tiforth::pipeline::ThreadId thread_id, std::optional<tiforth::pipeline::Batch> input) -> tiforth::pipeline::OpResult {
            if (thread_id != 0)
                return arrow::Status::Invalid("CollectSinkOp only supports thread_id=0");
            if (outputs == nullptr)
                return arrow::Status::Invalid("outputs must not be null");
            if (!input.has_value())
                return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
            auto batch = std::move(*input);
            if (batch != nullptr)
                outputs->push_back(std::move(batch));
            return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
        };
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> * outputs = nullptr;
};

} // namespace

arrow::Result<std::vector<BlockConversionResult>> RunTiForthPipeOpsOnBlocks(
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block,
    std::size_t dop)
{
    ARROW_RETURN_NOT_OK(ensurePool(pool));
    if (dop != 1)
        return arrow::Status::NotImplemented("RunTiForthPipeOpsOnBlocks supports dop=1 only");

    std::vector<std::shared_ptr<arrow::RecordBatch>> input_batches;
    input_batches.reserve(input_blocks.size() + ((input_blocks.empty() && sample_block != nullptr) ? 1 : 0));

    std::shared_ptr<arrow::Schema> schema;
    if (input_blocks.empty() && sample_block != nullptr)
    {
        ARROW_ASSIGN_OR_RAISE(auto batch, toArrowRecordBatch(*sample_block, input_options_by_name, pool));
        if (batch == nullptr)
            return arrow::Status::Invalid("converted arrow batch must not be null");
        schema = batch->schema();
        input_batches.push_back(std::move(batch));
    }

    for (const auto & block : input_blocks)
    {
        ARROW_ASSIGN_OR_RAISE(auto batch, toArrowRecordBatch(block, input_options_by_name, pool));
        if (batch == nullptr)
            return arrow::Status::Invalid("converted arrow batch must not be null");

        if (schema == nullptr)
        {
            schema = batch->schema();
        }
        else if (batch->schema() == nullptr || !schema->Equals(*batch->schema(), /*check_metadata=*/true))
        {
            return arrow::Status::Invalid("input blocks schema mismatch");
        }

        input_batches.push_back(std::move(batch));
    }

    auto source_op = std::make_unique<VectorSourceOp>(std::move(input_batches));

    std::vector<std::shared_ptr<arrow::RecordBatch>> output_batches;
    auto sink_op = std::make_unique<CollectSinkOp>(&output_batches);

    tiforth::pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops.reserve(pipe_ops.size());
    for (const auto & op : pipe_ops)
    {
        if (op == nullptr)
            return arrow::Status::Invalid("pipe op must not be null");
        channel.pipe_ops.push_back(op.get());
    }

    tiforth::pipeline::LogicalPipeline logical_pipeline{"BlockRunner", {std::move(channel)}, sink_op.get()};

    ARROW_ASSIGN_OR_RAISE(auto task_groups, tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, dop));
    const auto task_ctx = MakeTaskContext();
    ARROW_RETURN_NOT_OK(RunTaskGroupsToCompletion(task_groups, task_ctx));

    std::vector<BlockConversionResult> outputs;
    outputs.reserve(output_batches.size());
    for (const auto & batch : output_batches)
    {
        if (batch == nullptr)
            return arrow::Status::Invalid("output batch must not be null");
        ARROW_ASSIGN_OR_RAISE(auto out, fromArrowRecordBatch(batch));
        outputs.push_back(std::move(out));
    }

    return outputs;
}

arrow::Result<std::vector<BlockConversionResult>> RunTiForthHashAggOnBlocks(
    const tiforth::Engine * engine,
    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> build_pipe_ops,
    std::vector<tiforth::AggKey> keys,
    std::vector<tiforth::AggFunc> aggs,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block,
    std::size_t dop)
{
    ARROW_RETURN_NOT_OK(ensurePool(pool));
    if (engine == nullptr)
        return arrow::Status::Invalid("tiforth engine must not be null");
    if (dop != 1)
        return arrow::Status::NotImplemented("RunTiForthHashAggOnBlocks supports dop=1 only");

    std::vector<std::shared_ptr<arrow::RecordBatch>> input_batches;
    input_batches.reserve(input_blocks.size() + ((input_blocks.empty() && sample_block != nullptr) ? 1 : 0));

    std::shared_ptr<arrow::Schema> schema;
    if (input_blocks.empty() && sample_block != nullptr)
    {
        ARROW_ASSIGN_OR_RAISE(auto batch, toArrowRecordBatch(*sample_block, input_options_by_name, pool));
        if (batch == nullptr)
            return arrow::Status::Invalid("converted arrow batch must not be null");
        schema = batch->schema();
        input_batches.push_back(std::move(batch));
    }

    for (const auto & block : input_blocks)
    {
        ARROW_ASSIGN_OR_RAISE(auto batch, toArrowRecordBatch(block, input_options_by_name, pool));
        if (batch == nullptr)
            return arrow::Status::Invalid("converted arrow batch must not be null");

        if (schema == nullptr)
        {
            schema = batch->schema();
        }
        else if (batch->schema() == nullptr || !schema->Equals(*batch->schema(), /*check_metadata=*/true))
        {
            return arrow::Status::Invalid("input blocks schema mismatch");
        }

        input_batches.push_back(std::move(batch));
    }

    std::shared_ptr<tiforth::HashAggState> agg_state =
        std::make_shared<tiforth::HashAggState>(
            engine,
            std::move(keys),
            std::move(aggs),
            /*grouper_factory=*/tiforth::HashAggState::GrouperFactory{},
            pool,
            dop);

    // Build pipeline: source -> (optional pipes) -> hash agg sink.
    {
        auto source_op = std::make_unique<VectorSourceOp>(std::move(input_batches));
        auto sink_op = std::make_unique<tiforth::HashAggSinkOp>(agg_state);

        tiforth::pipeline::LogicalPipeline::Channel channel;
        channel.source_op = source_op.get();
        channel.pipe_ops.reserve(build_pipe_ops.size());
        for (const auto & op : build_pipe_ops)
        {
            if (op == nullptr)
                return arrow::Status::Invalid("build pipe op must not be null");
            channel.pipe_ops.push_back(op.get());
        }

        tiforth::pipeline::LogicalPipeline logical_pipeline{"HashAggBuild", {std::move(channel)}, sink_op.get()};
        ARROW_ASSIGN_OR_RAISE(auto task_groups, tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, dop));
        const auto task_ctx = MakeTaskContext();
        ARROW_RETURN_NOT_OK(RunTaskGroupsToCompletion(task_groups, task_ctx));
    }

    // Result pipeline: hash agg result source -> collect sink.
    std::vector<std::shared_ptr<arrow::RecordBatch>> output_batches;
    {
        auto source_op = std::make_unique<tiforth::HashAggResultSourceOp>(agg_state, /*max_output_rows=*/1 << 30);
        auto sink_op = std::make_unique<CollectSinkOp>(&output_batches);

        tiforth::pipeline::LogicalPipeline::Channel channel;
        channel.source_op = source_op.get();
        channel.pipe_ops = {};

        tiforth::pipeline::LogicalPipeline logical_pipeline{"HashAggResult", {std::move(channel)}, sink_op.get()};
        ARROW_ASSIGN_OR_RAISE(auto task_groups, tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, dop));
        const auto task_ctx = MakeTaskContext();
        ARROW_RETURN_NOT_OK(RunTaskGroupsToCompletion(task_groups, task_ctx));
    }

    std::vector<BlockConversionResult> outputs;
    outputs.reserve(output_batches.size());
    for (const auto & batch : output_batches)
    {
        if (batch == nullptr)
            return arrow::Status::Invalid("output batch must not be null");
        ARROW_ASSIGN_OR_RAISE(auto out, fromArrowRecordBatch(batch));
        outputs.push_back(std::move(out));
    }

    return outputs;
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
