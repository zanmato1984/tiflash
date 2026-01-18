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

#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/plan.h"
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

template <typename CreateTaskFn>
arrow::Result<std::vector<BlockConversionResult>> RunTiForthOnBlocksImpl(
    CreateTaskFn && create_task,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block)
{
    ARROW_RETURN_NOT_OK(ensurePool(pool));

    ARROW_ASSIGN_OR_RAISE(auto task, create_task());
    if (task == nullptr)
        return arrow::Status::Invalid("task must not be null");

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

    size_t next_input = 0;
    bool input_closed = false;
    std::vector<BlockConversionResult> outputs;

    while (true)
    {
        ARROW_ASSIGN_OR_RAISE(const auto state, task->Step());
        switch (state)
        {
        case tiforth::TaskState::kNeedInput:
        {
            if (next_input < input_batches.size())
            {
                ARROW_RETURN_NOT_OK(task->PushInput(input_batches[next_input]));
                ++next_input;
            }
            else if (!input_closed)
            {
                ARROW_RETURN_NOT_OK(task->CloseInput());
                input_closed = true;
            }
            break;
        }
        case tiforth::TaskState::kHasOutput:
        {
            ARROW_ASSIGN_OR_RAISE(auto out_batch, task->PullOutput());
            if (out_batch == nullptr)
                return arrow::Status::Invalid("expected non-null output batch");
            ARROW_ASSIGN_OR_RAISE(auto out, fromArrowRecordBatch(out_batch));
            outputs.push_back(std::move(out));
            break;
        }
        case tiforth::TaskState::kFinished:
            return outputs;
        case tiforth::TaskState::kCancelled:
            return arrow::Status::Cancelled("task is cancelled");
        case tiforth::TaskState::kWaiting:
        case tiforth::TaskState::kWaitForNotify:
        case tiforth::TaskState::kIOIn:
        case tiforth::TaskState::kIOOut:
            return arrow::Status::NotImplemented("task is blocked (IO/await/notify not wired)");
        }
    }
}

} // namespace

arrow::Result<std::vector<BlockConversionResult>> RunTiForthPipelineOnBlocks(
    const tiforth::Pipeline & pipeline,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block)
{
    return RunTiForthOnBlocksImpl(
        [&pipeline]() { return pipeline.CreateTask(); }, input_blocks, input_options_by_name, pool, sample_block);
}

arrow::Result<std::vector<BlockConversionResult>> RunTiForthPlanOnBlocks(
    const tiforth::Plan & plan,
    const std::vector<Block> & input_blocks,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name,
    arrow::MemoryPool * pool,
    const Block * sample_block)
{
    return RunTiForthOnBlocksImpl(
        [&plan]() { return plan.CreateTask(); }, input_blocks, input_options_by_name, pool, sample_block);
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
