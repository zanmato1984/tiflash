// Copyright 2023 PingCAP, Inc.
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

#include <Common/Logger.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalFilter.h>
#include <Interpreters/Context.h>
#include <Operators/FilterTransformOp.h>

#if defined(TIFLASH_ENABLE_TIFORTH)
#include <DataTypes/DataTypeNullable.h>
#include <Flash/TiForth/TiFlashMemoryPool.h>
#include <Flash/TiForth/TiForthPipelineBlockInputStream.h>
#include <Flash/TiForth/TipbExprToTiForthExpr.h>

#include <arrow/memory_pool.h>

#include "tiforth/engine.h"
#include "tiforth/operators/filter.h"
#endif // defined(TIFLASH_ENABLE_TIFORTH)

namespace DB
{
PhysicalPlanNodePtr PhysicalFilter::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Selection & selection,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_filter_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    String filter_column_name = analyzer.buildFilterColumn(before_filter_actions, selection.conditions(), true);

    std::vector<tipb::Expr> conditions;
    conditions.reserve(static_cast<size_t>(selection.conditions_size()));
    for (const auto & cond : selection.conditions())
        conditions.push_back(cond);

    auto physical_filter = std::make_shared<PhysicalFilter>(
        executor_id,
        child->getSchema(),
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        std::move(conditions),
        filter_column_name,
        before_filter_actions);

    return physical_filter;
}

void PhysicalFilter::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

#if defined(TIFLASH_ENABLE_TIFORTH)
    if (context.getSettingsRef().enable_tiforth_executor && context.getSettingsRef().enable_tiforth_translate_dag)
    {
        const auto input_header = pipeline.firstStream()->getHeader();
        bool has_string = false;
        for (const auto & col : input_header)
        {
            if (col.type == nullptr)
                continue;
            if (removeNullable(col.type)->isStringOrFixedString())
            {
                has_string = true;
                break;
            }
        }

        if (!has_string)
        {
            auto predicate_res = DB::TiForth::TipbSelectionConditionsToTiForthPredicate(conditions, child->getSchema());
            if (predicate_res.ok())
            {
                const auto predicate = predicate_res.ValueOrDie();
                auto pool_holder = DB::TiForth::MakeCurrentMemoryTrackerPoolOrDefault(arrow::default_memory_pool());

                NamesAndTypesList output_columns;
                for (const auto & col : input_header)
                    output_columns.emplace_back(col.name, col.type);

                BlockInputStreams new_streams;
                new_streams.reserve(pipeline.streams.size());
                bool ok = true;
                for (const auto & stream : pipeline.streams)
                {
                    tiforth::EngineOptions engine_options;
                    engine_options.memory_pool = pool_holder.get();
                    auto engine_res = tiforth::Engine::Create(engine_options);
                    if (!engine_res.ok() || engine_res.ValueOrDie() == nullptr)
                    {
                        ok = false;
                        break;
                    }
                    auto engine = std::move(engine_res).ValueOrDie();
                    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
                    pipe_ops.push_back(std::make_unique<tiforth::FilterPipeOp>(engine.get(), predicate));

                    new_streams.push_back(std::make_shared<DB::TiForth::TiForthPipelineBlockInputStream>(
                        "TiForthFilter",
                        stream,
                        std::move(engine),
                        std::move(pipe_ops),
                        output_columns,
                        /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
                        pool_holder,
                        input_header));
                }

                if (ok)
                {
                    pipeline.streams = std::move(new_streams);
                    return;
                }
            }
        }
    }
#endif // defined(TIFLASH_ENABLE_TIFORTH)

    pipeline.transform([&](auto & stream) {
        stream
            = std::make_shared<FilterBlockInputStream>(stream, before_filter_actions, filter_column, log->identifier());
    });
}

void PhysicalFilter::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    auto input_header = group_builder.getCurrentHeader();
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<FilterTransformOp>(
            exec_context,
            log->identifier(),
            input_header,
            before_filter_actions,
            filter_column));
    });
}

void PhysicalFilter::finalizeImpl(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.emplace_back(filter_column);
    before_filter_actions->finalize(required_output);

    child->finalize(before_filter_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_filter_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalFilter::getSampleBlock() const
{
    return before_filter_actions->getSampleBlock();
}
} // namespace DB
