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
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalProjection.h>
#include <Interpreters/Context.h>

#include <utility>

#if defined(TIFLASH_ENABLE_TIFORTH)
#include <DataTypes/DataTypeNullable.h>
#include <Flash/TiForth/TiFlashMemoryPool.h>
#include <Flash/TiForth/TiForthPipelineBlockInputStream.h>
#include <Flash/TiForth/TipbExprToTiForthExpr.h>

#include <arrow/memory_pool.h>

#include "tiforth/engine.h"
#include "tiforth/operators/projection.h"
#include "tiforth/pipeline.h"
#endif // defined(TIFLASH_ENABLE_TIFORTH)

namespace DB
{
PhysicalPlanNodePtr PhysicalProjection::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Projection & projection,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    NamesAndTypes schema;
    std::vector<tipb::Expr> tipb_exprs;
    tipb_exprs.reserve(static_cast<size_t>(projection.exprs_size()));
    for (const auto & expr : projection.exprs())
    {
        auto expr_name = analyzer.getActions(expr, project_actions);
        const auto & col = project_actions->getSampleBlock().getByName(expr_name);
        schema.emplace_back(col.name, col.type);
        tipb_exprs.push_back(expr);
    }

    auto physical_projection = std::make_shared<PhysicalProjection>(
        executor_id,
        schema,
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        "projection",
        project_actions,
        std::move(tipb_exprs));
    return physical_projection;
}

PhysicalPlanNodePtr PhysicalProjection::buildNonRootFinal(
    const Context & context,
    const LoggerPtr & log,
    const String & column_prefix,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());
    auto final_project_aliases = analyzer.genNonRootFinalProjectAliases(column_prefix);
    project_actions->add(ExpressionAction::project(final_project_aliases));

    NamesAndTypes schema = child->getSchema();
    RUNTIME_CHECK(final_project_aliases.size() == schema.size());
    // replace column name of schema by alias.
    for (size_t i = 0; i < final_project_aliases.size(); ++i)
    {
        RUNTIME_CHECK(schema[i].name == final_project_aliases[i].first);
        schema[i].name = final_project_aliases[i].second;
    }

    auto physical_projection = std::make_shared<PhysicalProjection>(
        child->execId(),
        schema,
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        "final projection",
        project_actions);
    // Final Projection is not a tidb operator, so no need to record profile streams.
    physical_projection->notTiDBOperator();
    return physical_projection;
}

PhysicalPlanNodePtr PhysicalProjection::buildRootFinal(
    const Context & context,
    const LoggerPtr & log,
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info,
    const PhysicalPlanNodePtr & child,
    Int32 cte_id)
{
    RUNTIME_CHECK(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    NamesWithAliases final_project_aliases = analyzer.buildFinalProjection(
        project_actions,
        require_schema,
        output_offsets,
        column_prefix,
        keep_session_timezone_info);

    RUNTIME_CHECK(final_project_aliases.size() == output_offsets.size());

    if unlikely (cte_id >= 0)
        for (size_t i = 0; i < final_project_aliases.size(); i++)
            final_project_aliases[i].second = genNameForCTESource(cte_id, i);

    project_actions->add(ExpressionAction::project(final_project_aliases));

    NamesAndTypes schema;
    for (size_t i = 0; i < final_project_aliases.size(); ++i)
    {
        const auto & alias = final_project_aliases[i].second;
        RUNTIME_CHECK(!alias.empty());
        const auto & type = analyzer.getCurrentInputColumns()[output_offsets[i]].type;
        schema.emplace_back(alias, type);
    }

    auto physical_projection = std::make_shared<PhysicalProjection>(
        child->execId(),
        schema,
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        "final projection",
        project_actions);
    // Final Projection is not a tidb operator, so no need to record profile streams.
    physical_projection->notTiDBOperator();
    return physical_projection;
}

void PhysicalProjection::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

#if defined(TIFLASH_ENABLE_TIFORTH)
    if (context.getSettingsRef().enable_tiforth_executor && context.getSettingsRef().enable_tiforth_translate_dag && !tipb_exprs.empty())
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
            const auto & output_schema = getSchema();
            if (output_schema.size() == tipb_exprs.size())
            {
                std::vector<tiforth::ProjectionExpr> exprs;
                exprs.reserve(tipb_exprs.size());
                bool ok = true;
                for (size_t i = 0; i < tipb_exprs.size(); ++i)
                {
                    auto expr_res = DB::TiForth::TipbExprToTiForthExpr(tipb_exprs[i], child->getSchema());
                    if (!expr_res.ok() || expr_res.ValueOrDie() == nullptr)
                    {
                        ok = false;
                        break;
                    }
                    exprs.push_back({output_schema[i].name, expr_res.ValueOrDie()});
                }

                if (ok)
                {
                    auto pool_holder = DB::TiForth::MakeCurrentMemoryTrackerPoolOrDefault(arrow::default_memory_pool());

                    NamesAndTypesList output_columns;
                    for (const auto & out : output_schema)
                        output_columns.emplace_back(out.name, out.type);

                    BlockInputStreams new_streams;
                    new_streams.reserve(pipeline.streams.size());
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

                        auto builder_res = tiforth::PipelineBuilder::Create(engine.get());
                        if (!builder_res.ok() || builder_res.ValueOrDie() == nullptr)
                        {
                            ok = false;
                            break;
                        }
                        auto builder = std::move(builder_res).ValueOrDie();

                        const auto st = builder->AppendTransform(
                            [engine = engine.get(), exprs]() -> arrow::Result<tiforth::TransformOpPtr> {
                                return std::make_unique<tiforth::ProjectionTransformOp>(engine, exprs);
                            });
                        if (!st.ok())
                        {
                            ok = false;
                            break;
                        }

                        auto pipeline_res = builder->Finalize();
                        if (!pipeline_res.ok() || pipeline_res.ValueOrDie() == nullptr)
                        {
                            ok = false;
                            break;
                        }
                        auto tiforth_pipeline = std::move(pipeline_res).ValueOrDie();

                        new_streams.push_back(std::make_shared<DB::TiForth::TiForthPipelineBlockInputStream>(
                            "TiForthProjection",
                            stream,
                            std::move(engine),
                            std::move(tiforth_pipeline),
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
    }
#endif // defined(TIFLASH_ENABLE_TIFORTH)

    executeExpression(pipeline, project_actions, log, extra_info);
}

void PhysicalProjection::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    executeExpression(exec_context, group_builder, project_actions, log);
}

void PhysicalProjection::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);

    /// we can add a project action to remove the useless column for empty actions.
    if (project_actions->getActions().empty())
        PhysicalPlanHelper::addParentRequireProjectAction(project_actions, parent_require);

    project_actions->finalize(parent_require);
    child->finalize(project_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(project_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalProjection::getSampleBlock() const
{
    return project_actions->getSampleBlock();
}
} // namespace DB
