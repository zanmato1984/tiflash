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
#include <Common/TiFlashException.h>
#include <Core/FineGrainedOperatorSpillContext.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/AutoPassThroughAggregatingBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/PipelineBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalAggregation.h>
#include <Flash/Planner/Plans/PhysicalAggregationBuild.h>
#include <Flash/Planner/Plans/PhysicalAggregationConvergent.h>
#include <Interpreters/Context.h>
#include <Operators/AutoPassThroughAggregateTransform.h>
#include <Operators/LocalAggregateTransform.h>

#if defined(TIFLASH_ENABLE_TIFORTH)
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/TiForth/TiForthAggBlockInputStream.h>
#include <Flash/TiForth/TiForthPipelineBlockInputStream.h>
#include <Flash/TiForth/TiFlashMemoryPool.h>

#include <arrow/memory_pool.h>

#include <algorithm>
#include <optional>
#include <unordered_set>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/arrow_compute_agg.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline/op/op.h"
#endif // defined(TIFLASH_ENABLE_TIFORTH)

namespace DB
{

#if defined(TIFLASH_ENABLE_TIFORTH)
namespace
{

bool containsStringGroupKeys(const Block & header, const Names & keys)
{
    for (const auto & key : keys)
    {
        const auto & type = header.getByName(key).type;
        if (removeNullable(type)->isStringOrFixedString())
            return true;
    }
    return false;
}

std::optional<BlockInputStreamPtr> tryBuildTiForthAggStream(
    const Context & context,
    const LoggerPtr & log,
    const DAGPipeline & pipeline,
    const Block & before_agg_header,
    const Names & aggregation_keys,
    const KeyRefAggFuncMap & key_ref_agg_func,
    const AggregateDescriptions & aggregate_descriptions,
    const ExpressionActionsPtr & expr_after_agg)
{
    if (!context.getSettingsRef().enable_tiforth_executor)
        return std::nullopt;
    const bool allow_tiforth_agg = context.getSettingsRef().enable_tiforth_translate_dag
        || context.getSettingsRef().enable_tiforth_arrow_compute_agg
        || context.getSettingsRef().enable_tiforth_hash_agg;
    if (!allow_tiforth_agg)
        return std::nullopt;
    if (expr_after_agg == nullptr)
        return std::nullopt;
    if (!key_ref_agg_func.empty())
        return std::nullopt;

    const auto output_cols = expr_after_agg->getRequiredColumnsWithTypes();
    if (output_cols.empty())
        return std::nullopt;

    auto pool_holder = DB::TiForth::MakeCurrentMemoryTrackerPoolOrDefault(arrow::default_memory_pool());

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool_holder.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    if (!engine_res.ok())
    {
        LOG_WARNING(log, "TiForthAgg: failed to create engine, fallback to native agg: {}", engine_res.status().ToString());
        return std::nullopt;
    }
    auto engine = std::move(engine_res).ValueOrDie();

    std::vector<tiforth::AggKey> keys;
    keys.reserve(aggregation_keys.size());
    for (const auto & key : aggregation_keys)
    {
        if (!before_agg_header.has(key))
            return std::nullopt;
        const auto & type = before_agg_header.getByName(key).type;
        if (type == nullptr)
            return std::nullopt;
        keys.push_back({key, tiforth::MakeFieldRef(key)});
    }

    std::vector<tiforth::AggFunc> aggs;
    aggs.reserve(aggregate_descriptions.size());
    for (const auto & desc : aggregate_descriptions)
    {
        if (desc.function == nullptr)
            return std::nullopt;
        const auto func = desc.function->getName();
        if (func == "count" && desc.argument_names.empty())
        {
            aggs.push_back({desc.column_name, "count_all", nullptr});
            continue;
        }
        if (func != "count" && func != "sum" && func != "avg" && func != "min" && func != "max")
            return std::nullopt;
        if (desc.argument_names.size() != 1)
            return std::nullopt;
        if (!before_agg_header.has(desc.argument_names[0]))
            return std::nullopt;
        const auto & type = before_agg_header.getByName(desc.argument_names[0]).type;
        if (type == nullptr)
            return std::nullopt;
        aggs.push_back({desc.column_name, func, tiforth::MakeFieldRef(desc.argument_names[0])});
    }

    std::unordered_set<String> produced_names;
    produced_names.reserve(keys.size() + aggs.size());
    for (const auto & key : keys)
        produced_names.insert(key.name);
    for (const auto & agg : aggs)
        produced_names.insert(agg.name);
    if (produced_names.size() != output_cols.size())
        return std::nullopt;
    for (const auto & out : output_cols)
    {
        if (produced_names.find(out.name) == produced_names.end())
            return std::nullopt;
    }

    const bool has_string_key = containsStringGroupKeys(before_agg_header, aggregation_keys);
    const bool collation_sensitive = AggregationInterpreterHelper::isGroupByCollationSensitive(context);
    const bool allow_binary_string_keys = !collation_sensitive;
    const bool allow_arrow_compute_string_keys
        = context.getSettingsRef().enable_tiforth_arrow_compute_agg_string_keys
        && allow_binary_string_keys;
    const bool use_arrow_compute = context.getSettingsRef().enable_tiforth_arrow_compute_agg
        && !aggregation_keys.empty()
        && (!has_string_key || allow_arrow_compute_string_keys);
    const bool use_arrow_compute_string_keys = use_arrow_compute && has_string_key;
    // MS17..MS21: TiForth HashAgg is Arrow-based (Grouper + hash_* kernels).
    // - single-key string grouping is allowed (binary semantics by default; collation-aware via metadata)
    const bool use_hash_agg = context.getSettingsRef().enable_tiforth_hash_agg && !use_arrow_compute && !aggregation_keys.empty()
        && (!has_string_key || aggregation_keys.size() == 1);

    size_t max_threads = static_cast<size_t>(context.getSettingsRef().max_threads);
    if (max_threads == 0)
        max_threads = pipeline.streams.size();
    max_threads = std::min(max_threads, pipeline.streams.size());
    max_threads = std::max<size_t>(1, max_threads);

    const BlockInputStreamPtr input_stream
        = pipeline.streams.size() == 1
        ? pipeline.streams.front()
        : std::make_shared<UnionBlockInputStream<>>(
            pipeline.streams,
            BlockInputStreams{},
            max_threads,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log->identifier());

    if (!use_arrow_compute && !use_hash_agg)
        return std::nullopt;

    BlockInputStreamPtr tiforth_stream;
    if (use_arrow_compute)
    {
        std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
        tiforth::ArrowComputeAggOptions options;
        options.stable_dictionary_encode_binary_keys = use_arrow_compute_string_keys;
        pipe_ops.push_back(std::make_unique<tiforth::ArrowComputeAggPipeOp>(engine.get(), keys, aggs, options));

        tiforth_stream = std::make_shared<DB::TiForth::TiForthPipelineBlockInputStream>(
            "TiForthAggArrowCompute",
            input_stream,
            std::move(engine),
            std::move(pipe_ops),
            output_cols,
            /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
            std::move(pool_holder),
            before_agg_header);
    }
    else
    {
        RUNTIME_CHECK(use_hash_agg);
        tiforth_stream = std::make_shared<DB::TiForth::TiForthAggBlockInputStream>(
            input_stream,
            std::move(engine),
            std::move(keys),
            std::move(aggs),
            output_cols,
            /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
            std::move(pool_holder),
            before_agg_header);
    }

    if (has_string_key)
    {
        if (use_arrow_compute_string_keys)
            LOG_DEBUG(log, "TiForthAgg: string group keys detected, using ArrowComputeAgg stable dictionary mode");
        else if (use_hash_agg)
        {
            if (collation_sensitive)
                LOG_DEBUG(log, "TiForthAgg: string group keys detected, using HashAgg (collation-aware via metadata)");
            else
                LOG_DEBUG(log, "TiForthAgg: string group keys detected, using HashAgg (binary semantics)");
        }
        else
            LOG_DEBUG(log, "TiForthAgg: string group keys detected, fallback to native agg");
    }
    return tiforth_stream;
}

} // namespace
#endif // defined(TIFLASH_ENABLE_TIFORTH)

PhysicalPlanNodePtr PhysicalAggregation::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Aggregation & aggregation,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    if (unlikely(aggregation.group_by_size() == 0 && aggregation.agg_func_size() == 0))
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_agg_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());
    NamesAndTypes aggregated_columns;
    AggregateDescriptions aggregate_descriptions;
    Names aggregation_keys;
    // key_ref_agg_func and agg_func_ref_key are two optimizations for aggregation:
    // 1. key_ref_agg_func: for group by key with collation, there will always be a first_row agg func
    //    to help keep the original column. For these key columns, no need to copy it from HashMap to result column,
    //    instead a pointer to reference to their corresponding first_row agg func is enough.
    // 2. agg_func_ref_key: for group by key without collation and corresponding first_row exists.
    //    We can eliminate their first_row func, a pointer to reference the group by key is enough.
    //    So we can avoid unnecessary agg func computation, also it's good for memory usage.
    KeyRefAggFuncMap key_ref_agg_func;
    AggFuncRefKeyMap agg_func_ref_key;

    std::unordered_map<String, TiDB::TiDBCollatorPtr> collators;
    {
        std::unordered_set<String> agg_key_set;
        const bool collation_sensitive = AggregationInterpreterHelper::isGroupByCollationSensitive(context);
        analyzer.buildAggFuncs(aggregation, before_agg_actions, aggregate_descriptions, aggregated_columns);
        analyzer.buildAggGroupBy(
            aggregation.group_by(),
            before_agg_actions,
            aggregate_descriptions,
            aggregated_columns,
            aggregation_keys,
            agg_key_set,
            key_ref_agg_func,
            collation_sensitive,
            collators);
        analyzer.tryEliminateFirstRow(aggregation_keys, collators, agg_func_ref_key, aggregate_descriptions);
    }

    auto expr_after_agg_actions
        = analyzer.appendCopyColumnAfterAgg(aggregated_columns, key_ref_agg_func, agg_func_ref_key);
    analyzer.appendCastAfterAgg(expr_after_agg_actions, aggregation);
    /// project action after aggregation to remove useless columns.
    auto schema = PhysicalPlanHelper::addSchemaProjectAction(expr_after_agg_actions, analyzer.getCurrentInputColumns());

    AutoPassThroughSwitcher auto_pass_through_switcher(aggregation);
    if (aggregation_keys.empty())
    {
        LOG_DEBUG(log, "switch off auto pass through because agg keys_size is zero");
        auto_pass_through_switcher.mode = ::tipb::TiFlashPreAggMode::ForcePreAgg;
    }

    auto physical_agg = std::make_shared<PhysicalAggregation>(
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        child,
        before_agg_actions,
        aggregation_keys,
        key_ref_agg_func,
        agg_func_ref_key,
        collators,
        AggregationInterpreterHelper::isFinalAgg(aggregation),
        auto_pass_through_switcher,
        aggregate_descriptions,
        expr_after_agg_actions);
    return physical_agg;
}

void PhysicalAggregation::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    // Agg cannot be both auto pass through and fine grained shuffle at the same time.
    // Fine grained shuffle is for 2nd agg, auto pass through is for 1st agg.
    RUNTIME_CHECK(!(fine_grained_shuffle.enabled() && auto_pass_through_switcher.enabled()));

    child->buildBlockInputStream(pipeline, context, max_streams);

    executeExpression(pipeline, before_agg_actions, log, "before aggregation");

#if defined(TIFLASH_ENABLE_TIFORTH)
    if (is_final_agg && !fine_grained_shuffle.enabled() && !auto_pass_through_switcher.enabled())
    {
        const auto before_agg_header = pipeline.firstStream()->getHeader();
        if (auto tiforth_stream = tryBuildTiForthAggStream(
                context,
                log,
                pipeline,
                before_agg_header,
                aggregation_keys,
                key_ref_agg_func,
                aggregate_descriptions,
                expr_after_agg))
        {
            pipeline.streams.resize(1);
            pipeline.firstStream() = std::move(*tiforth_stream);
            RUNTIME_CHECK(expr_after_agg && !expr_after_agg->getActions().empty());
            executeExpression(pipeline, expr_after_agg, log, "expr after aggregation");
            return;
        }
    }
#endif // defined(TIFLASH_ENABLE_TIFORTH)

    Block before_agg_header = pipeline.firstStream()->getHeader();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        log->identifier(),
        context.getSettingsRef().max_cached_data_bytes_in_spiller,
        context.getSettingsRef().max_spilled_rows_per_file,
        context.getSettingsRef().max_spilled_bytes_per_file,
        context.getFileProvider(),
        context.getSettingsRef().max_threads,
        context.getSettingsRef().max_block_size);
    auto params = *AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        pipeline.streams.size(),
        fine_grained_shuffle.enabled() ? pipeline.streams.size() : 1,
        aggregation_keys,
        key_ref_agg_func,
        agg_func_ref_key,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);

    if (fine_grained_shuffle.enabled())
    {
        std::shared_ptr<FineGrainedOperatorSpillContext> fine_grained_spill_context;
        if (context.getDAGContext() != nullptr && context.getDAGContext()->isInAutoSpillMode()
            && pipeline.hasMoreThanOneStream())
            fine_grained_spill_context = std::make_shared<FineGrainedOperatorSpillContext>("aggregation", log);
        /// For fine_grained_shuffle, just do aggregation in streams independently
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<AggregatingBlockInputStream>(
                stream,
                params,
                true,
                log->identifier(),
                [&](const OperatorSpillContextPtr & operator_spill_context) {
                    if (fine_grained_spill_context != nullptr)
                        fine_grained_spill_context->addOperatorSpillContext(operator_spill_context);
                    else if (context.getDAGContext() != nullptr)
                        context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                });
            stream->setExtraInfo(String(enableFineGrainedShuffleExtraInfo));
        });
        if (fine_grained_spill_context != nullptr)
            context.getDAGContext()->registerOperatorSpillContext(fine_grained_spill_context);
    }
    else if (auto_pass_through_switcher.enabled())
    {
        if (auto_pass_through_switcher.forceStreaming())
        {
            pipeline.transform([&](auto & stream) {
                stream = std::make_shared<AutoPassThroughAggregatingBlockInputStream<true>>(
                    stream,
                    params,
                    log->identifier(),
                    context.getSettings().max_block_size);
                stream->setExtraInfo(String(autoPassThroughAggregatingExtraInfo));
            });
        }
        else if (auto_pass_through_switcher.isAuto())
        {
            pipeline.transform([&](auto & stream) {
                stream = std::make_shared<AutoPassThroughAggregatingBlockInputStream<false>>(
                    stream,
                    params,
                    log->identifier(),
                    context.getSettings().max_block_size);
                stream->setExtraInfo(String(autoPassThroughAggregatingExtraInfo));
            });
        }
        else
        {
            throw Exception("unexpected auto_pass_through_switcher");
        }
    }
    else if (pipeline.streams.size() > 1)
    {
        /// If there are several sources, then we perform parallel aggregation
        const Settings & settings = context.getSettingsRef();
        BlockInputStreamPtr stream = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            BlockInputStreams{},
            params,
            true,
            max_streams,
            settings.max_buffered_bytes_in_executor,
            settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads),
            log->identifier(),
            [&](const OperatorSpillContextPtr & operator_spill_context) {
                if (context.getDAGContext() != nullptr)
                {
                    context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                }
            });

        pipeline.streams.resize(1);
        pipeline.firstStream() = std::move(stream);

        restoreConcurrency(
            pipeline,
            context.getDAGContext()->final_concurrency,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log);
    }
    else
    {
        RUNTIME_CHECK(pipeline.streams.size() == 1);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            pipeline.firstStream(),
            params,
            true,
            log->identifier(),
            [&](const OperatorSpillContextPtr & operator_spill_context) {
                if (context.getDAGContext() != nullptr)
                {
                    context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                }
            });
    }

    // we can record for agg after restore concurrency.
    // Because the streams of expr_after_agg will provide the correct ProfileInfo.
    // See #3804.
    RUNTIME_CHECK(expr_after_agg && !expr_after_agg->getActions().empty());
    executeExpression(pipeline, expr_after_agg, log, "expr after aggregation");
}

void PhysicalAggregation::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    // When got here, one of fine_grained_shuffle and auto_pass_through must be true.
    // Because for non fine grained shuffle, AggregateBuild and AggregateConvergent will be used to build aggregation.
    // Also auto pass through hashagg use PhysicalAggregation to build. But they cannot be true at the same time.
    RUNTIME_CHECK(fine_grained_shuffle.enabled() != auto_pass_through_switcher.enabled());

    // Auto pass through hashagg doesn't handle empty_result_for_aggregation_by_empty_set.
    // Also tidb shouldn't generate this kind plan because all data is aggregated into one row if keys_size == 0.
    RUNTIME_CHECK(
        fine_grained_shuffle.enabled() || (auto_pass_through_switcher.enabled() && !aggregation_keys.empty()));

    executeExpression(exec_context, group_builder, before_agg_actions, log);

    Block before_agg_header = group_builder.getCurrentHeader();
    size_t concurrency = group_builder.concurrency();
    std::shared_ptr<FineGrainedOperatorSpillContext> fine_grained_spill_context;
    if (context.getDAGContext() != nullptr && context.getDAGContext()->isInAutoSpillMode() && concurrency > 1)
        fine_grained_spill_context = std::make_shared<FineGrainedOperatorSpillContext>("aggregation", log);
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        log->identifier(),
        context.getSettingsRef().max_cached_data_bytes_in_spiller,
        context.getSettingsRef().max_spilled_rows_per_file,
        context.getSettingsRef().max_spilled_bytes_per_file,
        context.getFileProvider(),
        context.getSettingsRef().max_threads,
        context.getSettingsRef().max_block_size);
    auto params = *AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        concurrency,
        concurrency,
        aggregation_keys,
        key_ref_agg_func,
        agg_func_ref_key,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);
    if (fine_grained_shuffle.enabled())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<LocalAggregateTransform>(
                exec_context,
                log->identifier(),
                params,
                fine_grained_spill_context));
        });
        if (fine_grained_spill_context != nullptr)
            context.getDAGContext()->registerOperatorSpillContext(fine_grained_spill_context);
    }
    else if (auto_pass_through_switcher.enabled())
    {
        if (auto_pass_through_switcher.forceStreaming())
        {
            group_builder.transform([&](auto & builder) {
                builder.appendTransformOp(std::make_unique<AutoPassThroughAggregateTransform<true>>(
                    builder.getCurrentHeader(),
                    exec_context,
                    params,
                    log->identifier(),
                    context.getSettings().max_block_size));
            });
        }
        else if (auto_pass_through_switcher.isAuto())
        {
            group_builder.transform([&](auto & builder) {
                builder.appendTransformOp(std::make_unique<AutoPassThroughAggregateTransform<false>>(
                    builder.getCurrentHeader(),
                    exec_context,
                    params,
                    log->identifier(),
                    context.getSettings().max_block_size));
            });
        }
        else
        {
            throw Exception("unexpected auto_pass_through_switcher");
        }
    }
    else
    {
        throw Exception(fmt::format(
            "fine grained shuffle({}) or auto pass through({}) should be true",
            fine_grained_shuffle.enabled(),
            auto_pass_through_switcher.enabled()));
    }

    executeExpression(exec_context, group_builder, expr_after_agg, log);
}

void PhysicalAggregation::buildPipeline(
    PipelineBuilder & builder,
    Context & context,
    PipelineExecutorContext & exec_context)
{
    auto aggregate_context = std::make_shared<AggregateContext>(log->identifier());
    // fine_grained_shuffle and auto_pass_through cannot be ture at the same time.
    RUNTIME_CHECK(!(fine_grained_shuffle.enabled() && auto_pass_through_switcher.enabled()));
    if (fine_grained_shuffle.enabled() || auto_pass_through_switcher.enabled())
    {
        // For fine grained shuffle, Aggregate wouldn't be broken.
        child->buildPipeline(builder, context, exec_context);
        builder.addPlanNode(shared_from_this());
    }
    else
    {
        // For non fine grained shuffle, Aggregate would be broken into AggregateBuild and AggregateConvergent.
        auto agg_build = std::make_shared<PhysicalAggregationBuild>(
            executor_id,
            schema,
            req_id,
            child,
            before_agg_actions,
            aggregation_keys,
            aggregation_collators,
            key_ref_agg_func,
            agg_func_ref_key,
            is_final_agg,
            aggregate_descriptions,
            aggregate_context);
        // Break the pipeline for agg_build.
        auto agg_build_builder = builder.breakPipeline(agg_build);
        // agg_build pipeline.
        child->buildPipeline(agg_build_builder, context, exec_context);
        agg_build_builder.build();
        // agg_convergent pipeline.
        auto agg_convergent = std::make_shared<PhysicalAggregationConvergent>(
            executor_id,
            schema,
            req_id,
            aggregate_context,
            expr_after_agg);
        builder.addPlanNode(agg_convergent);
    }
}

void PhysicalAggregation::finalizeImpl(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    expr_after_agg->finalize(DB::toNames(schema));

    Names before_agg_output;
    // set required output for agg funcs's arguments and group by keys.
    for (const auto & aggregate_description : aggregate_descriptions)
    {
        for (const auto & argument_name : aggregate_description.argument_names)
            before_agg_output.push_back(argument_name);
    }
    for (const auto & aggregation_key : aggregation_keys)
    {
        before_agg_output.push_back(aggregation_key);
    }

    before_agg_actions->finalize(before_agg_output);
    child->finalize(before_agg_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_agg_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalAggregation::getSampleBlock() const
{
    return expr_after_agg->getSampleBlock();
}
} // namespace DB
