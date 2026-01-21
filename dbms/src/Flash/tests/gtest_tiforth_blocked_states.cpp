#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/BlockPipelineRunner.h>
#include <Flash/TiForth/TiForthAggBlockInputStream.h>

#include <arrow/memory_pool.h>

#include <unordered_map>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/operators/pilot.h"
#include "tiforth/plan.h"
#include "tiforth/pipeline.h"

namespace DB::tests
{
namespace
{

Block makeInt64Block(std::initializer_list<Int64> values)
{
    auto col = ColumnInt64::create();
    col->reserve(values.size());
    for (const auto v : values)
        col->insert(v);
    auto type = std::make_shared<DataTypeInt64>();

    ColumnsWithTypeAndName cols;
    cols.emplace_back(std::move(col), type, "col0");
    return Block(std::move(cols));
}

std::vector<Int64> flattenInt64Column(const std::vector<Block> & blocks)
{
    std::vector<Int64> out;
    for (const auto & block : blocks)
    {
        const auto * col = typeid_cast<const ColumnInt64 *>(block.getByName("col0").column.get());
        if (col == nullptr)
            return {};
        const auto & data = col->getData();
        out.insert(out.end(), data.begin(), data.end());
    }
    return out;
}

arrow::Result<std::unique_ptr<tiforth::Pipeline>> makePilotPipeline(
    const tiforth::Engine * engine,
    tiforth::PilotBlockKind block_kind,
    int32_t block_cycles)
{
    if (engine == nullptr)
        return arrow::Status::Invalid("engine must not be null");

    ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine));
    tiforth::PilotAsyncTransformOptions options;
    options.block_kind = block_kind;
    options.block_cycles = block_cycles;

    ARROW_RETURN_NOT_OK(builder->AppendTransform([options]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::PilotAsyncTransformOp>(options);
    }));
    return builder->Finalize();
}

void runPilotOnBlocks(tiforth::PilotBlockKind kind)
{
    auto pool_holder = std::shared_ptr<arrow::MemoryPool>(arrow::default_memory_pool(), [](arrow::MemoryPool *) {});

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool_holder.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    ASSERT_TRUE(engine_res.ok()) << engine_res.status().ToString();
    auto engine = std::move(engine_res).ValueOrDie();
    ASSERT_NE(engine, nullptr);

    auto pipeline_res = makePilotPipeline(engine.get(), kind, /*block_cycles=*/3);
    ASSERT_TRUE(pipeline_res.ok()) << pipeline_res.status().ToString();
    auto pipeline = std::move(pipeline_res).ValueOrDie();
    ASSERT_NE(pipeline, nullptr);

    std::vector<Block> input_blocks;
    input_blocks.push_back(makeInt64Block({0, 1, 2}));
    input_blocks.push_back(makeInt64Block({3, 4}));

    auto outputs_res = DB::TiForth::RunTiForthPipelineOnBlocks(
        *pipeline,
        input_blocks,
        /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
        pool_holder.get());
    ASSERT_TRUE(outputs_res.ok()) << outputs_res.status().ToString();
    auto outputs = std::move(outputs_res).ValueOrDie();

    std::vector<Block> out_blocks;
    out_blocks.reserve(outputs.size());
    for (auto & out : outputs)
        out_blocks.push_back(std::move(out.block));

    EXPECT_EQ(flattenInt64Column(out_blocks), (std::vector<Int64>{0, 1, 2, 3, 4}));
}

void runPilotOnAggBlockInputStream(tiforth::PilotBlockKind kind)
{
    auto pool_holder = std::shared_ptr<arrow::MemoryPool>(arrow::default_memory_pool(), [](arrow::MemoryPool *) {});

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool_holder.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    ASSERT_TRUE(engine_res.ok()) << engine_res.status().ToString();
    auto engine = std::move(engine_res).ValueOrDie();
    ASSERT_NE(engine, nullptr);

    auto builder_res = tiforth::PlanBuilder::Create(engine.get());
    ASSERT_TRUE(builder_res.ok()) << builder_res.status().ToString();
    auto builder = std::move(builder_res).ValueOrDie();
    ASSERT_NE(builder, nullptr);

    auto stage_res = builder->AddStage();
    ASSERT_TRUE(stage_res.ok()) << stage_res.status().ToString();
    const auto stage = stage_res.ValueOrDie();

    tiforth::PilotAsyncTransformOptions options;
    options.block_kind = kind;
    options.block_cycles = 3;
    ASSERT_TRUE(builder->AppendTransform(
        stage,
        [options](tiforth::PlanTaskContext *) -> arrow::Result<tiforth::TransformOpPtr> {
            return std::make_unique<tiforth::PilotAsyncTransformOp>(options);
        }).ok());

    auto plan_res = builder->Finalize();
    ASSERT_TRUE(plan_res.ok()) << plan_res.status().ToString();
    auto plan = std::move(plan_res).ValueOrDie();
    ASSERT_NE(plan, nullptr);

    BlocksList blocks;
    blocks.push_back(makeInt64Block({0, 1, 2}));
    blocks.push_back(makeInt64Block({3, 4}));
    auto input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
    ASSERT_NE(input_stream, nullptr);

    NamesAndTypesList output_columns;
    output_columns.emplace_back("col0", std::make_shared<DataTypeInt64>());

    const Block sample_input_block = input_stream->getHeader();
    auto stream = std::make_shared<DB::TiForth::TiForthAggBlockInputStream>(
        input_stream,
        std::move(engine),
        std::move(plan),
        output_columns,
        /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
        pool_holder,
        sample_input_block);
    ASSERT_NE(stream, nullptr);

    stream->readPrefix();
    std::vector<Block> out_blocks;
    while (Block out = stream->read())
        out_blocks.push_back(std::move(out));
    stream->readSuffix();

    EXPECT_EQ(flattenInt64Column(out_blocks), (std::vector<Int64>{0, 1, 2, 3, 4}));
}

} // namespace

TEST(TiForthBlockedStatesTest, PilotIOIn)
{
    runPilotOnBlocks(tiforth::PilotBlockKind::kIOIn);
    runPilotOnAggBlockInputStream(tiforth::PilotBlockKind::kIOIn);
}

TEST(TiForthBlockedStatesTest, PilotIOOut)
{
    runPilotOnBlocks(tiforth::PilotBlockKind::kIOOut);
    runPilotOnAggBlockInputStream(tiforth::PilotBlockKind::kIOOut);
}

TEST(TiForthBlockedStatesTest, PilotWaiting)
{
    runPilotOnBlocks(tiforth::PilotBlockKind::kWaiting);
    runPilotOnAggBlockInputStream(tiforth::PilotBlockKind::kWaiting);
}

TEST(TiForthBlockedStatesTest, PilotWaitForNotify)
{
    runPilotOnBlocks(tiforth::PilotBlockKind::kWaitForNotify);
    runPilotOnAggBlockInputStream(tiforth::PilotBlockKind::kWaitForNotify);
}

} // namespace DB::tests

#else

TEST(TiForthBlockedStatesTest, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
