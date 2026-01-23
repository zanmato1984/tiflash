#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/MemoryTracker.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/TiFlashMemoryPool.h>
#include <Flash/TiForth/TiForthPipelineBlockInputStream.h>
#include <Flash/TiForth/TiForthQueryExecutor.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <arrow/memory_pool.h>

#include <unordered_map>

#include "tiforth/engine.h"
#include "tiforth/operators/pass_through.h"

namespace DB::tests
{
namespace
{

class ConstInt64BlocksInputStream final : public IProfilingBlockInputStream
{
public:
    ConstInt64BlocksInputStream(size_t blocks, size_t rows, Int64 value)
        : blocks_left(blocks)
        , rows_per_block(rows)
        , type(std::make_shared<DataTypeInt64>())
    {
        auto nested_mut = ColumnInt64::create();
        nested_mut->insert(value);
        nested = std::move(nested_mut);
    }

    String getName() const override { return "ConstInt64Blocks"; }

    Block getHeader() const override { return Block({{type, "col0"}}); }

protected:
    Block readImpl() override
    {
        if (blocks_left == 0)
            return {};
        --blocks_left;

        auto col = ColumnConst::create(nested, rows_per_block);
        Block block;
        block.insert({std::move(col), type, "col0"});
        return block;
    }

private:
    size_t blocks_left;
    size_t rows_per_block;
    DataTypePtr type;
    ColumnPtr nested;
};

} // namespace

TEST(TiForthStreamingExecutorTest, QueryExecutorPassThroughLargeInputUnderMemoryLimit)
{
    constexpr size_t kBlocks = 256;
    constexpr size_t kRowsPerBlock = 16384;
    constexpr Int64 kLimitBytes = 8 * 1024 * 1024;

    auto context = TiFlashTestEnv::getContext();
    ASSERT_NE(context, nullptr);

    auto tracker = MemoryTracker::create(kLimitBytes);
    ASSERT_NE(tracker, nullptr);

    auto pool = DB::TiForth::MakeTiFlashMemoryPool(tracker, arrow::default_memory_pool());
    ASSERT_NE(pool, nullptr);

    auto input_stream = std::make_shared<ConstInt64BlocksInputStream>(kBlocks, kRowsPerBlock, /*value=*/1);
    ASSERT_NE(input_stream, nullptr);

    auto executor_res = DB::TiForth::TiForthQueryExecutor::CreatePassThrough(
        tracker,
        *context,
        /*req_id=*/"tiforth_streaming_large_pass_through",
        input_stream,
        /*input_options_by_name=*/{},
        pool);
    ASSERT_TRUE(executor_res.ok()) << executor_res.status().ToString();

    auto executor = std::move(executor_res).ValueOrDie();
    ASSERT_NE(executor, nullptr);

    size_t blocks_seen = 0;
    size_t total_rows = 0;
    auto exec_res = static_cast<DB::QueryExecutor *>(executor.get())->execute([&](const Block & block) {
        ++blocks_seen;
        total_rows += block.rows();
    });
    ASSERT_TRUE(exec_res.is_success);
    EXPECT_EQ(blocks_seen, kBlocks);
    EXPECT_EQ(total_rows, kBlocks * kRowsPerBlock);

    executor.reset();
    pool.reset();
    EXPECT_EQ(tracker->get(), 0);
}

TEST(TiForthStreamingExecutorTest, AggBlockInputStreamPassThroughLargeInputUnderMemoryLimit)
{
    constexpr size_t kBlocks = 256;
    constexpr size_t kRowsPerBlock = 16384;
    constexpr Int64 kLimitBytes = 8 * 1024 * 1024;

    auto tracker = MemoryTracker::create(kLimitBytes);
    ASSERT_NE(tracker, nullptr);

    auto pool = DB::TiForth::MakeTiFlashMemoryPool(tracker, arrow::default_memory_pool());
    ASSERT_NE(pool, nullptr);

    auto input_stream = std::make_shared<ConstInt64BlocksInputStream>(kBlocks, kRowsPerBlock, /*value=*/1);
    ASSERT_NE(input_stream, nullptr);

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    ASSERT_TRUE(engine_res.ok()) << engine_res.status().ToString();
    auto engine = std::move(engine_res).ValueOrDie();
    ASSERT_NE(engine, nullptr);

    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
    pipe_ops.push_back(std::make_unique<tiforth::PassThroughPipeOp>());

    const auto output_type = std::make_shared<DataTypeInt64>();
    NamesAndTypesList output_columns;
    output_columns.emplace_back("col0", output_type);

    const Block sample_block = input_stream->getHeader();
    auto agg_stream = std::make_shared<DB::TiForth::TiForthPipelineBlockInputStream>(
        "TiForthStreamingPassThrough",
        input_stream,
        std::move(engine),
        std::move(pipe_ops),
        output_columns,
        /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
        pool,
        sample_block);
    ASSERT_NE(agg_stream, nullptr);

    agg_stream->readPrefix();
    size_t blocks_seen = 0;
    size_t total_rows = 0;
    while (Block out = agg_stream->read())
    {
        ++blocks_seen;
        total_rows += out.rows();
    }
    agg_stream->readSuffix();

    EXPECT_EQ(blocks_seen, kBlocks);
    EXPECT_EQ(total_rows, kBlocks * kRowsPerBlock);

    agg_stream.reset();
    pool.reset();
    EXPECT_EQ(tracker->get(), 0);
}

} // namespace DB::tests

#else

TEST(TiForthStreamingExecutorTest, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
