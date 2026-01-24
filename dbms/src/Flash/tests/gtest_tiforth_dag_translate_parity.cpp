#include <gtest/gtest.h>

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <ext/scope_guard.h>

#include <Interpreters/Context.h>

namespace DB::tests
{

namespace
{

class TiForthDagTranslateParityTestRunner : public ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable(
            {"test_db", "tiforth_translate_agg"},
            {{"k", TiDB::TP::TypeLongLong}, {"v", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("k", {1, 1, 2, std::nullopt, 2}),
             toNullableVec<Int64>("v", {10, std::nullopt, 20, 7, 1})});

        context.addMockTable(
            {"test_db", "tiforth_translate_join_l"},
            {{"k", TiDB::TP::TypeLongLong}, {"v", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("k", {1, 2, 2, std::nullopt}),
             toNullableVec<Int64>("v", {10, 20, 21, 7})});

        context.addMockTable(
            {"test_db", "tiforth_translate_join_r"},
            {{"k", TiDB::TP::TypeLongLong}, {"bv", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("k", {1, 2, 2, std::nullopt}),
             toNullableVec<Int64>("bv", {100, 200, 201, 999})});
    }
};

} // namespace

TEST_F(TiForthDagTranslateParityTestRunner, FilterProjectAggDagParity)
try
{
    auto request = context.scan("test_db", "tiforth_translate_agg")
                       .filter(gt(col("k"), lit(Field(static_cast<Int64>(1)))))
                       .project({col("k"), col("v")})
                       .aggregation({Sum(col("v"))}, {col("k")})
                       .project({"sum(v)", "k"})
                       .build(context);

    for (const size_t concurrency : {1, 10})
    {
        for (const size_t block_size : {1, DEFAULT_BLOCK_SIZE})
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));

            enablePipeline(ExecutorTest::ExecutorMode::Dag);
            const auto expected = executeStreams(request, concurrency);

            const bool old_translate = context.context->getSettingsRef().enable_tiforth_translate_dag;
            context.context->setSetting("enable_tiforth_translate_dag", "true");
            SCOPE_EXIT({
                context.context->setSetting("enable_tiforth_translate_dag", old_translate ? "true" : "false");
            });

            enablePipeline(ExecutorTest::ExecutorMode::TiForth);
            const auto actual = executeStreams(request, concurrency);
            ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
                << "\nexpected:\n"
                << getColumnsContent(expected)
                << "\nactual:\n"
                << getColumnsContent(actual);
        }
    }
}
CATCH

TEST_F(TiForthDagTranslateParityTestRunner, JoinDagParity)
try
{
    auto request = context.scan("test_db", "tiforth_translate_join_l")
                       .join(context.scan("test_db", "tiforth_translate_join_r"), tipb::JoinType::TypeInnerJoin, {col("k")})
                       .build(context);

    for (const size_t concurrency : {1, 10})
    {
        for (const size_t block_size : {1, DEFAULT_BLOCK_SIZE})
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));

            enablePipeline(ExecutorTest::ExecutorMode::Dag);
            const auto expected = executeStreams(request, concurrency);

            const bool old_translate = context.context->getSettingsRef().enable_tiforth_translate_dag;
            context.context->setSetting("enable_tiforth_translate_dag", "true");
            SCOPE_EXIT({
                context.context->setSetting("enable_tiforth_translate_dag", old_translate ? "true" : "false");
            });

            enablePipeline(ExecutorTest::ExecutorMode::TiForth);
            const auto actual = executeStreams(request, concurrency);
            ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
                << "\nexpected:\n"
                << getColumnsContent(expected)
                << "\nactual:\n"
                << getColumnsContent(actual);
        }
    }
}
CATCH

} // namespace DB::tests

#else

TEST(TiForthDagTranslateParityTestRunner, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
