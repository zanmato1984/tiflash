#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

namespace DB::tests
{

class TiForthQueryExecutorPassThroughTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"test_db", "tiforth_passthrough"},
            {{"col0", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("col0", {0, 1, 2, 3, 4})});
        context.addMockTable(
            {"test_db", "tiforth_agg_i64"},
            {{"k", TiDB::TP::TypeLongLong}, {"v", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("k", {1, 1, 2, std::nullopt, 2}),
             toNullableVec<Int64>("v", {10, std::nullopt, 20, 7, 1})});
        context.addMockTable(
            {"test_db", "tiforth_agg_str"},
            {{"k", TiDB::TP::TypeString}, {"v", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("k", {String("a"), String("b"), String("a"), std::nullopt}),
             toNullableVec<Int64>("v", {10, 20, 1, 7})});
    }
};

TEST_F(TiForthQueryExecutorPassThroughTestRunner, TableScanPassThrough)
try
{
    auto request = context.scan("test_db", "tiforth_passthrough").build(context);
    const auto expect_columns = ColumnsWithTypeAndName{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4})};

    WRAP_FOR_TEST_BEGIN
    executeAndAssertColumnsEqual(request, expect_columns);
    WRAP_FOR_TEST_END
}
CATCH

TEST_F(TiForthQueryExecutorPassThroughTestRunner, ArrowComputeAggGroupByInt64Sum)
try
{
    const bool old_flag = context.context->getSettingsRef().enable_tiforth_arrow_compute_agg;
    context.context->setSetting("enable_tiforth_arrow_compute_agg", "true");
    SCOPE_EXIT({
        context.context->setSetting("enable_tiforth_arrow_compute_agg", old_flag ? "true" : "false");
    });

    auto request = context.scan("test_db", "tiforth_agg_i64")
                       .aggregation({Sum(col("v"))}, {col("k")})
                       .project({"sum(v)", "k"})
                       .build(context);
    const auto expect_columns = ColumnsWithTypeAndName{
        toNullableVec<Int64>("sum(v)", {10, 21, 7}),
        toNullableVec<Int64>("k", {1, 2, std::nullopt}),
    };

    WRAP_FOR_TEST_BEGIN
    executeAndAssertColumnsEqual(request, expect_columns);
    WRAP_FOR_TEST_END
}
CATCH

TEST_F(TiForthQueryExecutorPassThroughTestRunner, ArrowComputeAggGroupByStringSumFallbackHashAgg)
try
{
    const bool old_flag = context.context->getSettingsRef().enable_tiforth_arrow_compute_agg;
    context.context->setSetting("enable_tiforth_arrow_compute_agg", "true");
    SCOPE_EXIT({
        context.context->setSetting("enable_tiforth_arrow_compute_agg", old_flag ? "true" : "false");
    });

    auto request = context.scan("test_db", "tiforth_agg_str")
                       .aggregation({Sum(col("v"))}, {col("k")})
                       .project({"sum(v)", "k"})
                       .build(context);
    const auto expect_columns = ColumnsWithTypeAndName{
        toNullableVec<Int64>("sum(v)", {11, 20, 7}),
        toNullableVec<String>("k", {String("a"), String("b"), std::nullopt}),
    };

    WRAP_FOR_TEST_BEGIN
    executeAndAssertColumnsEqual(request, expect_columns);
    WRAP_FOR_TEST_END
}
CATCH

} // namespace DB::tests

#else

TEST(TiForthQueryExecutorPassThroughTestRunner, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
