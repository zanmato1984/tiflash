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

} // namespace DB::tests

#else

TEST(TiForthQueryExecutorPassThroughTestRunner, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif

