#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/MemoryTracker.h>
#include <Flash/TiForth/TiFlashMemoryPool.h>

#include <arrow/memory_pool.h>

namespace DB::tests
{

TEST(TiForthMemoryPoolTestRunner, ChargesMemoryTracker)
{
    auto tracker = MemoryTracker::create(/*limit=*/0);
    ASSERT_NE(tracker, nullptr);
    ASSERT_EQ(tracker->get(), 0);

    auto pool = DB::TiForth::MakeTiFlashMemoryPool(tracker, arrow::default_memory_pool());
    ASSERT_NE(pool, nullptr);

    uint8_t * ptr = nullptr;
    ASSERT_TRUE(pool->Allocate(/*size=*/1024, &ptr).ok());
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(tracker->get(), 1024);

    ASSERT_TRUE(pool->Reallocate(/*old_size=*/1024, /*new_size=*/2048, &ptr).ok());
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(tracker->get(), 2048);

    ASSERT_TRUE(pool->Reallocate(/*old_size=*/2048, /*new_size=*/512, &ptr).ok());
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(tracker->get(), 512);

    pool->Free(ptr, /*size=*/512);
    ASSERT_EQ(tracker->get(), 0);
}

} // namespace DB::tests

#else

TEST(TiForthMemoryPoolTestRunner, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
