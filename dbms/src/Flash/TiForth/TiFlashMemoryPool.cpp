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

#include <Flash/TiForth/TiFlashMemoryPool.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/Exception.h>

namespace DB::TiForth
{

namespace
{

std::shared_ptr<arrow::MemoryPool> nonOwningPool(arrow::MemoryPool * pool)
{
    // Arrow memory pools are expected to outlive queries for process lifetime.
    return std::shared_ptr<arrow::MemoryPool>(pool, [](arrow::MemoryPool *) {});
}

} // namespace

TiFlashMemoryPool::TiFlashMemoryPool(MemoryTrackerPtr memory_tracker_, arrow::MemoryPool * delegate_)
    : memory_tracker(std::move(memory_tracker_))
    , delegate(delegate_)
{
    RUNTIME_CHECK_MSG(delegate != nullptr, "delegate memory pool must not be null");
    RUNTIME_CHECK_MSG(memory_tracker != nullptr, "memory tracker must not be null");
}

arrow::Status TiFlashMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t ** out)
{
    if (out == nullptr)
        return arrow::Status::Invalid("out pointer must not be null");
    if (size < 0)
        return arrow::Status::Invalid("allocation size must not be negative");
    if (alignment <= 0)
        return arrow::Status::Invalid("allocation alignment must be positive");

    try
    {
        if (size > 0)
            memory_tracker->alloc(size);
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::OutOfMemory(e.displayText());
    }
    catch (...)
    {
        return arrow::Status::OutOfMemory("TiFlashMemoryPool allocation rejected by MemoryTracker");
    }

    auto st = delegate->Allocate(size, alignment, out);
    if (!st.ok())
    {
        if (size > 0)
            memory_tracker->free(size);
        return st;
    }
    return st;
}

arrow::Status TiFlashMemoryPool::Reallocate(
    int64_t old_size,
    int64_t new_size,
    int64_t alignment,
    uint8_t ** ptr)
{
    if (ptr == nullptr)
        return arrow::Status::Invalid("ptr must not be null");
    if (old_size < 0 || new_size < 0)
        return arrow::Status::Invalid("reallocation sizes must not be negative");
    if (alignment <= 0)
        return arrow::Status::Invalid("reallocation alignment must be positive");

    const int64_t delta = new_size - old_size;
    if (delta > 0)
    {
        try
        {
            memory_tracker->alloc(delta);
        }
        catch (const DB::Exception & e)
        {
            return arrow::Status::OutOfMemory(e.displayText());
        }
        catch (...)
        {
            return arrow::Status::OutOfMemory("TiFlashMemoryPool reallocation rejected by MemoryTracker");
        }
    }

    auto st = delegate->Reallocate(old_size, new_size, alignment, ptr);
    if (!st.ok())
    {
        if (delta > 0)
            memory_tracker->free(delta);
        return st;
    }

    if (delta < 0)
        memory_tracker->free(-delta);
    return st;
}

void TiFlashMemoryPool::Free(uint8_t * buffer, int64_t size, int64_t alignment)
{
    delegate->Free(buffer, size, alignment);
    if (size > 0)
        memory_tracker->free(size);
}

int64_t TiFlashMemoryPool::bytes_allocated() const
{
    return delegate->bytes_allocated();
}

int64_t TiFlashMemoryPool::max_memory() const
{
    return delegate->max_memory();
}

std::string TiFlashMemoryPool::backend_name() const
{
    return "tiflash_memory_tracker(" + delegate->backend_name() + ")";
}

int64_t TiFlashMemoryPool::total_bytes_allocated() const
{
    return delegate->total_bytes_allocated();
}

int64_t TiFlashMemoryPool::num_allocations() const
{
    return delegate->num_allocations();
}

std::shared_ptr<arrow::MemoryPool> MakeTiFlashMemoryPool(
    const MemoryTrackerPtr & memory_tracker,
    arrow::MemoryPool * delegate)
{
    RUNTIME_CHECK_MSG(delegate != nullptr, "delegate memory pool must not be null");
    if (memory_tracker == nullptr)
        return nonOwningPool(delegate);
    return std::make_shared<TiFlashMemoryPool>(memory_tracker, delegate);
}

std::shared_ptr<arrow::MemoryPool> MakeCurrentMemoryTrackerPoolOrDefault(arrow::MemoryPool * delegate)
{
    RUNTIME_CHECK_MSG(delegate != nullptr, "delegate memory pool must not be null");
    if (current_memory_tracker == nullptr)
        return nonOwningPool(delegate);

    try
    {
        return MakeTiFlashMemoryPool(current_memory_tracker->shared_from_this(), delegate);
    }
    catch (...)
    {
        return nonOwningPool(delegate);
    }
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
