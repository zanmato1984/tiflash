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

#pragma once

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/MemoryTracker.h>

#include <arrow/memory_pool.h>

#include <memory>
#include <string>

namespace DB::TiForth
{

/// An Arrow memory pool wrapper that charges allocations to a TiFlash MemoryTracker.
/// The underlying allocation is delegated to another Arrow memory pool (by default,
/// `arrow::default_memory_pool()`).
///
/// Notes:
/// - If `memory_tracker` is null, callers should avoid creating this wrapper and
///   just use the delegate pool.
/// - This pool is thread-safe as MemoryTracker is thread-safe and Arrow pools are
///   expected to be thread-safe.
class TiFlashMemoryPool final : public arrow::MemoryPool
{
public:
    TiFlashMemoryPool(MemoryTrackerPtr memory_tracker_, arrow::MemoryPool * delegate_);

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t ** out) override;
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t ** ptr) override;
    void Free(uint8_t * buffer, int64_t size, int64_t alignment) override;

    int64_t bytes_allocated() const override;
    int64_t max_memory() const override;
    std::string backend_name() const override;
    int64_t total_bytes_allocated() const override;
    int64_t num_allocations() const override;

private:
    MemoryTrackerPtr memory_tracker;
    arrow::MemoryPool * delegate = nullptr;
};

/// Returns a pool that charges `memory_tracker` (if non-null) and delegates allocations
/// to `delegate` (must be non-null).
///
/// The returned shared_ptr is always non-null and is safe to store for lifetime management.
std::shared_ptr<arrow::MemoryPool> MakeTiFlashMemoryPool(
    const MemoryTrackerPtr & memory_tracker,
    arrow::MemoryPool * delegate = arrow::default_memory_pool());

/// Best-effort helper for query execution paths:
/// - If `current_memory_tracker` is set, use it (via `shared_from_this()`).
/// - Otherwise return the delegate as-is.
std::shared_ptr<arrow::MemoryPool> MakeCurrentMemoryTrackerPoolOrDefault(
    arrow::MemoryPool * delegate = arrow::default_memory_pool());

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
