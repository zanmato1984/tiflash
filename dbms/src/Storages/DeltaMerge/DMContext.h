#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{

class StoragePathPool;

class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

namespace DM
{

class StoragePool;
using NotCompress = std::unordered_set<ColId>;
struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;

/**
 * This context object carries table infos. And those infos are only meaningful to current context.
 */
struct DMContext : private boost::noncopyable
{
    const Context &         db_context;
    const TiFlashMetricsPtr metrics;

    StoragePathPool & path_pool;
    StoragePool &     storage_pool;
    const UInt64      hash_salt;

    // gc safe-point, maybe update.
    DB::Timestamp min_version;

    const NotCompress & not_compress; // Not used currently.

    // The rows of segment.
    const size_t segment_limit_rows;
    // The threshold of delta.
    const size_t delta_limit_rows;
    // The threshold of cache in delta.
    const size_t delta_cache_limit_rows;
    // Determine whether a pack is small or not.
    const size_t delta_small_pack_rows;
    // The expected stable pack rows.
    const size_t stable_pack_rows;

    const bool enable_logical_split;
    const bool read_delta_only;
    const bool read_stable_only;
    const bool enable_skippable_place;

public:
    DMContext(const Context &          db_context_,
              StoragePathPool &        path_pool_,
              StoragePool &            storage_pool_,
              const UInt64             hash_salt_,
              const DB::Timestamp      min_version_,
              const NotCompress &      not_compress_,
              const DB::Settings &     settings)
        : db_context(db_context_),
          metrics(db_context.getTiFlashMetrics()),
          path_pool(path_pool_),
          storage_pool(storage_pool_),
          hash_salt(hash_salt_),
          min_version(min_version_),
          not_compress(not_compress_),
          segment_limit_rows(settings.dt_segment_limit_rows),
          delta_limit_rows(settings.dt_segment_delta_limit_rows),
          delta_cache_limit_rows(settings.dt_segment_delta_cache_limit_rows),
          delta_small_pack_rows(settings.dt_segment_delta_small_pack_rows),
          stable_pack_rows(settings.dt_segment_stable_pack_rows),
          enable_logical_split(settings.dt_enable_logical_split),
          read_delta_only(settings.dt_read_delta_only),
          read_stable_only(settings.dt_read_stable_only),
          enable_skippable_place(settings.dt_enable_skippable_place)
    {
    }
};

} // namespace DM
} // namespace DB
