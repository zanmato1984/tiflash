#include <Common/TiFlashMetrics.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/Pack.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/WriteBatches.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
extern const Event PSMWriteBytes;
extern const Event WriteBufferFromFileDescriptorWriteBytes;
extern const Event WriteBufferAIOWriteBytes;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric DT_WriteAmplification;
}

namespace DB::DM
{

struct FlushPackTask
{
    FlushPackTask(const PackPtr & pack_) : pack(pack_) {}

    ConstPackPtr pack;

    PageId data_page = 0;
};
using FlushPackTasks = std::vector<FlushPackTask>;

bool DeltaValueSpace::flush(DMContext & context)
{
    LOG_DEBUG(log, info() << ", Flush start");

    FlushPackTasks tasks;
    WriteBatches   wbs(context.storage_pool);

    size_t flush_rows    = 0;
    size_t flush_bytes   = 0;
    size_t flush_deletes = 0;
    {
        /// Prepare data which will be written to disk.
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, simpleInfo() << "Flush stop because abandoned");
            return false;
        }

        size_t total_rows    = 0;
        size_t total_deletes = 0;
        for (auto & pack : packs)
        {
            if (unlikely(!tasks.empty() && pack->isSaved()))
            {
                String msg = "Pack should not already saved, because previous packs are not saved.";

                LOG_ERROR(log, simpleInfo() << msg << " Packs: " << packsToString(packs));
                throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
            }

            if (!pack->isSaved())
            {
                auto & task = tasks.emplace_back(pack);
                // We only write the pack's data if it is not a delete range, and it's data haven't been saved.
                // Otherwise, simply save it's metadata is enough.
                if (pack->dataFlushable())
                {
                    if (unlikely(!pack->cache))
                        throw Exception("Mutable pack does not have cache", ErrorCodes::LOGICAL_ERROR);
                    task.data_page = writePackData(context, pack->cache->block, pack->cache_offset, pack->rows, wbs);
                }
                flush_rows += pack->rows;
                flush_bytes += pack->bytes;
                flush_deletes += pack->isDeleteRange();
            }
            total_rows += pack->rows;
            total_deletes += pack->isDeleteRange();

            // Stop other threads appending to this pack.
            pack->appendable = false;
        }

        if (unlikely(flush_rows != unsaved_rows || flush_deletes != unsaved_deletes || total_rows != rows || total_deletes != deletes))
            throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);
    }

    // No update, return successfully.
    if (tasks.empty())
    {
        LOG_DEBUG(log, simpleInfo() << " Nothing to flush");
        return true;
    }

    {
        /// Write prepared data to disk.
        wbs.writeLogAndData();
    }

    {
        /// If this instance is still valid, then commit.
        std::scoped_lock lock(mutex);

        if (abandoned.load(std::memory_order_relaxed))
        {
            // Delete written data.
            wbs.setRollback();
            LOG_DEBUG(log, simpleInfo() << " Flush stop because abandoned");
            return false;
        }

        Packs::iterator flush_start_point;
        Packs::iterator flush_end_point;

        {
            /// Do some checks before continue, in case other threads do some modifications during current operation,
            /// as we don't always have the lock.

            auto p_it = packs.begin();
            auto t_it = tasks.begin();
            for (; p_it != packs.end(); ++p_it)
            {
                if (*p_it == t_it->pack)
                    break;
            }

            flush_start_point = p_it;

            for (; t_it != tasks.end(); ++t_it, ++p_it)
            {
                if (p_it == packs.end() || *p_it != t_it->pack || (*p_it)->isSaved())
                {
                    // The packs have been modified, or this pack already saved by another thread.
                    // Let's rollback and break up.
                    wbs.rollbackWrittenLogAndData();
                    LOG_DEBUG(log, simpleInfo() << " Stop flush because structure got updated");
                    return false;
                }
            }

            flush_end_point = p_it;
        }

        /// Things look good, let's continue.

        // Create a temporary packs copy, used to generate serialized data.
        // Save the previous saved packs, and the packs we are saving, and the later packs appended during the io operation.
        Packs packs_copy(packs.begin(), flush_start_point);
        for (auto & task : tasks)
        {
            // Use a new pack instance to do the serializing.
            auto shadow = std::make_shared<Pack>(*task.pack);
            // Set saved to true, otherwise it cannot be serialized.
            shadow->saved = true;
            // If it's data have been updated, use the new pages info.
            if (task.data_page != 0)
                shadow->data_page = task.data_page;

            packs_copy.push_back(shadow);
        }
        packs_copy.insert(packs_copy.end(), flush_end_point, packs.end());

        if constexpr (DM_RUN_CHECK)
        {
            size_t check_unsaved_rows    = 0;
            size_t check_unsaved_deletes = 0;
            size_t total_rows            = 0;
            size_t total_deletes         = 0;
            for (auto & pack : packs_copy)
            {
                if (!pack->isSaved())
                {
                    check_unsaved_rows += pack->rows;
                    check_unsaved_deletes += pack->isDeleteRange();
                }
                total_rows += pack->rows;
                total_deletes += pack->isDeleteRange();
            }
            if (unlikely(check_unsaved_rows + flush_rows != unsaved_rows             //
                         || check_unsaved_deletes + flush_deletes != unsaved_deletes //
                         || total_rows != rows                                       //
                         || total_deletes != deletes))
                throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);
        }

        /// Save the new metadata of packs to disk.
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializeSavedPacks(buf, packs_copy);
        const auto data_size = buf.count();

        wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
        wbs.writeMeta();

        /// Commit updates in memory.
        packs.swap(packs_copy);

        for (auto & pack : packs)
        {
            if (pack->cache && pack->data_page != 0 && pack->rows >= context.delta_small_pack_rows)
            {
                // This pack is too large to use cache.
                pack->cache        = {};
                pack->cache_offset = 0;
            }
        }

        unsaved_rows -= flush_rows;
        unsaved_deletes -= flush_deletes;

        LOG_DEBUG(log,
                  simpleInfo() << " Flush end. Flushed " << tasks.size() << " packs, " << flush_rows << " rows and " << flush_deletes
                               << " deletes.");
    }


    ProfileEvents::increment(ProfileEvents::DMWriteBytes, flush_bytes);

    // Also update the write amplification
    auto total_write  = ProfileEvents::counters[ProfileEvents::DMWriteBytes].load(std::memory_order_relaxed);
    auto actual_write = ProfileEvents::counters[ProfileEvents::PSMWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferFromFileDescriptorWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferAIOWriteBytes].load(std::memory_order_relaxed);
    GET_METRIC(context.metrics, tiflash_storage_write_amplification)
        .Set((double)(actual_write / 1024 / 1024) / (total_write / 1024 / 1024));

    return true;
}

} // namespace DB::DM
