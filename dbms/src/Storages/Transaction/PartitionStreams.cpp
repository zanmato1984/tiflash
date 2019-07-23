#include <Core/Block.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{

RegionTable::BlockOption RegionTable::getBlockInputStreamByRegion(TableID table_id,
    RegionPtr region,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    RegionDataReadInfoList & data_list_for_remove)
{
    return std::get<0>(getBlockInputStreamByRegion(table_id,
        region,
        InvalidRegionVersion,
        InvalidRegionVersion,
        table_info,
        columns,
        ordered_columns,
        false,
        false,
        0,
        &data_list_for_remove,
        log));
}

std::tuple<RegionTable::BlockOption, RegionTable::RegionReadStatus> RegionTable::getBlockInputStreamByRegion(TableID table_id,
    RegionPtr region,
    const RegionVersion region_version,
    const RegionVersion conf_version,
    const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & ordered_columns,
    bool learner_read,
    bool resolve_locks,
    Timestamp start_ts,
    RegionDataReadInfoList * data_list_for_remove,
    Logger * log)
{
    if (!region)
        return {BlockOption{}, NOT_FOUND};

    if (learner_read)
        region->waitIndex(region->learnerRead());

    auto schema_fetcher = [&](TableID) {
        return std::make_tuple<const TiDB::TableInfo *, const ColumnsDescription *, const Names *>(&table_info, &columns, &ordered_columns);
    };

    {
        RegionDataReadInfoList data_list;
        const auto [table_info, columns, ordered_columns] = schema_fetcher(table_id);

        bool need_value = true;

        if (ordered_columns->size() == 3)
            need_value = false;

        auto start_time = Clock::now();

        {
            auto scanner = region->createCommittedScanner(table_id);

            if (region->isPendingRemove())
                return {BlockOption{}, PENDING_REMOVE};

            if (region_version != InvalidRegionVersion && (region->version() != region_version || region->confVer() != conf_version))
                return {BlockOption{}, VERSION_ERROR};

            if (resolve_locks)
            {
                LockInfoPtr lock_info = scanner->getLockInfo(start_ts);
                if (lock_info)
                {
                    LockInfos lock_infos;
                    lock_infos.emplace_back(std::move(lock_info));
                    throw LockException(std::move(lock_infos));
                }
            }

            if (!scanner->hasNext())
                return {BlockOption{}, OK};

            do
            {
                data_list.emplace_back(scanner->next(need_value));
            } while (scanner->hasNext());
        }

        const auto scan_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
        start_time = Clock::now();

        auto block = RegionBlockRead(*table_info, *columns, *ordered_columns, data_list);

        auto compute_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();

        if (log)
        {
            LOG_TRACE(log,
                region->toString(false) << " read " << data_list.size() << " rows, cost [scan " << scan_cost << ", compute " << compute_cost
                                        << "] ms");
        }

        if (data_list_for_remove)
            *data_list_for_remove = std::move(data_list);

        return {std::move(block), OK};
    }
}

} // namespace DB
