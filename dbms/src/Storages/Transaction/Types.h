#pragma once

#include <chrono>
#include <unordered_set>

#include <Core/Types.h>

namespace DB
{

using TableID = Int64;
using TableIDSet = std::unordered_set<TableID>;

enum : TableID
{
    InvalidTableID = 0,
};

using DatabaseID = Int64;

using ColumnID = Int64;

enum : ColumnID
{
    InvalidColumnID = -1
};

using HandleID = Int64;
using Timestamp = UInt64;

using RegionID = UInt64;

enum : RegionID
{
    InvalidRegionID = 0
};

using RegionVersion = UInt64;

enum : RegionVersion
{
    InvalidRegionVersion = std::numeric_limits<RegionVersion>::max()
};

using Clock = std::chrono::system_clock;
using Timepoint = Clock::time_point;
using Duration = Clock::duration;
using Seconds = std::chrono::seconds;

} // namespace DB
