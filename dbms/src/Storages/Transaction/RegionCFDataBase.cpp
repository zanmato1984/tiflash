#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>
#include <Storages/Transaction/RegionRangeKeys.h>

namespace DB
{

const std::string RegionWriteCFDataTrait::name = "write";
const std::string RegionDefaultCFDataTrait::name = "default";
const std::string RegionLockCFDataTrait::name = "lock";

template <typename Trait>
const TiKVKey & RegionCFDataBase<Trait>::getTiKVKey(const Value & val)
{
    return *std::get<0>(val);
}

template <typename Value>
const std::shared_ptr<const TiKVValue> & getTiKVValuePtr(const Value & val)
{
    return std::get<1>(val);
}

template <typename Trait>
const TiKVValue & RegionCFDataBase<Trait>::getTiKVValue(const Value & val)
{
    return *getTiKVValuePtr<Value>(val);
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(TiKVKey && key, TiKVValue && value)
{
    const auto & raw_key = RecordKVFormat::decodeTiKVKey(key);
    return insert(std::move(key), std::move(value), raw_key);
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(TiKVKey && key, TiKVValue && value, const DecodedTiKVKey & raw_key)
{
    Pair kv_pair = Trait::genKVPair(std::move(key), raw_key, std::move(value));
    if (shouldIgnoreInsert(kv_pair.second))
        return false;

    return insert(std::move(kv_pair));
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(std::pair<Key, Value> && kv_pair)
{
    auto & map = data;
    auto [it, ok] = map.emplace(std::move(kv_pair));
    if (!ok)
        throw Exception("Found existing key in hex: " + getTiKVKey(it->second).toHex(), ErrorCodes::LOGICAL_ERROR);

    if constexpr (std::is_same_v<Trait, RegionWriteCFDataTrait>)
        pre_decode.add(Trait::getRowRawValuePtr(it->second));
    else
        pre_decode.add(getTiKVValuePtr(it->second));
    return true;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const Value & value)
{
    return calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value));
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value)
{
    if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
        return 0;
    else
        return key.dataSize() + value.dataSize();
}


template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreRemove(const RegionCFDataBase::Value &)
{
    return false;
}

template <>
bool RegionCFDataBase<RegionWriteCFDataTrait>::shouldIgnoreRemove(const RegionCFDataBase::Value & value)
{
    return RegionWriteCFDataTrait::getWriteType(value) == CFModifyFlag::DelFlag;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreInsert(const RegionCFDataBase::Value &)
{
    return false;
}

template <>
bool RegionCFDataBase<RegionWriteCFDataTrait>::shouldIgnoreInsert(const RegionCFDataBase::Value & value)
{
    // only keep records with DelFlag or PutFlag.
    const auto flag = RegionWriteCFDataTrait::getWriteType(value);
    return flag != CFModifyFlag::DelFlag && flag != CFModifyFlag::PutFlag;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::remove(const Key & key, bool quiet)
{
    auto & map = data;

    if (auto it = map.find(key); it != map.end())
    {
        const Value & value = it->second;

        if (shouldIgnoreRemove(value))
            return 0;

        size_t size = calcTiKVKeyValueSize(value);
        map.erase(it);
        return size;
    }
    else if (!quiet)
        throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);

    return 0;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::cmp(const Map & a, const Map & b)
{
    if (a.size() != b.size())
        return false;
    for (const auto & [key, value] : a)
    {
        if (auto it = b.find(key); it != b.end())
        {
            if (getTiKVKey(value) != getTiKVKey(it->second) || getTiKVValue(value) != getTiKVValue(it->second))
                return false;
        }
        else
            return false;
    }
    return true;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::operator==(const RegionCFDataBase & cf) const
{
    return cmp(cf.data, data);
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::getSize() const
{
    return data.size();
}

template <typename Trait>
RegionCFDataBase<Trait>::RegionCFDataBase(RegionCFDataBase && region) : data(std::move(region.data))
{}

template <typename Trait>
RegionCFDataBase<Trait> & RegionCFDataBase<Trait>::operator=(RegionCFDataBase && region)
{
    data = std::move(region.data);
    return *this;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::mergeFrom(const RegionCFDataBase & ori_region_data)
{
    size_t size_changed = 0;

    const auto & ori_map = ori_region_data.data;
    auto & tar_map = data;

    for (auto it = ori_map.begin(); it != ori_map.end(); it++)
    {
        size_changed += calcTiKVKeyValueSize(it->second);
        auto ok = tar_map.emplace(*it).second;
        if (!ok)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": got duplicate key", ErrorCodes::LOGICAL_ERROR);
    }

    return size_changed;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::splitInto(const RegionRange & range, RegionCFDataBase & new_region_data)
{
    const auto & [start_key, end_key] = range;
    size_t size_changed = 0;

    {
        auto & ori_map = data;
        auto & tar_map = new_region_data.data;

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            if (start_key.compare(key) <= 0 && end_key.compare(key) > 0)
            {
                size_changed += calcTiKVKeyValueSize(it->second);
                tar_map.insert(std::move(*it));
                it = ori_map.erase(it);
            }
            else
                ++it;
        }
    }
    return size_changed;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::serialize(WriteBuffer & buf) const
{
    size_t total_size = 0;

    size_t size = getSize();

    total_size += writeBinary2(size, buf);

    for (const auto & ele : data)
    {
        const auto & key = getTiKVKey(ele.second);
        const auto & value = getTiKVValue(ele.second);
        total_size += key.serialize(buf);
        total_size += value.serialize(buf);
    }

    return total_size;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data)
{
    size_t size = readBinary2<size_t>(buf);
    size_t cf_data_size = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        const auto size = calcTiKVKeyValueSize(key, value);
        new_region_data.insert(std::move(key), std::move(value));
        cf_data_size += size;
    }
    return cf_data_size;
}

template <typename Trait>
const typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getData() const
{
    return data;
}

template <typename Trait>
typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getDataMut()
{
    return data;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::deleteRange(const RegionRange & range)
{
    size_t size_changed = 0;

    const auto & [start_key, end_key] = range;

    {
        auto & ori_map = data;

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            if (start_key.compare(key) <= 0 && end_key.compare(key) > 0)
            {
                size_changed += calcTiKVKeyValueSize(it->second);
                it = ori_map.erase(it);
            }
            else
                ++it;
        }
    }

    return size_changed;
}

template <typename Trait>
CFDataPreDecode<Trait> & RegionCFDataBase<Trait>::getCFDataPreDecode()
{
    return pre_decode;
}

template <>
RegionDataRes RegionCFDataBase<RegionLockCFDataTrait>::insert(
    std::pair<RegionLockCFDataTrait::Key, RegionLockCFDataTrait::Value> && kv_pair)
{
    // according to the process of pessimistic lock, just overwrite.
    data.insert_or_assign(std::move(kv_pair.first), std::move(kv_pair.second));
    return true;
}

template struct RegionCFDataBase<RegionWriteCFDataTrait>;
template struct RegionCFDataBase<RegionDefaultCFDataTrait>;
template struct RegionCFDataBase<RegionLockCFDataTrait>;

} // namespace DB
