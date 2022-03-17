#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/WriteHelpers.h>
#include <WindowFunctions/WindowFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_WINDOW_FUNCTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

void WindowFunctionFactory::registerFunction(const String & name, Creator creator)
{
    if (creator == nullptr)
        throw Exception(
            "WindowFunctionFactory: the window function " + name + " has been provided a null constructor",
            ErrorCodes::LOGICAL_ERROR);

    if (!window_functions.emplace(name, creator).second)
        throw Exception(
            "WindowFunctionFactory: the window function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

WindowFunctionPtr WindowFunctionFactory::get(
    const String & name,
    const DataTypes & argument_types) const
{
    auto res = getImpl(name, argument_types);
    if (!res)
        throw Exception("Logical error: WindowFunctionFactory returned nullptr", ErrorCodes::LOGICAL_ERROR);
    return res;
}


WindowFunctionPtr WindowFunctionFactory::getImpl(
    const String & name,
    const DataTypes & argument_types) const
{
    /// Find by exact match.
    auto it = window_functions.find(name);
    if (it != window_functions.end())
        return it->second(name, argument_types);

    throw Exception("Unknown window function " + name, ErrorCodes::UNKNOWN_WINDOW_FUNCTION);
}


WindowFunctionPtr WindowFunctionFactory::tryGet(const String & name, const DataTypes & argument_types) const
{
    return isWindowFunctionName(name)
        ? get(name, argument_types)
        : nullptr;
}


bool WindowFunctionFactory::isWindowFunctionName(const String & name) const
{
    return window_functions.count(name);
}

} // namespace DB
