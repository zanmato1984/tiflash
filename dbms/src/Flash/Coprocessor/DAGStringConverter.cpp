#include <Flash/Coprocessor/DAGStringConverter.h>

#include <Core/QueryProcessingStage.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int COP_BAD_DAG_REQUEST;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

void DAGStringConverter::buildTSString(const tipb::TableScan & ts, std::stringstream & ss)
{
    TableID table_id;
    if (ts.has_table_id())
    {
        table_id = ts.table_id();
    }
    else
    {
        // do not have table id
        throw Exception("Table id not specified in table scan executor", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(table_id);
    if (storage == nullptr)
    {
        throw Exception("Table " + std::to_string(table_id) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
    }
    const auto * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
    if (!merge_tree)
    {
        throw Exception("Only MergeTree table is supported in DAG request", ErrorCodes::COP_BAD_DAG_REQUEST);
    }

    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw Exception("No column is selected in table scan executor", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    const auto & column_list = storage->getColumns().getAllPhysical();
    for (auto & column : column_list)
    {
        columns_from_ts.emplace_back(column.name, column.type);
    }
    for (const tipb::ColumnInfo & ci : ts.columns())
    {
        ColumnID cid = ci.column_id();
        if (cid == -1)
        {
            // Column ID -1 returns the handle column
            auto pk_handle_col = storage->getTableInfo().getPKHandleColumn();
            pk_handle_col.has_value();
            auto pair = storage->getColumns().getPhysical(
                pk_handle_col.has_value() ? pk_handle_col->get().name : MutableSupport::tidb_pk_column_name);
            columns_from_ts.push_back(pair);
            continue;
        }
        auto name = storage->getTableInfo().getColumnName(cid);
        auto pair = storage->getColumns().getPhysical(name);
        columns_from_ts.push_back(pair);
    }
    ss << "FROM " << storage->getDatabaseName() << "." << storage->getTableName() << " ";
}

void DAGStringConverter::buildSelString(const tipb::Selection & sel, std::stringstream & ss)
{
    bool first = true;
    for (const tipb::Expr & expr : sel.conditions())
    {
        auto s = exprToString(expr, getCurrentColumns());
        if (first)
        {
            ss << "WHERE ";
            first = false;
        }
        else
        {
            ss << "AND ";
        }
        ss << s << " ";
    }
}

void DAGStringConverter::buildLimitString(const tipb::Limit & limit, std::stringstream & ss) { ss << "LIMIT " << limit.limit() << " "; }

void DAGStringConverter::buildAggString(const tipb::Aggregation & agg, std::stringstream & ss)
{
    if (agg.group_by_size() != 0)
    {
        ss << "GROUP BY ";
        bool first = true;
        for (auto & group_by : agg.group_by())
        {
            if (first)
                first = false;
            else
                ss << ", ";
            ss << exprToString(group_by, getCurrentColumns());
        }
    }
    for (auto & agg_func : agg.agg_func())
    {
        columns_from_agg.emplace_back(exprToString(agg_func, getCurrentColumns()), getDataTypeByFieldType(agg_func.field_type()));
    }
    afterAgg = true;
}
void DAGStringConverter::buildTopNString(const tipb::TopN & topN, std::stringstream & ss)
{
    ss << "ORDER BY ";
    bool first = true;
    for (auto & order_by_item : topN.order_by())
    {
        if (first)
            first = false;
        else
            ss << ", ";
        ss << exprToString(order_by_item.expr(), getCurrentColumns()) << " ";
        ss << (order_by_item.desc() ? "DESC" : "ASC");
    }
    ss << " LIMIT " << topN.limit() << " ";
}

//todo return the error message
void DAGStringConverter::buildString(const tipb::Executor & executor, std::stringstream & ss)
{
    switch (executor.tp())
    {
        case tipb::ExecType::TypeTableScan:
            return buildTSString(executor.tbl_scan(), ss);
        case tipb::ExecType::TypeIndexScan:
            // index scan not supported
            throw Exception("IndexScan is not supported", ErrorCodes::NOT_IMPLEMENTED);
        case tipb::ExecType::TypeSelection:
            return buildSelString(executor.selection(), ss);
        case tipb::ExecType::TypeAggregation:
            // stream agg is not supported, treated as normal agg
        case tipb::ExecType::TypeStreamAgg:
            return buildAggString(executor.aggregation(), ss);
        case tipb::ExecType::TypeTopN:
            return buildTopNString(executor.topn(), ss);
        case tipb::ExecType::TypeLimit:
            return buildLimitString(executor.limit(), ss);
    }
}

bool isProject(const tipb::Executor &)
{
    // currently, project is not pushed so always return false
    return false;
}
DAGStringConverter::DAGStringConverter(Context & context_, const tipb::DAGRequest & dag_request_)
    : context(context_), dag_request(dag_request_)
{
    afterAgg = false;
}

String DAGStringConverter::buildSqlString()
{
    std::stringstream query_buf;
    std::stringstream project;
    for (const tipb::Executor & executor : dag_request.executors())
    {
        buildString(executor, query_buf);
    }
    if (!isProject(dag_request.executors(dag_request.executors_size() - 1)))
    {
        //append final project
        project << "SELECT ";
        bool first = true;
        for (UInt32 index : dag_request.output_offsets())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                project << ", ";
            }
            project << getCurrentColumns()[index].name;
        }
        project << " ";
    }
    return project.str() + query_buf.str();
}

} // namespace DB
