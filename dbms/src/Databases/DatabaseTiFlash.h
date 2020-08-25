#pragma once

#include <Databases/DatabasesCommon.h>
#include <Encryption/FileProvider.h>

namespace TiDB
{
struct DBInfo;
using DBInfoPtr = std::shared_ptr<DBInfo>;
} // namespace TiDB

namespace DB
{

class DatabaseTiFlash : public DatabaseWithOwnTablesBase
{
public:
    using Version = UInt32;
    static constexpr Version CURRENT_VERSION = 1;

public:
    DatabaseTiFlash(String name_, const String & metadata_path_, const TiDB::DBInfo & db_info_, Version version_, const Context & context);

    String getEngineName() const override { return "TiFlash"; }


    void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag) override;

    void createTable(const Context & context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void removeTable(const Context & context, const String & table_name) override;

    // Rename action synced from TiDB should use this method.
    // We need display database / table name for updating TiDB::TableInfo
    void renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name,
        const String & display_database, const String & display_table);

    // This method should never called.
    void renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name) override;


    void alterTable(
        const Context & context, const String & name, const ColumnsDescription & columns, const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(const Context & context, const String & table_name) override;

    ASTPtr getCreateTableQuery(const Context & context, const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(const Context & context, const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    String getDataPath() const override;
    String getMetadataPath() const override;
    String getTableMetadataPath(const String & table_name) const override;

    void shutdown() override;
    void drop(const Context & context) override;

    TiDB::DBInfo & getDatabaseInfo() const;

private:
    const String metadata_path;
    const String data_path;
    TiDB::DBInfoPtr db_info;
    Poco::Logger * log;

    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};

} // namespace DB
