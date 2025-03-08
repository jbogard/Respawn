using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    internal class SqlServerDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        private byte? _compatibilityLevel;
        private int? _engineEdition;

        public string BuildTableCommandText(RespawnerOptions options)
        {
            string commandText = @"
select s.name, t.name
from sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE 1=1";

            if (options.TablesToIgnore.Any())
            {
                var tablesToIgnoreGroups = options.TablesToIgnore
                    .GroupBy(
                        t => t.Schema != null,
                        t => t,
                        (hasSchema, tables) => new
                        {
                            HasSchema = hasSchema, Tables = tables
                        })
                    .ToList();
                foreach (var tableGroup in tablesToIgnoreGroups)
                {
                    if (tableGroup.HasSchema)
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Schema}.{table.Name}'"));

                        commandText += " AND s.name + '.' + t.name NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Name}'"));

                        commandText += " AND t.name NOT IN (" + args + ")";
                    }
                }
            }
            if (options.TablesToInclude.Any())
            {
                var tablesToIncludeGroups = options.TablesToInclude
                    .GroupBy(
                        t => t.Schema != null,
                        t => t,
                        (hasSchema, tables) => new
                        {
                            HasSchema = hasSchema,
                            Tables = tables
                        })
                    .ToList();
                foreach (var tableGroup in tablesToIncludeGroups)
                {
                    if (tableGroup.HasSchema)
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Schema}.{table.Name}'"));

                        commandText += " AND s.name + '.' + t.name IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Name}'"));

                        commandText += " AND t.name IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(schema => $"N'{schema}'"));

                commandText += " AND s.name NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(schema => $"N'{schema}'"));

                commandText += " AND s.name IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            string commandText = @"
select
   fk_schema.name, so_fk.name,
   pk_schema.name, so_pk.name,
   sfk.name
from
sys.foreign_keys sfk
    inner join sys.objects so_pk on sfk.referenced_object_id = so_pk.object_id
    inner join sys.schemas pk_schema on so_pk.schema_id = pk_schema.schema_id
    inner join sys.objects so_fk on sfk.parent_object_id = so_fk.object_id
    inner join sys.schemas fk_schema on so_fk.schema_id = fk_schema.schema_id
where 1=1";

            if (options.TablesToIgnore.Any())
            {
                var tablesToIgnoreGroups = options.TablesToIgnore
                    .GroupBy(
                        t => t.Schema != null,
                        t => t,
                        (hasSchema, tables) => new
                        {
                            HasSchema = hasSchema,
                            Tables = tables
                        })
                    .ToList();
                foreach (var tableGroup in tablesToIgnoreGroups)
                {
                    if (tableGroup.HasSchema)
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Schema}.{table.Name}'"));

                        commandText += " AND pk_schema.name + '.' + so_pk.name NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Name}'"));

                        commandText += " AND so_pk.name NOT IN (" + args + ")";
                    }
                }
            }
            if (options.TablesToInclude.Any())
            {
                var tablesToIncludeGroups = options.TablesToInclude
                    .GroupBy(
                        t => t.Schema != null,
                        t => t,
                        (hasSchema, tables) => new
                        {
                            HasSchema = hasSchema,
                            Tables = tables
                        })
                    .ToList();
                foreach (var tableGroup in tablesToIncludeGroups)
                {
                    if (tableGroup.HasSchema)
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Schema}.{table.Name}'"));

                        commandText += " AND pk_schema.name + '.' + so_pk.name IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Name}'"));

                        commandText += " AND so_pk.name IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(schema => $"N'{schema}'"));

                commandText += " AND pk_schema.name NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(schema => $"N'{schema}'"));

                commandText += " AND pk_schema.name IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph, RespawnerOptions options)
        {
            var builder = new StringBuilder();

            foreach (var table in graph.CyclicalTableRelationships.Select(rel => rel.ParentTable))
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} NOCHECK CONSTRAINT ALL;");
            }
            foreach (var table in graph.ToDelete)
            {
                builder.AppendLine(options.FormatDeleteStatement?.Invoke(table) ?? $"DELETE {table.GetFullName(QuoteCharacter)};");
            }
            foreach (var table in graph.CyclicalTableRelationships.Select(rel => rel.ParentTable))
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} WITH CHECK CHECK CONSTRAINT ALL;");
            }

            return builder.ToString();
        }

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
        {
            string sql = $@"
DECLARE @Schema sysname = N''
DECLARE @TableName sysname = N''
DECLARE @DoReseed sql_variant = 0
DECLARE @NewSeed bigint = 0
DECLARE @IdentityInitialSeedValue int = 0
DECLARE @SQL nvarchar(4000) = N''

-- find all non-system tables and load into a cursor
DECLARE IdentityTables CURSOR FAST_FORWARD
FOR
    SELECT  QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id, db_id())) as schemaName,
            QUOTENAME(t.name) as tableName,
            ic.last_value,
            IDENT_SEED(QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id, db_id())) + '.' + QUOTENAME(t.name)) as identityInitialSeedValue
     FROM sys.tables t
        JOIN sys.columns c ON t.object_id=c.object_id
        JOIN sys.identity_columns ic on ic.object_id = c.object_id
    WHERE c.is_identity = 1
    AND OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name in ('{string.Join("', '", tablesToDelete)}')
OPEN IdentityTables
FETCH NEXT FROM IdentityTables INTO @Schema, @TableName, @DoReseed, @IdentityInitialSeedValue
WHILE @@FETCH_STATUS = 0
    BEGIN
     -- reseed the identity only on tables that actually have had a value, otherwise next value will be off-by-one
     -- https://stackoverflow.com/questions/472578/dbcc-checkident-sets-identity-to-0
        if (@DoReseed is not null)
           SET @SQL = N'DBCC CHECKIDENT(''' +  @Schema + '.' + @TableName + ''', RESEED, ' + Convert(varchar(max), @IdentityInitialSeedValue - 1) + ')'
        else
           SET @SQL = null
        if (@sql is not null) EXECUTE (@SQL)
        --Print isnull(@sql,  @Schema + '.' + @TableName + ' null')
        FETCH NEXT FROM IdentityTables INTO  @Schema, @TableName, @DoReseed, @IdentityInitialSeedValue
    END
 DEALLOCATE IdentityTables";

            return sql;
        }

        public string BuildTemporalTableCommandText(RespawnerOptions options)
        {
            string commandText = @"
select s.name, t.name, temp_s.name, temp_t.name
from sys.tables t
INNER JOIN sys.schemas s on t.schema_id = s.schema_id
INNER JOIN sys.tables temp_t on t.history_table_id = temp_t.object_id
INNER JOIN sys.schemas temp_s on temp_t.schema_id = temp_s.schema_id
WHERE t.temporal_type = 2";

            if (options.TablesToIgnore.Any())
            {
                var tablesToIgnoreGroups = options.TablesToIgnore
                    .GroupBy(
                        t => t.Schema != null,
                        t => t,
                        (hasSchema, tables) => new
                        {
                            HasSchema = hasSchema,
                            Tables = tables
                        })
                    .ToList();

                foreach (var tableGroup in tablesToIgnoreGroups)
                {
                    if (tableGroup.HasSchema)
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Schema}.{table.Name}'"));

                        commandText += " AND s.name + '.' + t.name NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"N'{table.Name}'"));

                        commandText += " AND t.name NOT IN (" + args + ")";
                    }
                }
            }
            return commandText;
        }

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning)
        {
            var builder = new StringBuilder();
            foreach (var table in tablesToTurnOffSystemVersioning)
            {
                builder.Append($"alter table {QuoteCharacter}{table.Schema}{QuoteCharacter}.{QuoteCharacter}{table.Name}{QuoteCharacter} set (SYSTEM_VERSIONING = OFF);\r\n");
            }
            return builder.ToString();
        }

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning)
        {
            var builder = new StringBuilder();
            foreach (var table in tablesToTurnOnSystemVersioning)
            {
                builder.Append($"alter table {QuoteCharacter}{table.Schema}{QuoteCharacter}.{QuoteCharacter}{table.Name}{QuoteCharacter} set (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {QuoteCharacter}{table.HistoryTableSchema}{QuoteCharacter}.{QuoteCharacter}{table.HistoryTableName}{QuoteCharacter}));\r\n");
            }
            return builder.ToString();
        }

        public async Task<bool> CheckSupportsTemporalTables(DbConnection connection)
        {
            _compatibilityLevel ??= await GetCompatibilityLevel(connection).ConfigureAwait(false);
            _engineEdition ??= await GetEngineEdition(connection).ConfigureAwait(false);

            //Code taken from https://github.com/dotnet/efcore/blob/main/src/EFCore.SqlServer/Scaffolding/Internal/SqlServerDatabaseModelFactory.cs
            return _compatibilityLevel >= 130 && _engineEdition != 6;
        }

        private static async Task<int> GetEngineEdition(DbConnection connection)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT SERVERPROPERTY('EngineEdition');";
            var engineEdition = await command.ExecuteScalarAsync().ConfigureAwait(false);
            return (int)engineEdition!;
        }

        private static async Task<byte> GetCompatibilityLevel(DbConnection connection)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $@"
SELECT compatibility_level
FROM sys.databases
WHERE name = N'{connection.Database}';";

            var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
            return result != null ? Convert.ToByte(result) : (byte)0;
        }

        public bool RequiresStatementsToBeExecutedIndividually() => false;
    }
}