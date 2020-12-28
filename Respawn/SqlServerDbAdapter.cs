using System.Collections.Generic;
using System.Linq;
using System.Text;
using Respawn.Graph;

namespace Respawn
{
    internal class SqlServerDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        public string BuildTableCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
select s.name, t.name
from sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE 1=1";

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                commandText += " AND t.name NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(t => $"N'{t}'"));

                commandText += " AND t.name IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"N'{t}'"));

                commandText += " AND s.name NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"N'{t}'"));

                commandText += " AND s.name IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(Checkpoint checkpoint)
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

            if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                commandText += " AND so_pk.name NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude != null && checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(t => $"N'{t}'"));

                commandText += " AND so_pk.name IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"N'{t}'"));

                commandText += " AND pk_schema.name NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"N'{t}'"));

                commandText += " AND pk_schema.name IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var builder = new StringBuilder();

            foreach (var table in graph.CyclicalTableRelationships.Select(rel => rel.ParentTable))
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} NOCHECK CONSTRAINT ALL;");
            }
            foreach (var table in graph.ToDelete)
            {
                builder.AppendLine($"DELETE {table.GetFullName(QuoteCharacter)};");
            }
            foreach (var table in graph.CyclicalTableRelationships.Select(rel => rel.ParentTable))
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} WITH CHECK CHECK CONSTRAINT ALL;");
            }

            return builder.ToString();
        }

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
        {
            string sql =
                "DECLARE @Schema sysname = N''                                                                                                     			\n" +
                "DECLARE @TableName sysname = N''                                                                                                  			\n" +
                "DECLARE @ColumnName sysname = N''                                                                                                 			\n" +
                "DECLARE @DoReseed sql_variant = 0																											\n" +
                "DECLARE @NewSeed bigint = 0                                                                                                       			\n" +
                "DECLARE @IdentityInitialSeedValue int = 0                                                                                                  \n" +
                "DECLARE @SQL nvarchar(4000) = N''                                                                                                 			\n" +
                "                                                                                                                                  			\n" +
                "-- find all non-system tables and load into a cursor                                                                              			\n" +
                "DECLARE IdentityTables CURSOR FAST_FORWARD                                                                                        			\n" +
                "FOR                                                                                                                               			\n" +
                "    SELECT  OBJECT_SCHEMA_NAME(t.object_id, db_id()) as schemaName,                                                                        \n" +
                "            t.name as tableName,                                                                                                           \n" +
                "            c.name as columnName,                                                                                                          \n" +
                "            ic.last_value,                                                                                                                 \n" +
                "            IDENT_SEED(OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name) as identityInitialSeedValue                                \n" +
                "     FROM sys.tables t 																										            \n" +
                "		JOIN sys.columns c ON t.object_id=c.object_id      																                	\n" +
                "		JOIN sys.identity_columns ic on ic.object_id = c.object_id  												                		\n" +
                "    WHERE c.is_identity = 1                                                                                    				            \n" +
                $"    AND OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name in ('{string.Join("', '", tablesToDelete)}')                              \n" +
                "OPEN IdentityTables                                                                                                               			\n" +
                "FETCH NEXT FROM IdentityTables INTO @Schema, @TableName, @ColumnName, @DoReseed, @IdentityInitialSeedValue                                 \n" +
                "WHILE @@FETCH_STATUS = 0                                                                                                          			\n" +
                "    BEGIN                                                                                                                         			\n" +
                "     -- reseed the identity only on tables that actually have had a value, otherwise next value will be off-by-one   			            \n" +
                "     -- https://stackoverflow.com/questions/472578/dbcc-checkident-sets-identity-to-0                                                      \n" +
                "        if (@DoReseed is not null)                                                                                                         \n" +
                "           SET @SQL = N'DBCC CHECKIDENT(''' +  @Schema + '.' + @TableName + ''', RESEED, ' + Convert(varchar(max), @IdentityInitialSeedValue - 1) + ')' \n" +
                "        else                                                                                                                               \n" +
                "           SET @SQL = null	                                                                                                                \n" +
                "        if (@sql is not null) EXECUTE (@SQL)  																								\n" +
                "		--Print isnull(@sql,  @Schema + '.' + @TableName + ' null')                                                                         \n" +
                "        FETCH NEXT FROM IdentityTables INTO  @Schema, @TableName, @ColumnName  , @DoReseed, @IdentityInitialSeedValue                      \n" +
                "    END                                                                                                                           			\n" +
                " DEALLOCATE IdentityTables                                                                                                                 \n";

            return sql;
        }

        public string BuildTemporalTableCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
select s.name, t.name, temp_s.name, temp_t.name
from sys.tables t
INNER JOIN sys.schemas s on t.schema_id = s.schema_id
INNER JOIN sys.tables temp_t on t.history_table_id = temp_t.object_id
INNER JOIN sys.schemas temp_s on temp_t.schema_id = temp_s.schema_id
WHERE t.temporal_type = 2";

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                commandText += " AND t.name NOT IN (" + args + ")";
            }
            return commandText;
        }

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning)
        {
            var builder = new StringBuilder();
            foreach (var table in tablesToTurnOffSystemVersioning)
            {
                builder.Append($"alter table {table.Schema}.{table.Name} set (SYSTEM_VERSIONING = OFF);\r\n");
            }
            return builder.ToString();
        }

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning)
        {
            var builder = new StringBuilder();
            foreach (var table in tablesToTurnOnSystemVersioning)
            {
                builder.Append($"alter table {table.Schema}.{table.Name} set (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {table.HistoryTableSchema}.{table.HistoryTableName}));\r\n");
            }
            return builder.ToString();
        }

        public bool SupportsTemporalTables => true;
    }
}