using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    internal class MySqlAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '`';

        public string BuildTableCommandText(RespawnerOptions options)
        {
            string commandText = @"
SELECT t.TABLE_SCHEMA, t.TABLE_NAME
FROM
    information_schema.tables AS t
WHERE
    table_type = 'BASE TABLE'
    AND TABLE_SCHEMA NOT IN ('mysql' , 'performance_schema')";

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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        commandText += " AND t.TABLE_SCHEMA + '.' + t.TABLE_NAME NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND t.TABLE_NAME NOT IN (" + args + ")";
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        commandText += " AND t.TABLE_SCHEMA + '.' + t.TABLE_NAME IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND t.TABLE_NAME IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND t.TABLE_SCHEMA NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND t.TABLE_SCHEMA IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            var commandText = @"
SELECT 
    CONSTRAINT_SCHEMA, 
    TABLE_NAME,
    UNIQUE_CONSTRAINT_SCHEMA, 
    REFERENCED_TABLE_NAME, 
    CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS";

            var whereText = new List<string>();

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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        whereText.Add("CONSTRAINT_SCHEMA + '.' + TABLE_NAME NOT IN (" + args + ")");
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        whereText.Add("TABLE_NAME NOT IN (" + args + ")");
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        whereText.Add("CONSTRAINT_SCHEMA + '.' + TABLE_NAME IN (" + args + ")");
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        whereText.Add("TABLE_NAME IN (" + args + ")");
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));
                whereText.Add("CONSTRAINT_SCHEMA NOT IN (" + args + ")");
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));
                whereText.Add("CONSTRAINT_SCHEMA IN (" + args + ")");
            }

            if (whereText.Any())
                commandText += $" WHERE {string.Join(" AND ", whereText.ToArray())}";
            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var builder = new StringBuilder();

            builder.AppendLine("SET FOREIGN_KEY_CHECKS=0;");
            foreach (var table in graph.ToDelete)
            {
                builder.AppendLine($"DELETE FROM {table.GetFullName(QuoteCharacter)};");
            }
            builder.AppendLine("SET FOREIGN_KEY_CHECKS=1;");

            return builder.ToString();
        }

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
        {
            var builder = new StringBuilder();
            foreach (var table in tablesToDelete)
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} AUTO_INCREMENT = 1;");
            }

            return builder.ToString();
        }

        public string BuildTemporalTableCommandText(RespawnerOptions options) => throw new System.NotImplementedException();

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();
        
        public Task<bool> CheckSupportsTemporalTables(DbConnection connection)
        {
            return Task.FromResult(false);
        }
    }
}