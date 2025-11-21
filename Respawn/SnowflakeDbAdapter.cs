using Respawn.Graph;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Respawn
{
    public class SnowflakeDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';
        private const string InformationSchema = "INFORMATION_SCHEMA";

        public string BuildTableCommandText(RespawnerOptions options)
        {
            string commandText = @"
select TABLE_SCHEMA, TABLE_NAME
from INFORMATION_SCHEMA.TABLES
where TABLE_TYPE = 'BASE TABLE'"
                ;

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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema!.ToUpper()}.{table.Name.ToUpper()}'"));

                        commandText += " AND TABLE_SCHEMA || '.' || TABLE_NAME NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name.ToUpper()}'"));

                        commandText += " AND TABLE_NAME NOT IN (" + args + ")";
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema!.ToUpper()}.{table.Name.ToUpper()}'"));

                        commandText += " AND TABLE_SCHEMA || '.' || TABLE_NAME IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name.ToUpper()}'"));

                        commandText += " AND TABLE_NAME IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t.ToUpper()}'"));
                args += $", '{InformationSchema}'";

                commandText += " AND TABLE_SCHEMA NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t.ToUpper()}'"));

                commandText += " AND TABLE_SCHEMA IN (" + args + ")";
            }
            else
            {
                commandText += $" AND TABLE_SCHEMA != '{InformationSchema}'";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            string commandText = @"select '' as name, '' as name, '' as name, '' as name, '' as name";
            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var builder = new StringBuilder();

            foreach (var table in graph.ToDelete)
            {
                string tableName = table.GetFullName(QuoteCharacter);
                tableName = tableName.Replace("\"", "");

                builder.AppendLine($"DELETE FROM {tableName};");
            }

            return builder.ToString();
        }

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete) => throw new System.NotImplementedException();

        public string BuildTemporalTableCommandText(RespawnerOptions options) => throw new System.NotImplementedException();

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();

        public Task<bool> CheckSupportsTemporalTables(DbConnection connection)
        {
            return Task.FromResult(false);
        }

        public bool RequiresStatementsToBeExecutedIndividually() => true;
    }
}