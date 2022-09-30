using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    internal class InformixDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        public string BuildTableCommandText(RespawnerOptions options)
        {
            string commandText = @"SELECT t.owner, t.tabname
                                       FROM 'informix'.systables t
                                       WHERE t.tabtype = 'T'
  	                                    AND t.tabid >= 100";

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

                        commandText += " AND t.owner + '.' + t.tabname NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND t.tabname NOT IN (" + args + ")";
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

                        commandText += " AND t.owner + '.' + t.tabname IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND t.tabname IN (" + args + ")";
                    }
                }
            }

            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND t.owner NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND t.owner IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            string commandText = @"SELECT T2.owner, T2.tabname, T1.owner, T1.tabname, C.constrname
                                       FROM sysreferences R
                                       	INNER JOIN sysconstraints C
                                       		ON R.constrid = C.constrid
                                       	INNER JOIN systables T1
                                       		ON (T1.tabid = R.ptabid) 
                                       	INNER JOIN systables T2
                                       		ON T2.tabid = C.tabid
                                       WHERE C.constrtype = 'R'
                                       	AND (T1.tabtype = 'T' AND T1.tabid >= 100)
                                       	AND (T2.tabtype = 'T' AND T2.tabid >= 100)";

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

                        commandText += " AND T2.owner + '.' + T2.tabname NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND T2.tabname NOT IN (" + args + ")";
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

                        commandText += " AND T2.owner + '.' + T2.tabname IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND T2.tabname IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND T2.owner NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND T2.owner IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var builder = new StringBuilder();

            foreach (var table in graph.CyclicalTableRelationships)
            {
                builder.AppendLine($"SET CONSTRAINTS {QuoteCharacter}{table.Name}{QuoteCharacter} DISABLED;");
            }
            foreach (var table in graph.ToDelete)
            {
                builder.AppendLine($"DELETE FROM {table.GetFullName(QuoteCharacter)};");
            }
            foreach (var table in graph.CyclicalTableRelationships)
            {
                builder.AppendLine($"SET CONSTRAINTS {QuoteCharacter}{table.Name}{QuoteCharacter} ENABLED;");
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
    }
}