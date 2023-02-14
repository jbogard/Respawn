using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    internal class OracleDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        public string BuildTableCommandText(RespawnerOptions options)
        {
            string commandText = @"
select OWNER, TABLE_NAME
from ALL_TABLES
where 1=1 "
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        commandText += " AND OWNER || '.' || TABLE_NAME NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        commandText += " AND OWNER || '.' || TABLE_NAME IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND TABLE_NAME IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(schema => $"'{schema}'").ToArray());

                commandText += " AND OWNER NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(schema => $"'{schema}'").ToArray());

                commandText += " AND OWNER IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            string commandText = @"
select a.owner as table_schema,a.table_name, b.owner as table_schema ,b.table_name, a.constraint_name
from all_CONSTRAINTS     a
         inner join all_CONSTRAINTS b on a.r_constraint_name=b.constraint_name  AND a.r_owner=b.owner
         where a.constraint_type in ('P','R')";

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

                        commandText += " AND a.OWNER || '.' || a.TABLE_NAME NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND a.TABLE_NAME NOT IN (" + args + ")";
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

                        commandText += " AND a.OWNER || '.' || a.TABLE_NAME IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND a.TABLE_NAME IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(s => $"'{s}'").ToArray());

                commandText += " AND a.OWNER NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(s => $"'{s}'").ToArray());

                commandText += " AND a.OWNER IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var deleteSql = string.Join("\n", BuildCommands(graph));
            return $"BEGIN\n{deleteSql}\nEND;";
        }

        private static IEnumerable<string> BuildCommands(GraphBuilder graph)
        {
            foreach (var rel in graph.CyclicalTableRelationships)
            {
                yield return $"EXECUTE IMMEDIATE 'ALTER TABLE {rel.ParentTable.GetFullName(QuoteCharacter)} DISABLE CONSTRAINT {QuoteCharacter}{rel.Name}{QuoteCharacter}';";
            }
            foreach (var table in graph.ToDelete)
            {
                yield return $"EXECUTE IMMEDIATE 'delete from {table.GetFullName(QuoteCharacter)}';";
            }
            foreach (var rel in graph.CyclicalTableRelationships)
            {
                yield return $"EXECUTE IMMEDIATE 'ALTER TABLE {rel.ParentTable.GetFullName(QuoteCharacter)} ENABLE CONSTRAINT {QuoteCharacter}{rel.Name}{QuoteCharacter}';";
            }
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