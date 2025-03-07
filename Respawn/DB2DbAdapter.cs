using Respawn.Graph;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Respawn
{
    internal class DB2DbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';
        private const string SysToolsSchema = "SYSTOOLS";

        public string BuildTableCommandText(RespawnerOptions options)
        {
            var sb = new StringBuilder(@"SELECT TABSCHEMA, TABNAME
                               FROM SYSCAT.TABLES
                               WHERE TYPE = 'T'
                               AND OWNERTYPE = 'U'");

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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema!.ToUpperInvariant()}.{table.Name.ToUpperInvariant()}'"));

                        sb.Append($" AND TABSCHEMA || '.' || TABNAME NOT IN ({args})");
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name.ToUpperInvariant()}'"));

                        sb.Append($" AND TABNAME NOT IN ({args})");
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema!.ToUpperInvariant()}.{table.Name.ToUpperInvariant()}'"));

                        sb.Append($" AND TABSCHEMA || '.' || TABNAME IN ({args})");
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name.ToUpperInvariant()}'"));

                        sb.Append($" AND TABNAME IN ({args})");
                    }
                }
            }

            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t.ToUpperInvariant()}'"));
                args += $", '{SysToolsSchema}'";
                sb.Append($" AND TABSCHEMA NOT IN ({args})");
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t.ToUpperInvariant()}'"));

                sb.Append($" AND TABSCHEMA IN ({args})");
            }
            else
            {
                sb.Append($" AND TABSCHEMA != '{SysToolsSchema}'");
            }

            return sb.ToString();
        }


        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            var sb = new StringBuilder();

            sb.Append(@"SELECT TABSCHEMA, TABNAME, REFTABSCHEMA, REFTABNAME, CONSTNAME FROM SYSCAT.REFERENCES WHERE 1=1");

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

                        sb.Append(" AND TABSCHEMA + '.' + TABSCHEMA NOT IN (" + args + ")");
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        sb.Append(" AND TABNAME NOT IN (" + args + ")");
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema!.ToUpperInvariant()}.{table.Name.ToUpperInvariant()}'"));

                        sb.Append(" AND TABSCHEMA + '.' + TABNAME IN (" + args + ")");
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name.ToUpperInvariant()}'"));

                        sb.Append(" AND TABNAME IN (" + args + ")");
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t.ToUpperInvariant()}'"));

                sb.Append(" AND TABSCHEMA NOT IN (" + args + ")");
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t.ToUpperInvariant()}'"));

                sb.Append(" AND TABSCHEMA IN (" + args + ")");
            }


            return sb.ToString();
        }

        public string BuildDeleteCommandText(GraphBuilder builder, RespawnerOptions options)
        {
            var sb = new StringBuilder();

            foreach (var table in builder.CyclicalTableRelationships)
            {
                sb.AppendLine($"ALTER TABLE {table.ParentTable} ALTER FOREIGN KEY {table.Name} NOT ENFORCED;");
            }
            foreach (var table in builder.ToDelete)
            {
                sb.AppendLine(options.FormatDeleteStatement?.Invoke(table) ?? $"DELETE FROM {table.GetFullName(QuoteCharacter)};");
            }
            foreach (var table in builder.CyclicalTableRelationships)
            {
                sb.AppendLine($"ALTER TABLE {table.ParentTable} ALTER FOREIGN KEY {table.Name} ENFORCED;");
            }

            return sb.ToString();
        }

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
        {
            throw new System.NotImplementedException();
        }

        public string BuildTemporalTableCommandText(RespawnerOptions options)
        {
            throw new System.NotImplementedException();
        }

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning)
        {
            throw new System.NotImplementedException();
        }

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning)
        {
            throw new System.NotImplementedException();
        }

        public Task<bool> CheckSupportsTemporalTables(DbConnection connection)
        {
            return Task.FromResult(false);
        }
    }
}
