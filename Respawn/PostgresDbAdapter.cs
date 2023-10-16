using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    internal class PostgresDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        // Postgres has some schemas containing internal schemas that should not be deleted.
        private const string InformationSchema = "information_schema";
        private const string PostgresSchemaPrefix = "pg_";

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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        commandText += " AND TABLE_SCHEMA || '.' || TABLE_NAME NOT IN (" + args + ")";
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

                        commandText += " AND TABLE_SCHEMA || '.' || TABLE_NAME IN (" + args + ")";
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
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));
                args += $", '{InformationSchema}'";

                commandText += " AND TABLE_SCHEMA NOT IN (" + args + ")";
                commandText += $" AND TABLE_SCHEMA NOT LIKE '{PostgresSchemaPrefix}%'";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND TABLE_SCHEMA IN (" + args + ")";
            }
            else
            {
                commandText += $" AND TABLE_SCHEMA != '{InformationSchema}'";
                commandText += $" AND TABLE_SCHEMA NOT LIKE '{PostgresSchemaPrefix}%'";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            string commandText = @"
select tc.table_schema, tc.table_name, ctu.table_schema, ctu.table_name, rc.constraint_name
from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
inner join INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu ON rc.constraint_name = ctu.constraint_name
inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON rc.constraint_name = tc.constraint_name
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
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Schema}.{table.Name}'"));

                        commandText += " AND tc.TABLE_SCHEMA || '.' || tc.TABLE_NAME NOT IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND tc.TABLE_NAME NOT IN (" + args + ")";
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

                        commandText += " AND tc.TABLE_SCHEMA || '.' || tc.TABLE_NAME IN (" + args + ")";
                    }
                    else
                    {
                        var args = string.Join(",", tableGroup.Tables.Select(table => $"'{table.Name}'"));

                        commandText += " AND tc.TABLE_NAME IN (" + args + ")";
                    }
                }
            }
            if (options.SchemasToExclude.Any())
            {
                var args = string.Join(",", options.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND tc.TABLE_SCHEMA NOT IN (" + args + ")";
            }
            else if (options.SchemasToInclude.Any())
            {
                var args = string.Join(",", options.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND tc.TABLE_SCHEMA IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var builder = new StringBuilder();

            foreach (var table in graph.CyclicalTableRelationships.Select(rel => rel.ParentTable))
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} DISABLE TRIGGER ALL;");
            }
            if (graph.ToDelete.Any())
            {
                var allTables = graph.ToDelete.Select(table => table.GetFullName(QuoteCharacter));
                builder.AppendLine($"truncate table {string.Join(",", allTables)} cascade;");
            }
            foreach (var table in graph.CyclicalTableRelationships.Select(rel => rel.ParentTable))
            {
                builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} ENABLE TRIGGER ALL;");
            }

            return builder.ToString();
        }

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
        {
            // Postgres has two sequence types, the "identity" column *type* and the serial *pseudo-type*.
            // In the case of serial, some behind-the-scenes work is done to create a column of type int or
            // bigint and then place a generated sequence behind it. Since both can be reseeded we can check
            // for both, within the constraints of the tables the user wants to reset.
            //
            // This more complex implementation accommodates use cases where the user may have renamed their
            // sequences in a manner where a regex parse would fail.
            var tableNames = string.Join(", ", tablesToDelete.Select(t => $"'{t.GetFullName(QuoteCharacter)}'"));
            return $@"
CREATE OR REPLACE FUNCTION pg_temp.reset_sequence(seq text) RETURNS void AS $$
DECLARE
BEGIN
	/* ALTER SEQUENCE doesn't work with variables, so we construct a statement
	 * and execute that instead.
	 */
	EXECUTE 'ALTER SEQUENCE ' || seq || ' RESTART;';
END;
$$ LANGUAGE plpgsql;

WITH all_sequences AS (
    SELECT pg_get_serial_sequence(quote_ident(table_schema) || '.' || quote_ident(table_name), column_name) AS sequence_name
    FROM information_schema.columns
    WHERE pg_get_serial_sequence(quote_ident(table_schema) || '.' || quote_ident(table_name), column_name) IS NOT NULL
        AND '""' || table_schema || '"".""' || table_name || '""' IN ({tableNames}))

SELECT pg_temp.reset_sequence(s.sequence_name) FROM all_sequences s;";
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