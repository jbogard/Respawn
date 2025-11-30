using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    internal class SqliteDbAdapter : IDbAdapter
    {
        public string BuildTableCommandText(RespawnerOptions options)
        {
            string commandText = @"
SELECT '' as schema_name, name as table_name
FROM sqlite_master
WHERE type = 'table'
AND name <> 'sqlite_sequence'
AND name <> 'sqlite_stat1'";

            if (options.TablesToIgnore.Any())
            {
                var args = string.Join(",", options.TablesToIgnore.Select(table => $"'{table.Name}'"));
                commandText += " AND name NOT IN (" + args + ")";
            }
            if (options.TablesToInclude.Any())
            {
                var args = string.Join(",", options.TablesToInclude.Select(table => $"'{table.Name}'"));
                commandText += " AND name IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(RespawnerOptions options)
        {
            // SQLite doesn't have a central repository of foreign key constraints like other databases
            // So we need to query pragmas for each table to get the relationships
            // This is implemented in the Respawner.cs class when creating the graphs

            return @"
SELECT 
    '' as parent_schema,
    m.name as parent_table,
    '' as referenced_schema,
    p.`table` as referenced_table,
    p.`from` as constraint_name
FROM 
    sqlite_master m,
    pragma_foreign_key_list(m.name) p
WHERE 
    m.type = 'table'";
        }
        public string BuildDeleteCommandText(GraphBuilder graph, RespawnerOptions options)
        {
            var builder = new StringBuilder();

            builder.AppendLine("PRAGMA foreign_keys = OFF;");

            foreach (var table in graph.ToDelete)
            {
                builder.AppendLine(options.FormatDeleteStatement?.Invoke(table) ?? $"DELETE FROM {table.Name};");
            }

            builder.AppendLine("PRAGMA foreign_keys = ON;"); return builder.ToString();
        }
        public string BuildReseedSql(IEnumerable<Table> tablesToDelete)
        {
            var tables = tablesToDelete as Table[] ?? tablesToDelete.ToArray();
            if (!tables.Any())
                return string.Empty;

            var builder = new StringBuilder();

            // SQLite handles auto-increment with a special table
            foreach (var table in tables)
            {
                builder.AppendLine($"DELETE FROM sqlite_sequence WHERE name = '{table.Name}';");
            }

            return builder.ToString();
        }

        public string BuildTemporalTableCommandText(RespawnerOptions options)
        {
            // SQLite does not support temporal tables
            return string.Empty;
        }

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning)
        {
            // SQLite does not support temporal tables
            return string.Empty;
        }

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning)
        {
            // SQLite does not support temporal tables
            return string.Empty;
        }

        public Task<bool> CheckSupportsTemporalTables(DbConnection connection)
        {
            return Task.FromResult(false);
        }

        public bool RequiresStatementsToBeExecutedIndividually()
        {
            return false;
        }
    }
}
