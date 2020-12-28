using System.Collections.Generic;
using System.Linq;
using System.Text;
using Respawn.Graph;

namespace Respawn
{
    internal class PostgresDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        // Postgres has some schemas containing internal schemas that should not be deleted.
        private const string InformationSchema = "information_schema";
        private const string PostgresSchemaPrefix = "pg_";

        public string BuildTableCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
select TABLE_SCHEMA, TABLE_NAME
from INFORMATION_SCHEMA.TABLES
where TABLE_TYPE = 'BASE TABLE'"
                ;

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));

                commandText += " AND TABLE_NAME NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(t => $"'{t}'"));

                commandText += " AND TABLE_NAME IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));
                args += $", '{InformationSchema}'";

                commandText += " AND TABLE_SCHEMA NOT IN (" + args + ")";
                commandText += $" AND TABLE_SCHEMA NOT LIKE '{PostgresSchemaPrefix}%'";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND TABLE_SCHEMA IN (" + args + ")";
            }
            else
            {
                commandText += $" AND TABLE_SCHEMA != '{InformationSchema}'";
                commandText += $" AND TABLE_SCHEMA NOT LIKE '{PostgresSchemaPrefix}%'";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
select tc.table_schema, tc.table_name, ctu.table_schema, ctu.table_name, rc.constraint_name
from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
inner join INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu ON rc.constraint_name = ctu.constraint_name
inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON rc.constraint_name = tc.constraint_name
where 1=1";

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));

                commandText += " AND tc.TABLE_NAME NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(t => $"'{t}'"));

                commandText += " AND tc.TABLE_NAME IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND tc.TABLE_SCHEMA NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));

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

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete) => throw new System.NotImplementedException();

        public string BuildTemporalTableCommandText(Checkpoint checkpoint) => throw new System.NotImplementedException();

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();

        public bool SupportsTemporalTables => false;
    }
}