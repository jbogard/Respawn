using System.Collections.Generic;
using System.Linq;
using System.Text;
using Respawn.Graph;

namespace Respawn
{
    internal class MySqlAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '`';

        public string BuildTableCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
SELECT t.TABLE_SCHEMA, t.TABLE_NAME
FROM
    information_schema.tables AS t
WHERE
    table_type = 'BASE TABLE'
    AND TABLE_SCHEMA NOT IN ('mysql' , 'performance_schema')";

            if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));

                commandText += " AND t.TABLE_NAME NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude != null && checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(t => $"'{t}'"));

                commandText += " AND t.TABLE_NAME IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND t.TABLE_SCHEMA NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND t.TABLE_SCHEMA IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(Checkpoint checkpoint)
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

            if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));
                whereText.Add("TABLE_NAME NOT IN (" + args + ")");
            }
            if (checkpoint.TablesToInclude != null && checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(t => $"'{t}'"));
                whereText.Add("TABLE_NAME IN (" + args + ")");
            }
            if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));
                whereText.Add("CONSTRAINT_SCHEMA NOT IN (" + args + ")");
            }
            else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));
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

        public string BuildReseedSql(IEnumerable<Table> tablesToDelete) => throw new System.NotImplementedException();

        public string BuildTemporalTableCommandText(Checkpoint checkpoint) => throw new System.NotImplementedException();

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();

        public bool SupportsTemporalTables => false;
    }
}