using System.Collections.Generic;
using System.Linq;
using Respawn.Graph;

namespace Respawn
{
    internal class OracleDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        public string BuildTableCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
select OWNER, TABLE_NAME
from ALL_TABLES
where 1=1 "
                ;

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(table => $"'{table}'").ToArray());

                commandText += " AND TABLE_NAME NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(table => $"'{table}'").ToArray());

                commandText += " AND TABLE_NAME IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(schema => $"'{schema}'").ToArray());

                commandText += " AND OWNER NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(schema => $"'{schema}'").ToArray());

                commandText += " AND OWNER IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(Checkpoint checkpoint)
        {
            string commandText = @"
select a.owner as table_schema,a.table_name, b.owner as table_schema ,b.table_name, a.constraint_name
from all_CONSTRAINTS     a
         inner join all_CONSTRAINTS b on a.r_constraint_name=b.constraint_name 
         where a.constraint_type in ('P','R')";

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(s => $"'{s}'").ToArray());

                commandText += " AND a.TABLE_NAME NOT IN (" + args + ")";
            }
            if (checkpoint.TablesToInclude.Any())
            {
                var args = string.Join(",", checkpoint.TablesToInclude.Select(s => $"'{s}'").ToArray());

                commandText += " AND a.TABLE_NAME IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(s => $"'{s}'").ToArray());

                commandText += " AND a.OWNER NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(s => $"'{s}'").ToArray());

                commandText += " AND a.OWNER IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildDeleteCommandText(GraphBuilder graph)
        {
            var deleteSql = string.Join("\n", BuildCommands(graph));
            return $"BEGIN\n{deleteSql}\nEND;";
        }

        private IEnumerable<string> BuildCommands(GraphBuilder graph)
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

        public string BuildTemporalTableCommandText(Checkpoint checkpoint) => throw new System.NotImplementedException();

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();

        public bool SupportsTemporalTables => false;
    }
}