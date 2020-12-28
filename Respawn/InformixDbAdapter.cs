using System.Collections.Generic;
using System.Linq;
using System.Text;
using Respawn.Graph;

namespace Respawn
{
    internal class InformixDbAdapter : IDbAdapter
    {
        private const char QuoteCharacter = '"';

        public string BuildTableCommandText(Checkpoint checkpoint)
        {
            string commandText = @"SELECT t.owner, t.tabname
                                       FROM 'informix'.systables t
                                       WHERE t.tabtype = 'T'
  	                                    AND t.tabid >= 100";

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));

                commandText += " AND t.tabname NOT IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND t.owner NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));

                commandText += " AND t.owner IN (" + args + ")";
            }

            return commandText;
        }

        public string BuildRelationshipCommandText(Checkpoint checkpoint)
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

            if (checkpoint.TablesToIgnore.Any())
            {
                var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));

                commandText += " AND T2.tabname NOT IN (" + args + ")";
            }
            if (checkpoint.SchemasToExclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));

                commandText += " AND T2.owner NOT IN (" + args + ")";
            }
            else if (checkpoint.SchemasToInclude.Any())
            {
                var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));

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

        public string BuildTemporalTableCommandText(Checkpoint checkpoint) => throw new System.NotImplementedException();

        public string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning) => throw new System.NotImplementedException();

        public string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning) => throw new System.NotImplementedException();

        public bool SupportsTemporalTables => false;
    }
}