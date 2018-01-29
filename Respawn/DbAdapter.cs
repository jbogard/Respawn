using Respawn.Graph;

namespace Respawn
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public interface IDbAdapter
    {
        string BuildTableCommandText(Checkpoint checkpoint);
        string BuildRelationshipCommandText(Checkpoint checkpoint);
        string BuildDeleteCommandText(GraphBuilder builder);
    }

    public static class DbAdapter
    {
        public static readonly IDbAdapter SqlServer = new SqlServerDbAdapter();
        public static readonly IDbAdapter Postgres = new PostgresDbAdapter();
        public static readonly IDbAdapter MySql = new MySqlAdapter();
        public static readonly IDbAdapter Oracle = new OracleDbAdapter();

        private class SqlServerDbAdapter : IDbAdapter
        {
            private const char QuoteCharacter = '"';

            public string BuildTableCommandText(Checkpoint checkpoint)
            {
                string commandText = @"
select s.name, t.name
from sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE 1=1";

                if (checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                    commandText += " AND t.name NOT IN (" + args + ")";
                }
                if (checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"N'{t}'"));

                    commandText += " AND s.name NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"N'{t}'"));

                    commandText += " AND s.name IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildRelationshipCommandText(Checkpoint checkpoint)
            {
                string commandText = @"
select
   pk_schema.name, so_pk.name,
   fk_schema.name, so_fk.name,
   sfk.name
from
sys.foreign_keys sfk
	inner join sys.objects so_pk on sfk.referenced_object_id = so_pk.object_id
	inner join sys.schemas pk_schema on so_pk.schema_id = pk_schema.schema_id
	inner join sys.objects so_fk on sfk.parent_object_id = so_fk.object_id			
	inner join sys.schemas fk_schema on so_fk.schema_id = fk_schema.schema_id
where 1=1";

                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                    commandText += " AND so_pk.name NOT IN (" + args + ")";
                }
                if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"N'{t}'"));

                    commandText += " AND pk_schema.name NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"N'{t}'"));

                    commandText += " AND pk_schema.name IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildDeleteCommandText(GraphBuilder graph)
            {
                var builder = new StringBuilder();

                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} NOCHECK CONSTRAINT ALL;");
                }
                foreach (var table in graph.CyclicalTableForeignKeyTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} NOCHECK CONSTRAINT ALL;");
                }
                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"DELETE {table.GetFullName(QuoteCharacter)};");
                }
                foreach (var table in graph.ToDelete)
                {
                    builder.AppendLine($"DELETE {table.GetFullName(QuoteCharacter)};");
                }
                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} WITH CHECK CHECK CONSTRAINT ALL;");
                }
                foreach (var table in graph.CyclicalTableForeignKeyTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} WITH CHECK CHECK CONSTRAINT ALL;");
                }

                return builder.ToString();
            }
        }

        private class PostgresDbAdapter : IDbAdapter
        {
            private const char QuoteCharacter = '"';

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
                if (checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"'{t}'"));

                    commandText += " AND TABLE_SCHEMA NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"'{t}'"));

                    commandText += " AND TABLE_SCHEMA IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildRelationshipCommandText(Checkpoint checkpoint)
            {
                string commandText = @"
select ctu.table_schema, ctu.table_name, tc.table_schema, tc.table_name, rc.constraint_name
from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
inner join INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu ON rc.constraint_name = ctu.constraint_name
inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON rc.constraint_name = tc.constraint_name
where 1=1";

                if (checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));

                    commandText += " AND tc.TABLE_NAME NOT IN (" + args + ")";
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

                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} DISABLE TRIGGER ALL;");
                }
                foreach (var table in graph.CyclicalTableForeignKeyTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} DISABLE TRIGGER ALL;");
                }
                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"truncate table {table.GetFullName(QuoteCharacter)} cascade;");
                }
                foreach (var table in graph.ToDelete)
                {
                    builder.AppendLine($"truncate table {table.GetFullName(QuoteCharacter)} cascade;");
                }
                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} ENABLE TRIGGER ALL;");
                }
                foreach (var table in graph.CyclicalTableForeignKeyTables)
                {
                    builder.AppendLine($"ALTER TABLE {table.GetFullName(QuoteCharacter)} ENABLE TRIGGER ALL;");
                }

                return builder.ToString();
            }
        }

        private class MySqlAdapter : IDbAdapter
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
SELECT UNIQUE_CONSTRAINT_SCHEMA, 
    REFERENCED_TABLE_NAME, 
    CONSTRAINT_SCHEMA, 
    TABLE_NAME,
    CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS";

                var whereText = new List<string>();

                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"'{t}'"));
                    whereText.Add("TABLE_NAME NOT IN (" + args + ")");
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
                foreach (var table in graph.CyclicalTables)
                {
                    builder.AppendLine($"DELETE FROM {table.GetFullName(QuoteCharacter)};");
                }
                foreach (var table in graph.ToDelete)
                {
                    builder.AppendLine($"DELETE FROM {table.GetFullName(QuoteCharacter)};");
                }
                builder.AppendLine("SET FOREIGN_KEY_CHECKS=1;");

                return builder.ToString();
            }
        }

        private class OracleDbAdapter : IDbAdapter
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
select b.owner as table_schema ,b.table_name, a.owner as table_schema,a.table_name, a.constraint_name
from all_CONSTRAINTS     a
         inner join all_CONSTRAINTS b on a.r_constraint_name=b.constraint_name 
         where a.constraint_type in ('P','R')";

                if (checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(s => $"'{s}'").ToArray());

                    commandText += " AND a.TABLE_NAME NOT IN (" + args + ")";
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
                    yield return $"EXECUTE IMMEDIATE 'ALTER TABLE {rel.ForeignKeyTable.GetFullName(QuoteCharacter)} DISABLE CONSTRAINT {QuoteCharacter}{rel.Name}{QuoteCharacter}';";
                }
                foreach (var table in graph.CyclicalTables)
                {
                    yield return $"EXECUTE IMMEDIATE 'delete from {table.GetFullName(QuoteCharacter)}';";
                }
                foreach (var table in graph.ToDelete)
                {
                    yield return $"EXECUTE IMMEDIATE 'delete from {table.GetFullName(QuoteCharacter)}';";
                }
                foreach (var rel in graph.CyclicalTableRelationships)
                {
                    yield return $"EXECUTE IMMEDIATE 'ALTER TABLE {rel.ForeignKeyTable.GetFullName(QuoteCharacter)} ENABLE CONSTRAINT {QuoteCharacter}{rel.Name}{QuoteCharacter}';";
                }
            }
        }
    }
}
