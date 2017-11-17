namespace Respawn
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public interface IDbAdapter
    {
        char QuoteCharacter { get; }
        string BuildTableCommandText(Checkpoint checkpoint);
        string BuildRelationshipCommandText(Checkpoint checkpoint);
        string BuildDeleteCommandText(IEnumerable<string> tablesToDelete);
    }

    public static class DbAdapter
    {
        public static readonly IDbAdapter SqlServer = new SqlServerDbAdapter();
        public static readonly IDbAdapter Postgres = new PostgresDbAdapter();
        public static readonly IDbAdapter SqlServerCe = new SqlServerCeDbAdapter();
        public static readonly IDbAdapter MySql = new MySqlAdapter();

        private class SqlServerDbAdapter : IDbAdapter
        {
            public char QuoteCharacter => '"';

            public string BuildTableCommandText(Checkpoint checkpoint)
            {
                string commandText = @"
select s.name, t.name
from sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.principal_id = '1'";

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
   fk_schema.name, so_fk.name
from
sysforeignkeys sfk
	inner join sys.objects so_pk on sfk.rkeyid = so_pk.object_id
	inner join sys.schemas pk_schema on so_pk.schema_id = pk_schema.schema_id
	inner join sys.objects so_fk on sfk.fkeyid = so_fk.object_id			
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

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append($"delete from {tableName};\r\n");
                }
                return builder.ToString();
            }
        }

        private class PostgresDbAdapter : IDbAdapter
        {
            public char QuoteCharacter => '"';

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
select ctu.table_schema, ctu.table_name, tc.table_schema, tc.table_name
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

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append($"truncate table {tableName} cascade;\r\n");
                }
                return builder.ToString();
            }
        }

        private class SqlServerCeDbAdapter : IDbAdapter
        {
            public char QuoteCharacter => '"';

            public string BuildTableCommandText(Checkpoint checkpoint)
            {
                string commandText = @"SELECT table_schema, table_name FROM information_schema.tables AS t WHERE TABLE_TYPE <> N'SYSTEM TABLE'";

                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                    commandText += " AND t.table_name NOT IN (" + args + ")";
                }
                if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select(t => $"N'{t}'"));

                    commandText += " AND s.table_name NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select(t => $"N'{t}'"));

                    commandText += " AND s.table_name IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildRelationshipCommandText(Checkpoint checkpoint)
            {
                string commandText = @"SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE 1=1";

                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select(t => $"N'{t}'"));

                    commandText += " AND CONSTRAINT_NAME NOT IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append($"delete from {tableName};\r\n");
                }
                return builder.ToString();
            }
        }

        private class MySqlAdapter : IDbAdapter
        {
            public char QuoteCharacter => '`';

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
SELECT CONSTRAINT_SCHEMA, 
    TABLE_NAME, 
    UNIQUE_CONSTRAINT_SCHEMA, 
    REFERENCED_TABLE_NAME 
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

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append($"DELETE FROM {tableName};{System.Environment.NewLine}");
                }
                return builder.ToString();
            }
        }
    }
}
