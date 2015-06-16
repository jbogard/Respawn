namespace Respawn
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public interface IDbAdapter
    {
        string BuildTableCommandText(Checkpoint checkpoint);
        string BuildRelationshipCommandText(Checkpoint checkpoint);
        string BuildDeleteCommandText(IEnumerable<string> tablesToDelete);
    }

    public static class DbAdapter
    {
        public static readonly IDbAdapter SqlServer = new SqlServerDbAdapter();
        public static readonly IDbAdapter Postgres = new PostgresDbAdapter();
        public static readonly IDbAdapter SqlServerCe = new SqlServerCeDbAdapter();

        private class SqlServerDbAdapter : IDbAdapter
        {
            public string BuildTableCommandText(Checkpoint checkpoint)
            {
                string commandText = @"
select s.name, t.name
from sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.principal_id = '1'";

                int position = 0;
                if (checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select((s, i) => "{" + i.ToString() + "}").ToArray());

                    commandText += " AND t.name NOT IN (" + args + ")";
                    position += checkpoint.TablesToIgnore.Length;
                }
                if (checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND s.name NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

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

                int position = 0;
                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select((s, i) => "{" + i.ToString() + "}").ToArray());

                    commandText += " AND so_pk.name NOT IN (" + args + ")";
                    position += checkpoint.TablesToIgnore.Length;
                }
                if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND pk_schema.name NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND pk_schema.name IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append(string.Format("delete from {0};\r\n", tableName));
                }
                return builder.ToString();
            }
        }

        private class PostgresDbAdapter : IDbAdapter
        {
            public string BuildTableCommandText(Checkpoint checkpoint)
            {
                string commandText = @"
select TABLE_SCHEMA, TABLE_NAME
from INFORMATION_SCHEMA.TABLES
where TABLE_TYPE = 'BASE TABLE'"
        ;

                int position = 0;
                if (checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select((s, i) => "{" + i.ToString() + "}").ToArray());

                    commandText += " AND TABLE_NAME NOT IN (" + args + ")";
                    position += checkpoint.TablesToIgnore.Length;
                }
                if (checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND TABLE_SCHEMA NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

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

                int position = 0;
                if (checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select((s, i) => "{" + i.ToString() + "}").ToArray());

                    commandText += " AND tc.TABLE_NAME NOT IN (" + args + ")";
                    position += checkpoint.TablesToIgnore.Length;
                }
                if (checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND tc.TABLE_SCHEMA NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND tc.TABLE_SCHEMA IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append(string.Format("truncate table {0} cascade;\r\n", tableName));
                }
                return builder.ToString();
            }
        }

        private class SqlServerCeDbAdapter : IDbAdapter
        {
            public string BuildTableCommandText(Checkpoint checkpoint)
            {
                string commandText = @"SELECT table_schema, table_name FROM information_schema.tables AS t WHERE TABLE_TYPE <> N'SYSTEM TABLE'";

                int position = 0;
                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select((s, i) => "{" + i.ToString() + "}").ToArray());

                    commandText += " AND t.table_name NOT IN (" + args + ")";
                    position += checkpoint.TablesToIgnore.Length;
                }
                if (checkpoint.SchemasToExclude != null && checkpoint.SchemasToExclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToExclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND s.table_name NOT IN (" + args + ")";
                }
                else if (checkpoint.SchemasToInclude != null && checkpoint.SchemasToInclude.Any())
                {
                    var args = string.Join(",", checkpoint.SchemasToInclude.Select((s, i) => "{" + (i + position).ToString() + "}").ToArray());

                    commandText += " AND s.table_name IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildRelationshipCommandText(Checkpoint checkpoint)
            {
                string commandText = @"SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE 1=1";

                if (checkpoint.TablesToIgnore != null && checkpoint.TablesToIgnore.Any())
                {
                    var args = string.Join(",", checkpoint.TablesToIgnore.Select((s, i) => "{" + i.ToString() + "}").ToArray());

                    commandText += " AND CONSTRAINT_NAME NOT IN (" + args + ")";
                }

                return commandText;
            }

            public string BuildDeleteCommandText(IEnumerable<string> tablesToDelete)
            {
                var builder = new StringBuilder();

                foreach (var tableName in tablesToDelete)
                {
                    builder.Append(string.Format("delete from {0};\r\n", tableName));
                }
                return builder.ToString();
            }
        }
    }
}