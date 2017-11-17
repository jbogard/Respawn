
namespace Respawn
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;

    public class Checkpoint
    {
        private string[] _tablesToDelete;
        private string _deleteSql;

        public string[] TablesToIgnore { get; set; } = new string[0];
        public string[] SchemasToInclude { get; set; } = new string[0];
        public string[] SchemasToExclude { get; set; } = new string[0];
        public IDbAdapter DbAdapter { get; set; } = Respawn.DbAdapter.SqlServer;

        public int? CommandTimeout { get; set; }

        private class Relationship
        {
            public string PrimaryKeyTable { get; set; }
            public string ForeignKeyTable { get; set; }

            public bool IsSelfReferencing => PrimaryKeyTable == ForeignKeyTable;

        }

        public virtual async Task Reset(string nameOrConnectionString)
        {
            using (var connection = new SqlConnection(nameOrConnectionString))
            {
                await connection.OpenAsync();

                await Reset(connection);
            }
        }

        public virtual async Task Reset(DbConnection connection)
        {
            if (string.IsNullOrWhiteSpace(_deleteSql))
            {
                await BuildDeleteTables(connection);
            }

            await ExecuteDeleteSqlAsync(connection);
        }

        private async Task ExecuteDeleteSqlAsync(DbConnection connection)
        {
            using (var tx = connection.BeginTransaction())
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;
                cmd.CommandText = _deleteSql;
                cmd.Transaction = tx;

                await cmd.ExecuteNonQueryAsync();

                tx.Commit();
            }
        }

        private async Task BuildDeleteTables(DbConnection connection)
        {
            var allTables = await GetAllTables(connection);

            var allRelationships = await GetRelationships(connection);

            _tablesToDelete = BuildTableList(allTables, allRelationships);

            _deleteSql = DbAdapter.BuildDeleteCommandText(_tablesToDelete);
        }

        private static string[] BuildTableList(ICollection<string> allTables, IList<Relationship> allRelationships,
            List<string> tablesToDelete = null)
        {
            if (tablesToDelete == null)
            {
                tablesToDelete = new List<string>();
            }

            var referencedTables = allRelationships
                .Where(rel => !rel.IsSelfReferencing)
                .Select(rel => rel.PrimaryKeyTable)
                .Distinct()
                .ToList();

            var leafTables = allTables.Except(referencedTables).ToList();

            if (referencedTables.Count > 0 && leafTables.Count == 0)
            {
                string message = string.Join(",", referencedTables);
                message = string.Join(Environment.NewLine, $@"There is a dependency involving the DB tables ({message}) and we can't safely build the list of tables to delete.",
                    "Check for circular references.",
                    "If you have TablesToIgnore you also need to ignore the tables to which these have primary key relationships.");
                throw new InvalidOperationException(message);
            }

            tablesToDelete.AddRange(leafTables);

            if (referencedTables.Any())
            {
                var relationships = allRelationships.Where(x => !leafTables.Contains(x.ForeignKeyTable)).ToArray();
                var tables = allTables.Except(leafTables).ToArray();
                BuildTableList(tables, relationships, tablesToDelete);
            }

            return tablesToDelete.ToArray();
        }

        private async Task<IList<Relationship>> GetRelationships(DbConnection connection)
        {
            var rels = new List<Relationship>();
            var commandText = DbAdapter.BuildRelationshipCommandText(this);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var rel = new Relationship
                        {
                            PrimaryKeyTable = CorrectSqlQuotes("\"" + reader.GetString(0) + "\".\"" + reader.GetString(1) + "\""),
                            ForeignKeyTable = CorrectSqlQuotes("\"" + reader.GetString(2) + "\".\"" + reader.GetString(3) + "\"")
                        };
                        rels.Add(rel);
                    }
                }
            }

            return rels;
        }

        private async Task<IList<string>> GetAllTables(DbConnection connection)
        {
            var tables = new List<string>();

            string commandText = DbAdapter.BuildTableCommandText(this);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        if (!await reader.IsDBNullAsync(0))
                        {
                            tables.Add(CorrectSqlQuotes("\"" + reader.GetString(0) + "\".\"" + reader.GetString(1) + "\""));
                        }
                        else
                        {
                            tables.Add(CorrectSqlQuotes("\"" + reader.GetString(1) + "\""));
                        }
                    }
                }
            }

            return tables.ToList();
        }

        private string CorrectSqlQuotes(string input)
        {
            return input.Replace('"', DbAdapter.QuoteCharacter);
        }
    }
}
