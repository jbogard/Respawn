
using System.Collections;
using Respawn.Graph;

namespace Respawn
{
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

            var graphBuilder = new GraphBuilder(allTables, allRelationships);



            _tablesToDelete = graphBuilder.ToDelete.ToArray();

            _deleteSql = DbAdapter.BuildDeleteCommandText(_tablesToDelete);
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
                            PrimaryKeyTable = $"{DbAdapter.QuoteCharacter}{reader.GetString(0)}{DbAdapter.QuoteCharacter}.{DbAdapter.QuoteCharacter}{reader.GetString(1)}{DbAdapter.QuoteCharacter}",
                            ForeignKeyTable = $"{DbAdapter.QuoteCharacter}{reader.GetString(2)}{DbAdapter.QuoteCharacter}.{DbAdapter.QuoteCharacter}{reader.GetString(3)}{DbAdapter.QuoteCharacter}",
                            Name = $"{DbAdapter.QuoteCharacter}{reader.GetString(3)}{DbAdapter.QuoteCharacter}"
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
                            tables.Add($"{DbAdapter.QuoteCharacter}{reader.GetString(0)}{DbAdapter.QuoteCharacter}.{DbAdapter.QuoteCharacter}{reader.GetString(1)}{DbAdapter.QuoteCharacter}");
                        }
                        else
                        {
                            tables.Add($"{DbAdapter.QuoteCharacter}{reader.GetString(1)}{DbAdapter.QuoteCharacter}");
                        }
                    }
                }
            }

            return tables.ToList();
        }
    }
}
