
using System.Collections;

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
            public string Name { get; set; }
        }

        private class Table : IEquatable<Table>, IComparable<Table>, IComparable
        {
            public Table(string name) => Name = name;

            public string Name { get; }

            public HashSet<Table> Relationships { get; } = new HashSet<Table>();

            public bool Equals(Table other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((Table) obj);
            }

            public override int GetHashCode()
            {
                return StringComparer.OrdinalIgnoreCase.GetHashCode(Name);
            }

            public int CompareTo(Table other)
            {
                if (ReferenceEquals(this, other)) return 0;
                if (ReferenceEquals(null, other)) return 1;
                return string.Compare(Name, other.Name, StringComparison.OrdinalIgnoreCase);
            }

            public int CompareTo(object obj)
            {
                if (ReferenceEquals(null, obj)) return 1;
                if (ReferenceEquals(this, obj)) return 0;
                if (!(obj is Table)) throw new ArgumentException($"Object must be of type {nameof(Table)}");
                return CompareTo((Table) obj);
            }

            public static bool operator ==(Table left, Table right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(Table left, Table right)
            {
                return !Equals(left, right);
            }
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

            var tables = BuildTables(allTables, allRelationships);

            var cycles = FindCycles(tables);

            var toDelete = new List<Table>();

            BuildTableList(tables, new HashSet<Table>(), toDelete);

            _tablesToDelete = toDelete.Distinct().Select(t => t.Name).ToArray();

            _deleteSql = DbAdapter.BuildDeleteCommandText(_tablesToDelete);
        }

        private HashSet<Table> BuildTables(IList<string> allTables, IList<Relationship> allRelationships)
        {
            var tables = new HashSet<Table>(allTables.Select(t => new Table(t)));

            foreach (var relationship in allRelationships)
            {
                var pkTable = tables.SingleOrDefault(t => t.Name == relationship.PrimaryKeyTable);
                var fkTable = tables.SingleOrDefault(t => t.Name == relationship.ForeignKeyTable);
                if (pkTable != null && fkTable != null)
                {
                    pkTable.Relationships.Add(fkTable);
                }
            }

            return tables;
        }

        private static HashSet<Table> FindCycles(HashSet<Table> allTables)
        {
            var visiting = new HashSet<Table>();
            var visited = new HashSet<Table>();
            while (allTables.Any())
            {
                var next = allTables.First();
                RemoveCycles(next, allTables, visiting, visited);
                // Todo: clear out the cycles and start again
            }

            return visiting;
        }

        private static bool RemoveCycles(
            Table table,
            HashSet<Table> notVisited,
            HashSet<Table> visiting,
            HashSet<Table> visited)
        {
            notVisited.Remove(table);
            visiting.Add(table);
            foreach (var relationship in table.Relationships)
            {
                if (visited.Contains(relationship))
                    continue;

                if (visiting.Contains(relationship))
                    return true;

                if (RemoveCycles(relationship, notVisited, visiting, visited))
                    return true;
            }

            visiting.Remove(table);
            visited.Add(table);

            return false;
        }

        private static void BuildTableList(HashSet<Table> tables, HashSet<Table> visited, List<Table> toDelete) 
        {
            foreach (var table in tables)
            {
                if (visited.Contains(table))
                    continue;

                BuildTableList(table.Relationships, visited, toDelete);

                toDelete.Add(table);
                visited.Add(table);
            }
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
