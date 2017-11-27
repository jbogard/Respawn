using System.Threading.Tasks;
using Xunit;

namespace Respawn.Tests
{
    using System;
    using Npgsql;
    using NPoco;
    using Shouldly;

    public class PostgresTests : IDisposable
    {
        private NpgsqlConnection _connection;
        private Database _database;

        public class foo
        {
            public int value { get; set; }
        }
        public class bar
        {
            public int value { get; set; }
        }

        public PostgresTests()
        {
            var rootConnString = "Server=127.0.0.1;Port=8081;User ID=docker;Password=Password12!;database=postgres";
            var dbConnString = "Server=127.0.0.1;Port=8081;User ID=docker;Password=Password12!;database={0}";
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APPVEYOR")))
            {
                rootConnString = "Server=127.0.0.1;Port=5432;User ID=postgres;Password=Password12!;database=postgres";
                dbConnString = "Server=127.0.0.1;Port=5432;User ID=postgres;Password=Password12!;database={0}";
            }
            var dbName = DateTime.Now.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString("N");
            using (var connection = new NpgsqlConnection(rootConnString))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "create database \"" + dbName + "\"";
                    cmd.ExecuteNonQuery();
                }
            }
            _connection = new NpgsqlConnection(string.Format(dbConnString, dbName));
            _connection.Open();

            _database = new Database(_connection, DatabaseType.PostgreSQL);
        }

        [Fact]
        public async Task ShouldDeleteData()
        {
            _database.Execute("create table \"foo\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "public" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("create table foo (value int)");
            _database.Execute("create table bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
                _database.Execute("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new[] { "public" },
                TablesToIgnore = new[] { "foo" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldExcludeSchemas()
        {
            _database.Execute("create schema a");
            _database.Execute("create schema b");
            _database.Execute("create table a.foo (value int)");
            _database.Execute("create table b.bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO a.foo VALUES (" + i + ")");
                _database.Execute("INSERT INTO b.bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToExclude = new [] { "a", "pg_catalog" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeSchemas()
        {
            _database.Execute("create schema a");
            _database.Execute("create schema b");
            _database.Execute("create table a.foo (value int)");
            _database.Execute("create table b.bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO a.foo VALUES (" + i + ")");
                _database.Execute("INSERT INTO b.bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "b" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.bar").ShouldBe(0);
        }



        public void Dispose()
        {
            _database.Dispose();
            _database = null;

            _connection.Close();
            _connection.Dispose();
            _connection = null;
        }
    }
}
