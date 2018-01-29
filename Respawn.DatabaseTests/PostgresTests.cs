using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    using System;
    using Npgsql;
    using NPoco;
    using Shouldly;

    public class PostgresTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
        private NpgsqlConnection _connection;
        private Database _database;

        public PostgresTests(ITestOutputHelper output) => _output = output;

        public async Task InitializeAsync()
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
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            _connection = new NpgsqlConnection(string.Format(dbConnString, dbName));
            _connection.Open();

            _database = new Database(_connection, DatabaseType.PostgreSQL);
        }

        public Task DisposeAsync()
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;
            return Task.FromResult(0);
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
            _database.Execute("create table foo (Value int)");
            _database.Execute("create table bar (Value int)");

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
        public async Task ShouldHandleRelationships()
        {
            _database.Execute("create table foo (value int, primary key (value))");
            _database.Execute("create table baz (value int, foovalue int, constraint FK_Foo foreign key (foovalue) references foo (value))");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
                _database.Execute("INSERT INTO \"baz\" VALUES (@0, @0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM baz").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "public" }
            };
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM baz").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleCircularRelationships()
        {
            _database.Execute("create table parent (id int primary key, childid int NULL)");
            _database.Execute("create table child (id int primary key, parentid int NULL)");
            _database.Execute("alter table parent add constraint FK_Child foreign key (ChildId) references Child (Id)");
            _database.Execute("alter table child add constraint FK_Parent foreign key (ParentId) references Parent (Id)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"parent\" VALUES (@0, null)", i);
                _database.Execute("INSERT INTO \"child\" VALUES (@0, null)", i);
            }

            _database.Execute("update parent set childid = 0");
            _database.Execute("update child set parentid = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new[] { "public" }
            };
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleSelfRelationships()
        {
            _database.Execute("create table foo (id int primary key, parentid int NULL)");
            _database.Execute("alter table foo add constraint FK_Parent foreign key (parentid) references foo (id)");

            _database.Execute("INSERT INTO \"foo\" VALUES (@0)", 1);
            for (int i = 1; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0, @1)", i+1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new[] { "public" }
            };
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
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
    }
}
