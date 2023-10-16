using System.Linq;
using System.Threading.Tasks;
using Respawn.Graph;
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
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI")))
            {
                rootConnString = "Server=127.0.0.1;Port=5432;User ID=postgres;Password=root;database=postgres";
                dbConnString = "Server=127.0.0.1;Port=5432;User ID=postgres;Password=root;database={0}";
            }
            var dbName = DateTime.Now.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString("N");
            await using (var connection = new NpgsqlConnection(rootConnString))
            {
                connection.Open();

                await using (var cmd = connection.CreateCommand())
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

        [SkipOnCI]
        public async Task ShouldDeleteData()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTables()
        {
            await _database.ExecuteAsync("create table foo (Value int)");
            await _database.ExecuteAsync("create table bar (Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                TablesToIgnore = new Table[] { "foo" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTablesIfSchemaSpecified()
        {
            await _database.ExecuteAsync("create schema eggs");
            await _database.ExecuteAsync("create table eggs.foo (Value int)");
            await _database.ExecuteAsync("create table eggs.bar (Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"eggs\".\"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"eggs\".\"bar\" VALUES (@0)", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                TablesToIgnore = new Table[] { new Table("eggs", "foo") }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM eggs.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM eggs.bar").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTables()
        {
            await _database.ExecuteAsync("create table foo (Value int)");
            await _database.ExecuteAsync("create table bar (Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                TablesToInclude = new Table[] { "foo" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(100);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTablesIfSchemaSpecified()
        {
            await _database.ExecuteAsync("create schema eggs");
            await _database.ExecuteAsync("create table eggs.foo (Value int)");
            await _database.ExecuteAsync("create table eggs.bar (Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"eggs\".\"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"eggs\".\"bar\" VALUES (@0)", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                TablesToInclude = new Table[] { new Table("eggs", "foo") }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM eggs.foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM eggs.bar").ShouldBe(100);
        }

        [SkipOnCI]
        public async Task ShouldHandleRelationships()
        {
            await _database.ExecuteAsync("create table foo (value int, primary key (value))");
            await _database.ExecuteAsync("create table baz (value int, foovalue int, constraint FK_Foo foreign key (foovalue) references foo (value))");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"baz\" VALUES (@0, @0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM baz").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "public" }
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM baz").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleCircularRelationships()
        {
            await _database.ExecuteAsync("create table parent (id int primary key, childid int NULL)");
            await _database.ExecuteAsync("create table child (id int primary key, parentid int NULL)");
            await _database.ExecuteAsync("alter table parent add constraint FK_Child foreign key (ChildId) references Child (Id)");
            await _database.ExecuteAsync("alter table child add constraint FK_Parent foreign key (ParentId) references Parent (Id)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"parent\" VALUES (@0, null)", i);
                await _database.ExecuteAsync("INSERT INTO \"child\" VALUES (@0, null)", i);
            }

            await _database.ExecuteAsync("update parent set childid = 0");
            await _database.ExecuteAsync("update child set parentid = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleSelfRelationships()
        {
            await _database.ExecuteAsync("create table foo (id int primary key, parentid int NULL)");
            await _database.ExecuteAsync("alter table foo add constraint FK_Parent foreign key (parentid) references foo (id)");

            await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", 1);
            for (int i = 1; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0, @1)", i+1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleComplexCycles()
        {
            await _database.ExecuteAsync("create table a (id int primary key, b_id int NULL)");
            await _database.ExecuteAsync("create table b (id int primary key, a_id int NULL, c_id int NULL, d_id int NULL)");
            await _database.ExecuteAsync("create table c (id int primary key, d_id int NULL)");
            await _database.ExecuteAsync("create table d (id int primary key)");
            await _database.ExecuteAsync("create table e (id int primary key, a_id int NULL)");
            await _database.ExecuteAsync("create table f (id int primary key, b_id int NULL)");
            await _database.ExecuteAsync("alter table a add constraint FK_a_b foreign key (b_id) references b (id)");
            await _database.ExecuteAsync("alter table b add constraint FK_b_a foreign key (a_id) references a (id)");
            await _database.ExecuteAsync("alter table b add constraint FK_b_c foreign key (c_id) references c (id)");
            await _database.ExecuteAsync("alter table b add constraint FK_b_d foreign key (d_id) references d (id)");
            await _database.ExecuteAsync("alter table c add constraint FK_c_d foreign key (d_id) references d (id)");
            await _database.ExecuteAsync("alter table e add constraint FK_e_a foreign key (a_id) references a (id)");
            await _database.ExecuteAsync("alter table f add constraint FK_f_b foreign key (b_id) references b (id)");


            await _database.ExecuteAsync("insert into d (id) values (1)");
            await _database.ExecuteAsync("insert into c (id, d_id) values (1, 1)");
            await _database.ExecuteAsync("insert into a (id) values (1)");
            await _database.ExecuteAsync("insert into b (id, c_id, d_id) values (1, 1, 1)");
            await _database.ExecuteAsync("insert into e (id, a_id) values (1, 1)");
            await _database.ExecuteAsync("insert into f (id, b_id) values (1, 1)");
            await _database.ExecuteAsync("update a set b_id = 1");
            await _database.ExecuteAsync("update b set a_id = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM c").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM d").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM e").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM f").ShouldBe(1);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM c").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM d").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM e").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM f").ShouldBe(0);
        }


        [SkipOnCI]
        public async Task ShouldExcludeSchemas()
        {
            await _database.ExecuteAsync("create schema a");
            await _database.ExecuteAsync("create schema b");
            await _database.ExecuteAsync("create table a.foo (value int)");
            await _database.ExecuteAsync("create table b.bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO a.foo VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT INTO b.bar VALUES (" + i + ")");
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToExclude = new [] { "a" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.bar").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIncludeSchemas()
        {
            await _database.ExecuteAsync("create schema a");
            await _database.ExecuteAsync("create schema b");
            await _database.ExecuteAsync("create table a.foo (value int)");
            await _database.ExecuteAsync("create table b.bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO a.foo VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT INTO b.bar VALUES (" + i + ")");
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "b" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.bar").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldResetSequencesAndIdentities()
        {
            await _database.ExecuteAsync("CREATE TABLE a (id INT GENERATED ALWAYS AS IDENTITY, value SERIAL)");
            await _database.ExecuteAsync("INSERT INTO a DEFAULT VALUES");
            await _database.ExecuteAsync("INSERT INTO a DEFAULT VALUES");
            await _database.ExecuteAsync("INSERT INTO a DEFAULT VALUES");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                WithReseed = true
            });

            await checkpoint.ResetAsync(_connection);
            _database.ExecuteScalar<int>("SELECT nextval('a_id_seq')").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT nextval('a_value_seq')").ShouldBe(1);
        }
    }
}
