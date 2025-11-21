using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using NPoco;
using Respawn.Graph;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    public class SqliteTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
        private SqliteConnection _connection;
        private Database _database;
        private string _dbFileName;

        public SqliteTests(ITestOutputHelper output) => _output = output;

        public async Task InitializeAsync()
        {
            _dbFileName = Path.Combine(Path.GetTempPath(), $"respawn_test_{Guid.NewGuid():N}.db");
            var connectionString = $"Data Source={_dbFileName};";

            _connection = new SqliteConnection(connectionString);
            await _connection.OpenAsync();

            _database = new Database(_connection);
        }

        public Task DisposeAsync()
        {
            // Close and dispose of the connection before attempting to delete the file
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;

            try
            {
                if (File.Exists(_dbFileName))
                {
                    File.Delete(_dbFileName);
                }
            }
            catch
            {
                // Ignore deletion errors
            }

            return Task.CompletedTask;
        }

        [Fact]
        public async Task ShouldDeleteData()
        {
            await _database.ExecuteAsync("CREATE TABLE foo (value INTEGER)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO foo VALUES (@0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            await _database.ExecuteAsync("CREATE TABLE foo (value INTEGER)");
            await _database.ExecuteAsync("CREATE TABLE bar (value INTEGER)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO foo VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO bar VALUES (@0)", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                TablesToIgnore = new Table[] { "foo" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeTables()
        {
            await _database.ExecuteAsync("CREATE TABLE foo (value INTEGER)");
            await _database.ExecuteAsync("CREATE TABLE bar (value INTEGER)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO foo VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO bar VALUES (@0)", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                TablesToInclude = new Table[] { "foo" }
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(100);
        }

        [Fact]
        public async Task ShouldHandleRelationships()
        {
            await _database.ExecuteAsync("CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, value INTEGER)");
            await _database.ExecuteAsync("CREATE TABLE bar (id INTEGER PRIMARY KEY AUTOINCREMENT, foo_id INTEGER, FOREIGN KEY(foo_id) REFERENCES foo(id))");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO foo (value) VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO bar (foo_id) VALUES (@0)", i + 1);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldReseedIds()
        {
            await _database.ExecuteAsync("CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, value INTEGER)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO foo (value) VALUES (@0)", i);
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });
            await checkpoint.ResetAsync(_connection);

            await _database.ExecuteAsync("INSERT INTO foo (value) VALUES (@0)", 1);
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldHandleSelfRelationships()
        {
            await _database.ExecuteAsync("CREATE TABLE foo (id INTEGER PRIMARY KEY, parentid INTEGER NULL)");
            await _database.ExecuteAsync("CREATE INDEX IX_foo_parentid ON foo(parentid)");
            await _database.ExecuteAsync("CREATE TRIGGER fk_foo_self BEFORE DELETE ON foo FOR EACH ROW BEGIN " +
                                        "DELETE FROM foo WHERE parentid = OLD.id; END");

            await _database.ExecuteAsync("INSERT INTO foo (id) VALUES (@0)", 1);
            for (int i = 1; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO foo VALUES (@0, @1)", i + 1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleCircularRelationships()
        {
            await _database.ExecuteAsync("CREATE TABLE parent (id INTEGER PRIMARY KEY, childid INTEGER NULL)");
            await _database.ExecuteAsync("CREATE TABLE child (id INTEGER PRIMARY KEY, parentid INTEGER NULL)");

            // In SQLite, we need to create triggers to enforce the foreign key relationships
            await _database.ExecuteAsync("CREATE TRIGGER fk_parent_child BEFORE DELETE ON parent FOR EACH ROW BEGIN " +
                                        "UPDATE child SET parentid = NULL WHERE parentid = OLD.id; END");
            await _database.ExecuteAsync("CREATE TRIGGER fk_child_parent BEFORE DELETE ON child FOR EACH ROW BEGIN " +
                                        "UPDATE parent SET childid = NULL WHERE childid = OLD.id; END");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO parent VALUES (@0, null)", i);
                await _database.ExecuteAsync("INSERT INTO child VALUES (@0, null)", i);
            }

            await _database.ExecuteAsync("UPDATE parent SET childid = 0");
            await _database.ExecuteAsync("UPDATE child SET parentid = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleComplexCycles()
        {
            await _database.ExecuteAsync("PRAGMA foreign_keys = ON");

            await _database.ExecuteAsync("CREATE TABLE a (id INTEGER PRIMARY KEY, b_id INTEGER NULL)");
            await _database.ExecuteAsync("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER NULL, c_id INTEGER NULL, d_id INTEGER NULL)");
            await _database.ExecuteAsync("CREATE TABLE c (id INTEGER PRIMARY KEY, d_id INTEGER NULL)");
            await _database.ExecuteAsync("CREATE TABLE d (id INTEGER PRIMARY KEY)");
            await _database.ExecuteAsync("CREATE TABLE e (id INTEGER PRIMARY KEY, a_id INTEGER NULL)");
            await _database.ExecuteAsync("CREATE TABLE f (id INTEGER PRIMARY KEY, b_id INTEGER NULL)");

            // Create the foreign key constraints
            await _database.ExecuteAsync("CREATE INDEX IX_a_b_id ON a(b_id)");
            await _database.ExecuteAsync("CREATE INDEX IX_b_a_id ON b(a_id)");
            await _database.ExecuteAsync("CREATE INDEX IX_b_c_id ON b(c_id)");
            await _database.ExecuteAsync("CREATE INDEX IX_b_d_id ON b(d_id)");
            await _database.ExecuteAsync("CREATE INDEX IX_c_d_id ON c(d_id)");
            await _database.ExecuteAsync("CREATE INDEX IX_e_a_id ON e(a_id)");
            await _database.ExecuteAsync("CREATE INDEX IX_f_b_id ON f(b_id)");

            await _database.ExecuteAsync("INSERT INTO d (id) VALUES (1)");
            await _database.ExecuteAsync("INSERT INTO c (id, d_id) VALUES (1, 1)");
            await _database.ExecuteAsync("INSERT INTO a (id) VALUES (1)");
            await _database.ExecuteAsync("INSERT INTO b (id, c_id, d_id) VALUES (1, 1, 1)");
            await _database.ExecuteAsync("INSERT INTO e (id, a_id) VALUES (1, 1)");
            await _database.ExecuteAsync("INSERT INTO f (id, b_id) VALUES (1, 1)");
            await _database.ExecuteAsync("UPDATE a SET b_id = 1");
            await _database.ExecuteAsync("UPDATE b SET a_id = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM c").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM d").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM e").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM f").ShouldBe(1);

            var checkpoint = await Respawner.CreateAsync(_connection);
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM c").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM d").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM e").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM f").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldDeleteDataWithRelationships()
        {
            await _database.ExecuteAsync("PRAGMA foreign_keys = ON");

            await _database.ExecuteAsync("CREATE TABLE bob (bobvalue INTEGER PRIMARY KEY)");
            await _database.ExecuteAsync("CREATE TABLE foo (foovalue INTEGER PRIMARY KEY, bobvalue INTEGER NOT NULL, " +
                                        "FOREIGN KEY(bobvalue) REFERENCES bob(bobvalue))");
            await _database.ExecuteAsync("CREATE TABLE bar (barvalue INTEGER PRIMARY KEY, " +
                                        "FOREIGN KEY(barvalue) REFERENCES foo(foovalue))");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO bob VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO foo VALUES (@0, @0)", i);
                await _database.ExecuteAsync("INSERT INTO bar VALUES (@0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bob").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bob").ShouldBe(0);
        }
    }
}
