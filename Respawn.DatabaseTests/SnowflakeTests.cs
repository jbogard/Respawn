using Respawn.Graph;
using Shouldly;
using Snowflake.Data.Client;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    public class SnowflakeTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
        private SnowflakeDbConnection _connection;

        public SnowflakeTests(ITestOutputHelper output) => _output = output;

        public Task DisposeAsync()
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;
            return Task.FromResult(0);
        }

        public async Task InitializeAsync()
        {
            var rootconnString = "account=YOUR_ACCOUNT;user=YOUR_USER;password=YOUR_PASSWORD;";

            await using (var connection = new SnowflakeDbConnection(rootconnString))
            {
                await connection.OpenAsync();

                var dropDbCommand = connection.CreateCommand();
                dropDbCommand.CommandText = "DROP DATABASE IF EXISTS MYTESTDB";
                await dropDbCommand.ExecuteNonQueryAsync();

                var createDbCommand = connection.CreateCommand();
                createDbCommand.CommandText = "CREATE DATABASE MYTESTDB";
                await createDbCommand.ExecuteNonQueryAsync();

                var createSchemaCommand = connection.CreateCommand();
                createSchemaCommand.CommandText = "CREATE SCHEMA MYTESTSCHEMA";
                await createSchemaCommand.ExecuteNonQueryAsync();
            }

            _connection = new SnowflakeDbConnection(rootconnString + "db=MYTESTDB;schema=MYTESTSCHEMA;");
            await _connection.OpenAsync();
        }

        private async Task ExecuteNonQueryAsync(string commandText)
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = commandText;
                await command.ExecuteNonQueryAsync();
            }
        }

        [SkipOnCI]
        public async Task ShouldDeleteData()
        {
            await ExecuteNonQueryAsync("CREATE OR REPLACE TABLE foo (value INT)");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("foo", i);
            }

            long countBeforeReset = await CountRowsAsync("foo");
            countBeforeReset.ShouldBe(10L);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake
            });

            await checkpoint.ResetAsync(_connection);

            long countAfterReset = await CountRowsAsync("foo");
            countAfterReset.ShouldBe(0L);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTables()
        {
            await CreateTableAsync("Foo");
            await CreateTableAsync("Bar");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("Foo", i);
                await InsertDataAsync("Bar", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake,
                TablesToIgnore = new Table[] { "Foo" },
                SchemasToInclude = new[] { "MYTESTSCHEMA" }
            });

            await checkpoint.ResetAsync(_connection);

            long countFoo = await CountRowsAsync("Foo");
            countFoo.ShouldBe(10);

            long countBar = await CountRowsAsync("Bar");
            countBar.ShouldBe(0L);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTablesIfSchemaSpecified()
        {
            await ExecuteNonQueryAsync("DROP SCHEMA IF EXISTS eggs");
            await ExecuteNonQueryAsync("CREATE SCHEMA eggs");

            await ExecuteNonQueryAsync("CREATE TABLE eggs.foo (Value INT)");
            await ExecuteNonQueryAsync("CREATE TABLE eggs.bar (Value INT)");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("eggs.foo", i);
                await InsertDataAsync("eggs.bar", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake,
                TablesToIgnore = new Table[] { new Table("eggs", "foo") }
            });

            await checkpoint.ResetAsync(_connection);

            long countFoo = await CountRowsAsync("eggs.foo");
            countFoo.ShouldBe(10L);

            long countBar = await CountRowsAsync("eggs.bar");
            countBar.ShouldBe(0L);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTables()
        {
            await CreateTableAsync("Foo");
            await CreateTableAsync("Bar");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("foo", i);
                await InsertDataAsync("bar", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake,
                TablesToInclude = new Table[] { "foo" }
            });
            await checkpoint.ResetAsync(_connection);

            long countFoo = await CountRowsAsync("Foo");
            countFoo.ShouldBe(0);

            long countBar = await CountRowsAsync("Bar");
            countBar.ShouldBe(10L);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTablesIfSchemaSpecified()
        {
            await ExecuteNonQueryAsync("DROP SCHEMA IF EXISTS eggs");
            await ExecuteNonQueryAsync("CREATE SCHEMA eggs");

            await ExecuteNonQueryAsync("CREATE TABLE eggs.foo (Value INT)");
            await ExecuteNonQueryAsync("CREATE TABLE eggs.bar (Value INT)");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("eggs.foo", i);
                await InsertDataAsync("eggs.bar", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake,
                TablesToInclude = new Table[] { new Table("eggs", "foo") }
            });
            await checkpoint.ResetAsync(_connection);

            long countFoo = await CountRowsAsync("Foo");
            countFoo.ShouldBe(0);

            long countBar = await CountRowsAsync("Bar");
            countBar.ShouldBe(10L);
        }

        [SkipOnCI]
        public async Task ShouldExcludeSchemas()
        {
            await ExecuteNonQueryAsync("CREATE SCHEMA a");
            await ExecuteNonQueryAsync("CREATE SCHEMA b");

            await ExecuteNonQueryAsync("CREATE TABLE a.foo (Value INT)");
            await ExecuteNonQueryAsync("CREATE TABLE b.bar (Value INT)");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("a.foo", i);
                await InsertDataAsync("b.bar", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake,
                SchemasToExclude = new[] { "a" }
            });

            await checkpoint.ResetAsync(_connection);

            long countFoo = await CountRowsAsync("a.Foo");
            countFoo.ShouldBe(10L);

            long countBar = await CountRowsAsync("Bar");
            countBar.ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIncludeSchemas()
        {
            await ExecuteNonQueryAsync("CREATE SCHEMA a");
            await ExecuteNonQueryAsync("CREATE SCHEMA b");

            await ExecuteNonQueryAsync("CREATE TABLE a.foo (Value INT)");
            await ExecuteNonQueryAsync("CREATE TABLE b.bar (Value INT)");

            for (int i = 0; i < 10; i++)
            {
                await InsertDataAsync("a.foo", i);
                await InsertDataAsync("b.bar", i);
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Snowflake,
                SchemasToInclude = new[] { "b" }
            });
            await checkpoint.ResetAsync(_connection);

            long countFoo = await CountRowsAsync("a.Foo");
            countFoo.ShouldBe(10L);

            long countBar = await CountRowsAsync("Bar");
            countBar.ShouldBe(0);
        }

        private async Task InsertDataAsync(string tableName, int value)
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = $"INSERT INTO {tableName} (Value) VALUES ({value})";
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task<long> CountRowsAsync(string tableName)
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(1) FROM {tableName}";
                return (long)await command.ExecuteScalarAsync();
            }
        }

        private async Task CreateTableAsync(string tableName)
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = $"CREATE TABLE {tableName} (value INT)";
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}