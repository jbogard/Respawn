using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Respawn.Graph;
using Testcontainers.MsSql;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    using System;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class EmptyDbTests : IAsyncLifetime
    {
        private SqlConnection _connection;
        private Database _database;
        private MsSqlContainer _msSqlContainer;


        public EmptyDbTests(ITestOutputHelper output)
        {
        }

        public async Task InitializeAsync()
        {
            _msSqlContainer = new MsSqlBuilder().Build();
            await _msSqlContainer.StartAsync();
            
            var connString = _msSqlContainer.GetConnectionString();

            await using (var connection = new SqlConnection(connString))
            {
                await connection.OpenAsync();
                using (var database = new Database(connection))
                {
                    await database.ExecuteAsync(@"IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'EmptyDbTests') alter database EmptyDbTests set single_user with rollback immediate");
                    await database.ExecuteAsync(@"DROP DATABASE IF EXISTS EmptyDbTests");
                    await database.ExecuteAsync("create database [EmptyDbTests]");
                }
            }
            
            var newConnString = new SqlConnectionStringBuilder(connString)
            {
                InitialCatalog = "EmptyDbTests"
            }.ConnectionString;

            _connection = new SqlConnection(newConnString);
            _connection.Open();

            _database = new Database(_connection);
        }

        public Task DisposeAsync()
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;
            return Task.FromResult(0);
        }

        [Fact]
        public async Task ShouldThrowWhenDatabaseEmpty()
        {
            var exception = await Should.ThrowAsync<InvalidOperationException>(Respawner.CreateAsync(_connection));

            exception.Message.ShouldContain("No tables found");
        }
    }
}
