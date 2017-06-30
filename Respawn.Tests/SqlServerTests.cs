using System.Globalization;
using System.Threading.Tasks;
using Xunit;

namespace Respawn.Tests
{
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class SqlServerTests : IDisposable
    {
        private SqlConnection _connection;
        private readonly Database _database;

        public class Foo
        {
            public int Value { get; set; }
        }
        public class Bar
        {
            public int Value { get; set; }
        }

        public SqlServerTests()
        {
            var isAppVeyor = Environment.GetEnvironmentVariable("Appveyor")?.ToUpperInvariant() == "TRUE";

            var connString =
                isAppVeyor
                    ? @"Server=(local)\SQL2016;Database=tempdb;User ID=sa;Password=Password12!"
                    : @"Data Source=.\sqlexpress;Initial Catalog=tempdb;Integrated Security=True";

            _connection = new SqlConnection(connString);
            _connection.Open();

            _database = new Database(_connection);

            _database.Execute(@"DROP DATABASE IF EXISTS SqlServerTests");
            _database.Execute("create database [SqlServerTests]");
        }

        [Fact]
        public async Task ShouldDeleteData()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("create table Foo (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bar");
            _database.Execute("create table Foo (Value [int])");
            _database.Execute("create table Bar (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                TablesToIgnore = new[] {"Foo"}
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldExcludeSchemas()
        {
            _database.Execute("drop table if exists A.Foo");
            _database.Execute("drop table if exists B.Bar");
            _database.Execute("drop schema if exists A");
            _database.Execute("drop schema if exists B");
            _database.Execute("create schema A");
            _database.Execute("create schema B");
            _database.Execute("create table A.Foo (Value [int])");
            _database.Execute("create table B.Bar (Value [int])");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT A.Foo VALUES (" + i + ")");
                _database.Execute("INSERT B.Bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                SchemasToExclude = new [] { "A" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeSchemas()
        {
            _database.Execute("drop table if exists A.Foo");
            _database.Execute("drop table if exists B.Bar");
            _database.Execute("drop schema if exists A");
            _database.Execute("drop schema if exists B");
            _database.Execute("create schema A");
            _database.Execute("create schema B");
            _database.Execute("create table A.Foo (Value [int])");
            _database.Execute("create table B.Bar (Value [int])");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT A.Foo VALUES (" + i + ")");
                _database.Execute("INSERT B.Bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                SchemasToInclude = new [] { "B" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }



        public void Dispose()
        {
            _connection.Close();
            _connection.Dispose();
            _connection = null;
        }
    }
}
