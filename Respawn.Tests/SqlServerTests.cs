using System.Globalization;
using System.Threading.Tasks;
using Xunit;

namespace Respawn.DatabaseTests
{
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class SqlServerTests : IAsyncLifetime
    {
        private SqlConnection _connection;
        private Database _database;

        public class Foo
        {
            public int Value { get; set; }
        }
        public class Bar
        {
            public int Value { get; set; }
        }
        public class Baz
        {
            public int Value { get; set; }
            public int FooValue { get; set; }
        }

        [PrimaryKey("Id", AutoIncrement = false)]
        public class Parent
        {
            public int Id { get; set; }
            public int? ChildId { get; set; }
        }

        [PrimaryKey("Id", AutoIncrement = false)]
        public class Child
        {
            public int Id { get; set; }
            public int? ParentId { get; set; }
        }

        public async Task InitializeAsync()
        {
            var isAppVeyor = Environment.GetEnvironmentVariable("Appveyor")?.ToUpperInvariant() == "TRUE";
            var connString =
                isAppVeyor
                    ? @"Server=(local)\SQL2016;Database=tempdb;User ID=sa;Password=Password12!"
                    : @"Server=(LocalDb)\mssqllocaldb;Database=tempdb;Integrated Security=True";

            using (var connection = new SqlConnection(connString))
            {
                await connection.OpenAsync();
                using (var database = new Database(connection))
                {
                    await database.ExecuteAsync(@"IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'SqlServerTests') alter database SqlServerTests set single_user with rollback immediate");
                    await database.ExecuteAsync(@"DROP DATABASE IF EXISTS SqlServerTests");
                    await database.ExecuteAsync("create database [SqlServerTests]");
                }
            }

            connString =
                isAppVeyor
                    ? @"Server=(local)\SQL2016;Database=SqlServerTests;User ID=sa;Password=Password12!"
                    : @"Server=(LocalDb)\mssqllocaldb;Database=SqlServerTests;Integrated Security=True";

            _connection = new SqlConnection(connString);
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
        public async Task ShouldDeleteData()
        {
            _database.Execute("create table Foo (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleRelationships()
        {
            _database.Execute("create table Foo (Value [int], constraint PK_Foo primary key nonclustered (value))");
            _database.Execute("create table Baz (Value [int], FooValue [int], constraint FK_Foo foreign key (FooValue) references Foo (Value))");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Baz { Value = i, FooValue = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Baz").ShouldBe(100);

            var checkpoint = new Checkpoint();
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Baz").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleCircularRelationships()
        {
            _database.Execute("create table Parent (Id [int] NOT NULL, ChildId [int] NULL, constraint PK_Parent primary key clustered (Id))");
            _database.Execute("create table Child (Id [int] NOT NULL, ParentId [int] NULL, constraint PK_Child primary key clustered (Id))");
            _database.Execute("alter table Parent add constraint FK_Child foreign key (ChildId) references Child (Id)");
            _database.Execute("alter table Child add constraint FK_Parent foreign key (ParentId) references Parent (Id)");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Parent { Id = i, ChildId = null }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Child { Id = i, ParentId = null }));

            _database.Execute("update Parent set ChildId = 0");
            _database.Execute("update Child set ParentId = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Child").ShouldBe(100);

            var checkpoint = new Checkpoint();
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Parent").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Child").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("create table Foo (Value [int])");
            _database.Execute("create table Bar (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                TablesToIgnore = new[] { "Foo" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldExcludeSchemas()
        {
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
                SchemasToExclude = new[] { "A" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeSchemas()
        {
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
                SchemasToInclude = new[] { "B" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }
    }
}
