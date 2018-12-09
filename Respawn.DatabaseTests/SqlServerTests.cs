using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class SqlServerTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
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

        public SqlServerTests(ITestOutputHelper output) => _output = output;

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
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

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
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Baz").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleSelfRelationships()
        {
            _database.Execute("create table circle (id int primary key, parentid int NULL)");
            _database.Execute("alter table circle add constraint FK_Parent foreign key (parentid) references circle (id)");

            _database.Execute("INSERT INTO \"circle\" (id) VALUES (@0)", 1);
            for (int i = 1; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"circle\" (id, parentid) VALUES (@0, @1)", i + 1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM circle").ShouldBe(100);

            var checkpoint = new Checkpoint();
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM circle").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldHandleComplexCycles()
        {
            _database.Execute("create table a (id int primary key, b_id int NULL)");
            _database.Execute("create table b (id int primary key, a_id int NULL, c_id int NULL, d_id int NULL)");
            _database.Execute("create table c (id int primary key, d_id int NULL)");
            _database.Execute("create table d (id int primary key)");
            _database.Execute("create table e (id int primary key, a_id int NULL)");
            _database.Execute("create table f (id int primary key, b_id int NULL)");
            _database.Execute("alter table a add constraint FK_a_b foreign key (b_id) references b (id)");
            _database.Execute("alter table b add constraint FK_b_a foreign key (a_id) references a (id)");
            _database.Execute("alter table b add constraint FK_b_c foreign key (c_id) references c (id)");
            _database.Execute("alter table b add constraint FK_b_d foreign key (d_id) references d (id)");
            _database.Execute("alter table c add constraint FK_c_d foreign key (d_id) references d (id)");
            _database.Execute("alter table e add constraint FK_e_a foreign key (a_id) references a (id)");
            _database.Execute("alter table f add constraint FK_f_b foreign key (b_id) references b (id)");


            _database.Execute("insert into d (id) values (1)");
            _database.Execute("insert into c (id, d_id) values (1, 1)");
            _database.Execute("insert into a (id) values (1)");
            _database.Execute("insert into b (id, c_id, d_id) values (1, 1, 1)");
            _database.Execute("insert into e (id, a_id) values (1, 1)");
            _database.Execute("insert into f (id, b_id) values (1, 1)");
            _database.Execute("update a set b_id = 1");
            _database.Execute("update b set a_id = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM c").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM d").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM e").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM f").ShouldBe(1);

            var checkpoint = new Checkpoint();
            try
            {
                await checkpoint.Reset(_connection);
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
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

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
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeTables()
        {
            _database.Execute("create table Foo (Value [int])");
            _database.Execute("create table Bar (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                TablesToInclude = new[] { "Foo" }
            };
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(100);
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
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

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
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldReseedId()
        {
            _database.Execute("create table Foo ([id] [int] IDENTITY(1,1), Value int)");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Insert(new Foo {Value = 0});
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldReseedId_TableWithSchema()
        {
            _database.Execute("IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'A') DROP SCHEMA A");
            _database.Execute("create schema A");
            _database.Execute("create table A.Foo ([id] [int] IDENTITY(1,1), Value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT A.Foo VALUES (" + i + ")");
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Execute("INSERT A.Foo VALUES (0)");

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldReseedId_TableHasNeverHadAnyData()
        {
            _database.Execute("drop schema if exists A");
            _database.Execute("create schema A");
            _database.Execute("create table A.Foo ([id] [int] IDENTITY(1,1), Value int)");
            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Execute("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldReseedId_TableWithSchemaHasNeverHadAnyData()
        {
            _database.Execute("create table Foo ([id] [int] IDENTITY(1,1), Value int)");
            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Insert(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldNotReseedId()
        {
            _database.Execute("create table Foo ([id] [int] IDENTITY(1,1), Value int)");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = false;
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Insert(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(101);
        }

        [Fact]
        public async Task ShouldNotReseedId_TableWithSchema()
        {
            _database.Execute("drop schema if exists A");
            _database.Execute("create schema A");
            _database.Execute("create table A.Foo ([id] [int] IDENTITY(1,1), Value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT A.Foo VALUES (" + i + ")");
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = false;
            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Execute("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(101);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue()
        {
            _database.Execute("create table Foo ([id] [int] IDENTITY(1001,1), Value int)");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1100);

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;

            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Insert(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue_TableWithSchema()
        {
            _database.Execute("drop schema if exists A");
            _database.Execute("create schema A");
            _database.Execute("create table A.Foo ([id] [int] IDENTITY(1001,1), Value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT A.Foo VALUES (" + i + ")");
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1100);

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;

            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Execute("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue_TableHasNeverHadAnyData()
        {
            _database.Execute("create table Foo ([id] [int] IDENTITY(1001,1), Value int)");

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;

            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Insert(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue_TableWithSchemaHasNeverHadAnyData()
        {
            _database.Execute("drop schema if exists A");
            _database.Execute("create schema A");
            _database.Execute("create table A.Foo ([id] [int] IDENTITY(1001,1), Value int)");

            var checkpoint = new Checkpoint();
            checkpoint.WithReseed = true;

            try
            {
                await checkpoint.Reset(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            _database.Execute("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1001);
        }
    }
}
