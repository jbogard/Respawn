using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Respawn.Graph;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    using System;
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
            var connString = @"Server=(LocalDb)\mssqllocaldb;Database=tempdb;Integrated Security=True";

            await using (var connection = new SqlConnection(connString))
            {
                await connection.OpenAsync();
                using (var database = new Database(connection))
                {
                    await database.ExecuteAsync(@"IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'SqlServerTests') alter database SqlServerTests set single_user with rollback immediate");
                    await database.ExecuteAsync(@"DROP DATABASE IF EXISTS SqlServerTests");
                    await database.ExecuteAsync("create database [SqlServerTests]");
                }
            }

            connString = @"Server=(LocalDb)\mssqllocaldb;Database=SqlServerTests;Integrated Security=True";

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
            await _database.ExecuteAsync("create table Foo (Value [int])");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            try
            {
                await checkpoint.ResetAsync(_connection);
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
            await _database.ExecuteAsync("create table Foo (Value [int], constraint PK_Foo primary key nonclustered (value))");
            await _database.ExecuteAsync("create table Baz (Value [int], FooValue [int], constraint FK_Foo foreign key (FooValue) references Foo (Value))");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Baz { Value = i, FooValue = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Baz").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            try
            {
                await checkpoint.ResetAsync(_connection);
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
            await _database.ExecuteAsync("create table circle (id int primary key, parentid int NULL)");
            await _database.ExecuteAsync("alter table circle add constraint FK_Parent foreign key (parentid) references circle (id)");

            await _database.ExecuteAsync("INSERT INTO \"circle\" (id) VALUES (@0)", 1);
            for (int i = 1; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"circle\" (id, parentid) VALUES (@0, @1)", i + 1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM circle").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            try
            {
                await checkpoint.ResetAsync(_connection);
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

            var checkpoint = await Respawner.CreateAsync(_connection);
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

        [Fact]
        public async Task ShouldHandleCircularRelationships()
        {
            await _database.ExecuteAsync("create table Parent (Id [int] NOT NULL, ChildId [int] NULL, constraint PK_Parent primary key clustered (Id))");
            await _database.ExecuteAsync("create table Child (Id [int] NOT NULL, ParentId [int] NULL, constraint PK_Child primary key clustered (Id))");
            await _database.ExecuteAsync("alter table Parent add constraint FK_Child foreign key (ChildId) references Child (Id)");
            await _database.ExecuteAsync("alter table Child add constraint FK_Parent foreign key (ParentId) references Parent (Id)");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Parent { Id = i, ChildId = null }));
            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Child { Id = i, ParentId = null }));

            await _database.ExecuteAsync("update Parent set ChildId = 0");
            await _database.ExecuteAsync("update Child set ParentId = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Child").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection);
            try
            {
                await checkpoint.ResetAsync(_connection);
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
            await _database.ExecuteAsync("create table Foo (Value [int])");
            await _database.ExecuteAsync("create table Bar (Value [int])");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                TablesToIgnore = new Table[] { "Foo" }
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            } 
            
            _output.WriteLine(checkpoint.DeleteSql);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTablesWithSchema()
        {
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("drop schema if exists B");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create schema B");
            await _database.ExecuteAsync("create table A.Foo (Value [int])");
            await _database.ExecuteAsync("create table A.FooWithBrackets (Value [int])");
            await _database.ExecuteAsync("create table B.Bar (Value [int])");
            await _database.ExecuteAsync("create table B.Foo (Value [int])");

            for (var i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT A.Foo VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT A.FooWithBrackets VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT B.Bar VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT B.Foo VALUES (" + i + ")");
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                TablesToIgnore = new[]
                {
                    new Table("A", "Foo"), 
                    new Table("A", "FooWithBrackets")
                }
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.DeleteSql);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.FooWithBrackets").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeTables()
        {
            await _database.ExecuteAsync("create table Foo (Value [int])");
            await _database.ExecuteAsync("create table Bar (Value [int])");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                TablesToInclude = new Table[] { "Foo" }
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
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
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("drop schema if exists B");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create schema B");
            await _database.ExecuteAsync("create table A.Foo (Value [int])");
            await _database.ExecuteAsync("create table B.Bar (Value [int])");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT A.Foo VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT B.Bar VALUES (" + i + ")");
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                SchemasToExclude = new[] { "A" }
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
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
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("drop schema if exists B");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create schema B");
            await _database.ExecuteAsync("create table A.Foo (Value [int])");
            await _database.ExecuteAsync("create table B.Bar (Value [int])");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT A.Foo VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT B.Bar VALUES (" + i + ")");
            }

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                SchemasToInclude = new[] { "B" }
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
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
            await _database.ExecuteAsync("create table Foo ([id] [int] IDENTITY(1,1), Value int)");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.InsertAsync(new Foo {Value = 0});
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldReseedId_TableWithSchema()
        {
            await _database.ExecuteAsync("IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'A') DROP SCHEMA A");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create table A.Foo ([id] [int] IDENTITY(1,1), Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT A.Foo VALUES (" + i + ")");
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.ExecuteAsync("INSERT A.Foo VALUES (0)");

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldReseedId_TableHasNeverHadAnyData()
        {
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create table A.Foo ([id] [int] IDENTITY(1,1), Value int)");
            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.ExecuteAsync("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldReseedId_TableWithSchemaHasNeverHadAnyData()
        {
            await _database.ExecuteAsync("create table Foo ([id] [int] IDENTITY(1,1), Value int)");
            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.InsertAsync(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1);
        }

        [Fact]
        public async Task ShouldNotReseedId()
        {
            await _database.ExecuteAsync("create table Foo ([id] [int] IDENTITY(1,1), Value int)");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = false
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.InsertAsync(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(101);
        }

        [Fact]
        public async Task ShouldNotReseedId_TableWithSchema()
        {
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create table A.Foo ([id] [int] IDENTITY(1,1), Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT A.Foo VALUES (" + i + ")");
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = false
            });
            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.ExecuteAsync("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(101);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue()
        {
            await _database.ExecuteAsync("create table Foo ([id] [int] IDENTITY(1001,1), Value int)");

            await _database.InsertBulkAsync(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });

            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.InsertAsync(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue_TableWithSchema()
        {
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create table A.Foo ([id] [int] IDENTITY(1001,1), Value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT A.Foo VALUES (" + i + ")");
            }

            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1100);

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });

            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.ExecuteAsync("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue_TableHasNeverHadAnyData()
        {
            await _database.ExecuteAsync("create table Foo ([id] [int] IDENTITY(1001,1), Value int)");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });

            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.InsertAsync(new Foo { Value = 0 });
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldReseedIdAccordingToIdentityInitialSeedValue_TableWithSchemaHasNeverHadAnyData()
        {
            await _database.ExecuteAsync("drop schema if exists A");
            await _database.ExecuteAsync("create schema A");
            await _database.ExecuteAsync("create table A.Foo ([id] [int] IDENTITY(1001,1), Value int)");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                WithReseed = true
            });

            try
            {
                await checkpoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkpoint.ReseedSql);
                throw;
            }

            await _database.ExecuteAsync("INSERT A.Foo VALUES (0)");
            _database.ExecuteScalar<int>("SELECT MAX(id) FROM A.Foo").ShouldBe(1001);
        }

        [Fact]
        public async Task ShouldDeleteTemporalTablesData()
        {
            await _database.ExecuteAsync("drop table if exists FooHistory");
            await _database.ExecuteAsync("IF OBJECT_ID(N'Foo', N'U') IS NOT NULL alter table Foo set (SYSTEM_VERSIONING = OFF)");
            await _database.ExecuteAsync("drop table if exists Foo");

            await _database.ExecuteAsync("create table Foo (Value [int] not null primary key clustered, " +
                                         "ValidFrom datetime2 generated always as row start, " +
                                         "ValidTo datetime2 generated always as row end," +
                                         " period for system_time(ValidFrom, ValidTo)" +
                                         ") with (system_versioning = on (history_table = dbo.FooHistory))");

            await _database.ExecuteAsync("INSERT Foo (Value) VALUES (1)");
            await _database.ExecuteAsync("UPDATE Foo SET Value = 2 Where Value = 1");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                CheckTemporalTables = true
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM FooHistory").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldResetTemporalTableDefaultName()
        {
            await _database.ExecuteAsync("drop table if exists FooHistory");
            await _database.ExecuteAsync("IF OBJECT_ID(N'Foo', N'U') IS NOT NULL alter table Foo set (SYSTEM_VERSIONING = OFF)");
            await _database.ExecuteAsync("drop table if exists Foo");

            await _database.ExecuteAsync("create table Foo (Value [int] not null primary key clustered, " +
                                         "ValidFrom datetime2 generated always as row start, " +
                                         "ValidTo datetime2 generated always as row end," +
                                         " period for system_time(ValidFrom, ValidTo)" +
                                         ") with (system_versioning = on (history_table = dbo.FooHistory))");

            await _database.ExecuteAsync("INSERT Foo (Value) VALUES (1)");
            await _database.ExecuteAsync("UPDATE Foo SET Value = 2 Where Value = 1");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                CheckTemporalTables = true
            });
            await checkpoint.ResetAsync(_connection);

            var sql = @"
SELECT t1.name 
FROM sys.tables t1 
WHERE t1.object_id = (SELECT history_table_id FROM sys.tables t2 WHERE t2.name = 'Foo')
";
            _database.ExecuteScalar<string>(sql).ShouldBe("FooHistory");
        }

        [Fact]
        public async Task ShouldResetTemporalTableAnonymousName()
        {
            // _database.Execute("drop table if exists FooHistory");
            await _database.ExecuteAsync("IF OBJECT_ID(N'Foo', N'U') IS NOT NULL alter table Foo set (SYSTEM_VERSIONING = OFF)");
            await _database.ExecuteAsync("drop table if exists Foo");

            await _database.ExecuteAsync("create table Foo (Value [int] not null primary key clustered, " +
                                         "ValidFrom datetime2 generated always as row start, " +
                                         "ValidTo datetime2 generated always as row end," +
                                         " period for system_time(ValidFrom, ValidTo)" +
                                         ") with (system_versioning = on)");

            await _database.ExecuteAsync("INSERT Foo (Value) VALUES (1)");
            await _database.ExecuteAsync("UPDATE Foo SET Value = 2 Where Value = 1");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                CheckTemporalTables = true
            });
            await checkpoint.ResetAsync(_connection);

            var sql = @"
SELECT t1.name 
FROM sys.tables t1 
WHERE t1.object_id = (SELECT history_table_id FROM sys.tables t2 WHERE t2.name = 'Foo')
";
            _database.ExecuteScalar<string>(sql).ShouldStartWith("MSSQL_TemporalHistoryFor_");
        }

        [Fact]
        public async Task ShouldDeleteTemporalTablesDataFromNotDefaultSchemas()
        {
            await _database.ExecuteAsync("CREATE SCHEMA [TableSchema] AUTHORIZATION [dbo];");
            await _database.ExecuteAsync("CREATE SCHEMA [HistorySchema] AUTHORIZATION [dbo];");

            await _database.ExecuteAsync("create table TableSchema.Foo (Value [int] not null primary key clustered, " +
                                         "ValidFrom datetime2 generated always as row start, " +
                                         "ValidTo datetime2 generated always as row end," +
                                         " period for system_time(ValidFrom, ValidTo)" +
                                         ") with (system_versioning = on (history_table = HistorySchema.FooHistory))");

            await _database.ExecuteAsync("INSERT TableSchema.Foo (Value) VALUES (1)");
            await _database.ExecuteAsync("UPDATE TableSchema.Foo SET Value = 2 Where Value = 1");

            var checkpoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                CheckTemporalTables = true
            });
            await checkpoint.ResetAsync(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM HistorySchema.FooHistory").ShouldBe(0);
        }
    }
}
