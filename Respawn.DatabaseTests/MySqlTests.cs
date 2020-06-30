using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    using System;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class MySqlTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private MySqlConnection _connection;
        private readonly IDatabase _database;
        
        public class Foo
        {
            public int Value { get; set; }
        }
        public class Bar
        {
            public int Value { get; set; }
        }

        public MySqlTests(ITestOutputHelper output)
        {
            _output = output;
            var isCI = Environment.GetEnvironmentVariable("CI")?.ToUpperInvariant() == "TRUE";

            var connString =
                isCI
                    ? @"Server=127.0.0.1; port = 3306; User Id = root; Password = Password12!"
                    : @"Server=127.0.0.1; port = 8082; User Id = root; Password = testytest";

            _connection = new MySqlConnection(connString);
            _connection.Open();
            
            _database = new Database(_connection);

            _database.Execute(@"DROP DATABASE IF EXISTS MySqlTests");
            _database.Execute("create database MySqlTests");
            _database.Execute("use MySqlTests");
        }

        [SkipOnCI]
        public async Task ShouldDeleteData()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("CREATE TABLE `Foo` (`Value` int(3))");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToInclude = new[] { "MySqlTests" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldDeleteDataWithRelationships()
        {
            // Tests a more complex scenario with 2 FK relationships
            
            // - Foo has both a PK and an FK relationship
            // - Bob.BobValue PK --> Foo.BobValue
            // - Foo.FooValue PK --> Bar.BarValue

            // It should delete the tables in the order Bar, Foo, Bob

            _database.Execute("drop table if exists Bar");
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bob");

            _database.Execute(@"
CREATE TABLE `Bob` (
  `BobValue` int(3) NOT NULL, 
  PRIMARY KEY (`BobValue`)
)");

            _database.Execute(@"
CREATE TABLE `Foo` (
  `FooValue` int(3) NOT NULL,
  `BobValue` int(3) NOT NULL,
  PRIMARY KEY (`FooValue`),
  KEY `IX_BobValue` (`BobValue`),
  CONSTRAINT `FK_FOO_BOB` FOREIGN KEY (`BobValue`) REFERENCES `Bob` (`BobValue`) ON DELETE NO ACTION ON UPDATE NO ACTION
)");

            _database.Execute(@"
CREATE TABLE `Bar` (
  `BarValue` int(3) NOT NULL,
  PRIMARY KEY (`BarValue`),
  CONSTRAINT `FK_BAR_FOO` FOREIGN KEY (`BarValue`) REFERENCES `Foo` (`FooValue`) ON DELETE NO ACTION ON UPDATE NO ACTION
)");

            for (var i = 0; i < 100; i++)
            {
                _database.Execute($"INSERT `Bob` VALUES ({i})");
                _database.Execute($"INSERT `Foo` VALUES ({i},{i})");
                _database.Execute($"INSERT `Bar` VALUES ({i})");
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bob").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToInclude = new[] { "MySqlTests" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bob").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleSelfRelationships()
        {
            _database.Execute("create table foo (id int primary key, parentid int NULL)");
            _database.Execute("alter table foo add constraint FK_Parent foreign key (parentid) references foo (id)");

            _database.Execute("INSERT INTO `foo` (id) VALUES (@0)", 1);
            for (int i = 1; i < 100; i++)
            {
                _database.Execute("INSERT INTO `foo` VALUES (@0, @1)", i + 1, i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToInclude = new[] { "MySqlTests" }
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


        [SkipOnCI]
        public async Task ShouldHandleCircularRelationships()
        {
            _database.Execute("create table parent (id int primary key, childid int NULL)");
            _database.Execute("create table child (id int primary key, parentid int NULL)");
            _database.Execute("alter table parent add constraint FK_Child foreign key (ChildId) references child (Id)");
            _database.Execute("alter table child add constraint FK_Parent foreign key (ParentId) references parent (Id)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO parent VALUES (@0, null)", i);
                _database.Execute("INSERT INTO child VALUES (@0, null)", i);
            }

            _database.Execute("update parent set childid = 0");
            _database.Execute("update child set parentid = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToInclude = new[] { "MySqlTests" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM parent").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM child").ShouldBe(0);
        }

        [SkipOnCI]
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

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToInclude = new[] { "MySqlTests" }
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

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM c").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM d").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM e").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM f").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bar");
            _database.Execute("create table `Foo` (`Value` int(3))");
            _database.Execute("create table `Bar` (`Value` int(3))");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                TablesToIgnore = new[] { "Foo" },
                SchemasToInclude = new[] { "MySqlTests" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTables()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bar");
            _database.Execute("create table `Foo` (`Value` int(3))");
            _database.Execute("create table `Bar` (`Value` int(3))");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                TablesToInclude = new[] { "Foo" },
                SchemasToInclude = new[] { "MySqlTests" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(100);
        }

        [SkipOnCI]
        public async Task ShouldExcludeSchemas()
        {
            _database.Execute("drop table if exists `A`.`Foo`");
            _database.Execute("drop table if exists `B`.`Bar`");
            _database.Execute("drop schema if exists `A`");
            _database.Execute("drop schema if exists `B`");
            _database.Execute("create schema `A`");
            _database.Execute("create schema `B`");
            _database.Execute("create table `A`.`Foo` (`Value` int(3))");
            _database.Execute("create table `B`.`Bar` (`Value` int(3))");

            for (var i = 0; i < 100; i++)
            {
                _database.Execute("INSERT `A`.`Foo` VALUES (" + i + ")");
                _database.Execute("INSERT `B`.`Bar` VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToExclude = new[] { "A", "MySqlTests" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIncludeSchemas()
        {
            _database.Execute("drop table if exists `A`.`Foo`");
            _database.Execute("drop table if exists `B`.`Bar`");
            _database.Execute("drop schema if exists `A`");
            _database.Execute("drop schema if exists `B`");
            _database.Execute("create schema `A`");
            _database.Execute("create schema `B`");
            _database.Execute("create table `A`.`Foo` (`Value` int(3))");
            _database.Execute("create table `B`.`Bar` (`Value` int(3))");

            for (var i = 0; i < 100; i++)
            {
                _database.Execute("INSERT A.Foo VALUES (" + i + ")");
                _database.Execute("INSERT B.Bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToInclude = new[] { "B" }
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
