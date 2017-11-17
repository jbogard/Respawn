using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Xunit;

namespace Respawn.Tests
{
    using System;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class MySqlTests : IDisposable
    {
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

        public MySqlTests()
        {
            var isAppVeyor = Environment.GetEnvironmentVariable("Appveyor")?.ToUpperInvariant() == "TRUE";

            var connString =
                isAppVeyor
                    ? @"Server=127.0.0.1; port = 3306; User Id = root; Password = Password12!"
                    : @"Server=127.0.0.1; port = 3306; User Id = root; Password = password";

            _connection = new MySqlConnection(connString);
            _connection.Open();
            
            _database = new Database(_connection);

            _database.Execute(@"DROP DATABASE IF EXISTS MySqlTests");
            _database.Execute("create database MySqlTests");
            _database.Execute("use MySqlTests");
        }

        [Fact]
        public async Task ShouldDeleteData()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("CREATE TABLE `Foo` (`Value` int(11))");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint()
            {
                DbAdapter = DbAdapter.MySql
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("drop table if exists Foo");
            _database.Execute("drop table if exists Bar");
            _database.Execute("create table `Foo` (`Value` int(11))");
            _database.Execute("create table `Bar` (`Value` int(11))");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                TablesToIgnore = new[] { "Foo" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldExcludeSchemas()
        {
            _database.Execute("drop table if exists `A`.`Foo`");
            _database.Execute("drop table if exists `B`.`Bar`");
            _database.Execute("drop schema if exists `A`");
            _database.Execute("drop schema if exists `B`");
            _database.Execute("create schema `A`");
            _database.Execute("create schema `B`");
            _database.Execute("create table `A`.`Foo` (`Value` int)");
            _database.Execute("create table `B`.`Bar` (`Value` int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT `A`.`Foo` VALUES (" + i + ")");
                _database.Execute("INSERT `B`.`Bar` VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.MySql,
                SchemasToExclude = new[] { "A" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIncludeSchemas()
        {
            _database.Execute("drop table if exists `A`.`Foo`");
            _database.Execute("drop table if exists `B`.`Bar`");
            _database.Execute("drop schema if exists `A`");
            _database.Execute("drop schema if exists `B`");
            _database.Execute("create schema `A`");
            _database.Execute("create schema `B`");
            _database.Execute("create table `A`.`Foo` (`Value` int)");
            _database.Execute("create table `B`.`Bar` (`Value` int)");

            for (int i = 0; i < 100; i++)
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
