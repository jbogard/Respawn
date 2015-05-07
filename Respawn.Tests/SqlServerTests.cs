namespace Respawn.Tests
{
    using System;
    using System.Data.SqlClient;
    using System.Data.SqlLocalDb;
    using System.Linq;
    using NPoco;
    using Shouldly;

    public class SqlServerTests : IDisposable
    {
        private readonly TemporarySqlLocalDbInstance _instance;
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
            _instance = TemporarySqlLocalDbInstance.Create(deleteFiles: true);

            _connection = _instance.CreateConnection();
            _connection.Open();

            _database = new Database(_connection);

            _database.Execute("create database [SqlServerTests]");
        }

        public void ShouldDeleteData()
        {
            _database.Execute("create table Foo (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint();
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        public void ShouldIgnoreTables()
        {
            _database.Execute("create table Foo (Value [int])");
            _database.Execute("create table Bar (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                TablesToIgnore = new[] {"Foo"}
            };
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        public void ShouldExcludeSchemas()
        {
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
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }

        public void ShouldIncludeSchemas()
        {
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
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM A.Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM B.Bar").ShouldBe(0);
        }



        public void Dispose()
        {
            _database.Execute("drop database [SqlServerTests]");
            _connection.Close();
            _connection.Dispose();
            _connection = null;

            _instance.Dispose();
        }
    }
}
