﻿namespace Respawn.Tests
{
    using System;
    using Npgsql;
    using NPoco;
    using Shouldly;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public class PostgresTests : IDisposable
    {
        private NpgsqlConnection _connection;
        private Database _database;

        public class foo
        {
            public int value { get; set; }
        }
        public class bar
        {
            public int value { get; set; }
        }

        public PostgresTests()
        {
            var dbName = DateTime.Now.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString("N");
            using (var connection = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Integrated Security=true;database=postgres"))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "create database \"" + dbName + "\"";
                    cmd.ExecuteNonQuery();
                }
            }
            _connection = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Integrated Security=true;Database=" + dbName);
            _connection.Open();

            _database = new Database(_connection, DatabaseType.PostgreSQL);
        }

        [TestMethod]
        public void ShouldDeleteData()
        {
            _database.Execute("create table \"foo\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "public" }
            };
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(0);
        }

        [TestMethod]
        public void ShouldIgnoreTables()
        {
            _database.Execute("create table foo (value int)");
            _database.Execute("create table bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
                _database.Execute("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new[] { "public" },
                TablesToIgnore = new[] { "foo" }
            };
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM bar").ShouldBe(0);
        }

        [TestMethod]
        public void ShouldExcludeSchemas()
        {
            _database.Execute("create schema a");
            _database.Execute("create schema b");
            _database.Execute("create table a.foo (value int)");
            _database.Execute("create table b.bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO a.foo VALUES (" + i + ")");
                _database.Execute("INSERT INTO b.bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToExclude = new [] { "a", "pg_catalog" }
            };
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.bar").ShouldBe(0);
        }

        [TestMethod]
        public void ShouldIncludeSchemas()
        {
            _database.Execute("create schema a");
            _database.Execute("create schema b");
            _database.Execute("create table a.foo (value int)");
            _database.Execute("create table b.bar (value int)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO a.foo VALUES (" + i + ")");
                _database.Execute("INSERT INTO b.bar VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = new [] { "b" }
            };
            checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.bar").ShouldBe(0);
        }



        [TestCleanup]
        public void Dispose()
        {
            _database.Dispose();
            _database = null;

            _connection.Close();
            _connection.Dispose();
            _connection = null;
        }
    }
}
