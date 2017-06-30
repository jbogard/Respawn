#if NET452
using System;
using System.Data.SqlServerCe;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NPoco;
using Shouldly;
using Xunit;

namespace Respawn.Tests
{
    public class SqlServerCeTests : IDisposable
    {
        private SqlCeEngine _engine;
        private SqlCeConnection _connection;
        private Database _database;

        public class Foo
        {
            public int Value { get; set; }
        }
        public class Bar
        {
            public int Value { get; set; }
        }

        public SqlServerCeTests()
        {
            _engine = new SqlCeEngine("Data Source=Test.sdf;Password=ppp123_");

            if (File.Exists(AppDomain.CurrentDomain.BaseDirectory + "\\Test.sdf"))
            {
                File.Delete(AppDomain.CurrentDomain.BaseDirectory + "\\Test.sdf");
            }

            _engine.CreateDatabase();
            _connection = new SqlCeConnection(_engine.LocalConnectionString);
            _connection.Open();

            _database = new Database(_connection);
        }

        [Fact]
        public async Task ShouldDeleteData()
        {
            _database.Execute("create table Foo (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new SqlServerTests.Foo { Value = i }));

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.SqlServerCe
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            _database.Execute("create table Foo (Value [int])");
            _database.Execute("create table Bar (Value [int])");

            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new SqlServerTests.Foo { Value = i }));
            _database.InsertBulk(Enumerable.Range(0, 100).Select(i => new SqlServerTests.Bar { Value = i }));

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.SqlServerCe,
                TablesToIgnore = new[] { "Foo" }
            };
            await checkpoint.Reset(_connection);

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Foo").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM Bar").ShouldBe(0);
        }

        public void Dispose()
        {
            _engine.Dispose();
            _connection.Close();
            _connection.Dispose();
        }
    }
}
#endif