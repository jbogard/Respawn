#if NET452
namespace Respawn.Tests
{
    using System;
    using System.Threading.Tasks;
    using NPoco;
    using Oracle.ManagedDataAccess.Client;
    using Shouldly;
    using Xunit;

    public class OracleTests : IAsyncLifetime
    {
        private OracleConnection _connection;
        private Database _database;
        private string _createdUser;

        public class foo
        {
            public int value { get; set; }
        }
        public class bar
        {
            public int value { get; set; }
        }

        public async Task InitializeAsync()
        {
            _createdUser = Guid.NewGuid().ToString().Substring(0, 8);
            await CreateUser(_createdUser);

            _connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=xe)));User Id=\"" + _createdUser + "\";Password=123456;");
            await _connection.OpenAsync();

            _database = new Database(_connection, DatabaseType.OracleManaged);
        }

        [Fact]
        public async Task ShouldDeleteData()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
            }

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(100);

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser }
            };
            await checkpoint.Reset(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(0);
        }

        [Fact]
        public async Task ShouldDeleteMultipleTables()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
            };
            await checkpoint.Reset(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(0);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(0);
        }

        [Fact]
        public async Task ShouldIgnoreTables()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
                TablesToIgnore = new[] { "foo" }
            };
            await checkpoint.Reset(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(0);
        }

        [Fact]
        public async Task ShouldExcludeSchemas()
        {
            var userA = Guid.NewGuid().ToString().Substring(0, 8);
            var userB = Guid.NewGuid().ToString().Substring(0, 8);
            await CreateUser(userA);
            await CreateUser(userB);
            await _database.ExecuteAsync("create table \"" + userA + "\".\"foo\" (value int)");
            await _database.ExecuteAsync("create table \"" + userB + "\".\"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"" + userA + "\".\"foo\" VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT INTO \"" + userB + "\".\"bar\" VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                // We must make sure we don't delete all these users that are used by Oracle
                DbAdapter = DbAdapter.Oracle,
                SchemasToExclude = new[]
                {
                    userA, "ANONYMOUS", "APEX_040000", "APEX_PUBLIC_USER", "APPQOSSYS",
                    "CTXSYS", "DBSNMP", "DIP", "FLOWS_FILES", "HR", "MDSYS",
                    "ORACLE_OCM", "OUTLN", "SYS", "XDB", "XS$NULL", "SYSTEM",
                    "GSMADMIN_INTERNAL", "WMSYS", "OJVMSYS", "ORDSYS", "ORDDATA",
                    "LBACSYS", "APEX_040200", "DVSYS", "AUDSYS", "OLAPSYS", "SCOTT"
                }
            };
            await checkpoint.Reset(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userA + "\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userB + "\".\"bar\"")).ShouldBe(0);

            // Clean up before leaving
            await DropUser(userA);
            await DropUser(userB);
        }

        [Fact]
        public async Task ShouldIncludeSchemas()
        {
            var userA = Guid.NewGuid().ToString().Substring(0, 8);
            var userB = Guid.NewGuid().ToString().Substring(0, 8);
            await CreateUser(userA);
            await CreateUser(userB);
            await _database.ExecuteAsync("create table \"" + userA + "\".\"foo\" (value int)");
            await _database.ExecuteAsync("create table \"" + userB + "\".\"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"" + userA + "\".\"foo\" VALUES (" + i + ")");
                await _database.ExecuteAsync("INSERT INTO \"" + userB + "\".\"bar\" VALUES (" + i + ")");
            }

            var checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { userB }
            };
            await checkpoint.Reset(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userA + "\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userB + "\".\"bar\"")).ShouldBe(0);

            // Clean up before leaving
            await DropUser(userA);
            await DropUser(userB);
        }

        private static async Task CreateUser(string userName)
        {
            using (var connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=xe)));User Id=system;Password=oracle;"))
            {
                await connection.OpenAsync();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "create user \"" + userName + "\" IDENTIFIED BY 123456";
                    await cmd.ExecuteNonQueryAsync();
                    // We need some permissions in order to execute all the test queries
                    cmd.CommandText = "alter user \"" + userName + "\" IDENTIFIED BY 123456 account unlock";
                    await cmd.ExecuteNonQueryAsync();
                    cmd.CommandText = "grant all privileges to \"" + userName + "\" IDENTIFIED BY 123456";
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        private static async Task DropUser(string userName)
        {
            using (var connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=xe)));User Id=system;Password=oracle;"))
            {
                await connection.OpenAsync();

                using (var cmd = connection.CreateCommand())
                {
                    // First we need to disconnect the user
                    cmd.CommandText = @"SELECT s.sid, s.serial#, s.status, p.spid FROM v$session s, v$process p WHERE s.username = '" + userName + "' AND p.addr(+) = s.paddr";

                    var dataReader = cmd.ExecuteReader();
                    if (await dataReader.ReadAsync())
                    {
                        var sid = dataReader.GetOracleDecimal(0);
                        var serial = dataReader.GetOracleDecimal(1);

                        cmd.CommandText = "ALTER SYSTEM KILL SESSION '" + sid + ", " + serial + "'";
                        await cmd.ExecuteNonQueryAsync();
                    }

                    cmd.CommandText = "drop user \"" + userName + "\" CASCADE";
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task DisposeAsync()
        {
            // Clean up our mess before leaving
            await DropUser(_createdUser);

            _connection.Close();
            _connection.Dispose();
            _connection = null;

            _database.Dispose();
            _database = null;
        }
    }
}
#endif