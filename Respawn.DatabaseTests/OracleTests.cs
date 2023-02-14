using Xunit.Abstractions;

#if ORACLE
namespace Respawn.DatabaseTests
{
    using System;
    using System.Threading.Tasks;
    using NPoco;
    using Oracle.ManagedDataAccess.Client;
    using Respawn.Graph;
    using Shouldly;
    using Xunit;

    public class OracleTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
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

        public OracleTests(ITestOutputHelper output)
        {
            _output = output;
        }

        public async Task InitializeAsync()
        {
            _createdUser = Guid.NewGuid().ToString().Substring(0, 8);
            await CreateUser(_createdUser);

            _connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=10521))(CONNECT_DATA=(SID=xe)));User Id=\"" + _createdUser + "\";Password=123456;");
            await _connection.OpenAsync();

            _database = new Database(_connection, DatabaseType.OracleManaged);
        }

        [SkipOnCI]
        public async Task ShouldDeleteData()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
            }

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(100);

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser }
            });
            try
            {
                await respawner.ResetAsync(_connection);
            }
            finally
            {
                _output.WriteLine(_createdUser);
                _output.WriteLine(respawner.DeleteSql);
            }

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldDeleteMultipleTables()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(0);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleRelationships()
        {
            _database.Execute("create table \"foo\" (value int, primary key (value))");
            _database.Execute("create table \"baz\" (value int, foovalue int, constraint FK_Foo foreign key (foovalue) references \"foo\" (value))");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
                _database.Execute("INSERT INTO \"baz\" VALUES (@0, @0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"baz\"").ShouldBe(100);

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
            });
            try
            {
                await respawner.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(respawner.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"baz\"").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleRelationshipsWithTableNames()
        {
            _database.Execute("create table \"foo\" (value int, primary key (value))");
            _database.Execute("create table \"baz\" (value int, foovalue int, constraint FK_Foo foreign key (foovalue) references \"foo\" (value))");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
                _database.Execute("INSERT INTO \"baz\" VALUES (@0, @0)", i);
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"baz\"").ShouldBe(100);

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
                TablesToInclude = new[] { new Table(_createdUser, "foo"), new Table(_createdUser, "baz") },
                TablesToIgnore = new[] { new Table(_createdUser, "bar") }
            });
            try
            {
                await respawner.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(respawner.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"baz\"").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleRelationshipsWithNamedPrimaryKeyConstraint()
        {
            var userA = Guid.NewGuid().ToString().Substring(0, 8);
            var userB = Guid.NewGuid().ToString().Substring(0, 8);
            await CreateUser(userA);
            await CreateUser(userB);
            await _database.ExecuteAsync($"create table \"{userA}\".\"foo\" (value int, constraint PK_Foo primary key (value))");
            await _database.ExecuteAsync($"create table \"{userA}\".\"baz\" (value int, foovalue int, constraint FK_Foo foreign key (foovalue) references \"{userA}\".\"foo\" (value))");
            await _database.ExecuteAsync($"create table \"{userB}\".\"foo\" (value int, constraint PK_Foo primary key (value))");
            await _database.ExecuteAsync($"create table \"{userB}\".\"baz\" (value int, foovalue int, constraint FK_Foo foreign key (foovalue) references \"{userB}\".\"foo\" (value))");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync($"INSERT INTO \"{userA}\".\"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync($"INSERT INTO \"{userA}\".\"baz\" VALUES (@0, @0)", i);
                await _database.ExecuteAsync($"INSERT INTO \"{userB}\".\"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync($"INSERT INTO \"{userB}\".\"baz\" VALUES (@0, @0)", i);
            }

            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userA}\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userA}\".\"baz\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userB}\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userB}\".\"baz\"")).ShouldBe(100);

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { userA },
            });
            try
            {
                await respawner.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(respawner.DeleteSql ?? string.Empty);
                throw;
            }

            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userA}\".\"foo\"")).ShouldBe(0);
            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userA}\".\"baz\"")).ShouldBe(0);
            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userB}\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM \"{userB}\".\"baz\"")).ShouldBe(100);
        
            await DropUser(userA);
            await DropUser(userB);
        }

        [SkipOnCI]
        public async Task ShouldHandleComplexCycles()
        {
            _database.Execute("create table \"a\" (\"id\" int primary key, \"b_id\" int NULL)");
            _database.Execute("create table \"b\" (\"id\" int primary key, \"a_id\" int NULL, \"c_id\" int NULL, \"d_id\" int NULL)");
            _database.Execute("create table \"c\" (\"id\" int primary key, \"d_id\" int NULL)");
            _database.Execute("create table \"d\" (\"id\" int primary key)");
            _database.Execute("create table \"e\" (\"id\" int primary key, \"a_id\" int NULL)");
            _database.Execute("create table \"f\" (\"id\" int primary key, \"b_id\" int NULL)");
            _database.Execute("alter table \"a\" add constraint \"FK_a_b\" foreign key (\"b_id\") references \"b\" (\"id\")");
            _database.Execute("alter table \"b\" add constraint \"FK_b_a\" foreign key (\"a_id\") references \"a\" (\"id\")");
            _database.Execute("alter table \"b\" add constraint \"FK_b_c\" foreign key (\"c_id\") references \"c\" (\"id\")");
            _database.Execute("alter table \"b\" add constraint \"FK_b_d\" foreign key (\"d_id\") references \"d\" (\"id\")");
            _database.Execute("alter table \"c\" add constraint \"FK_c_d\" foreign key (\"d_id\") references \"d\" (\"id\")");
            _database.Execute("alter table \"e\" add constraint \"FK_e_a\" foreign key (\"a_id\") references \"a\" (\"id\")");
            _database.Execute("alter table \"f\" add constraint \"FK_f_b\" foreign key (\"b_id\") references \"b\" (\"id\")");


            _database.Execute("insert into \"d\" (\"id\") values (1)");
            _database.Execute("insert into \"c\" (\"id\", \"d_id\") values (1, 1)");
            _database.Execute("insert into \"a\" (\"id\") values (1)");
            _database.Execute("insert into \"b\" (\"id\", \"c_id\", \"d_id\") values (1, 1, 1)");
            _database.Execute("insert into \"e\" (\"id\", \"a_id\") values (1, 1)");
            _database.Execute("insert into \"f\" (\"id\", \"b_id\") values (1, 1)");
            _database.Execute("update \"a\" set \"b_id\" = 1");
            _database.Execute("update \"b\" set \"a_id\" = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"a\"").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"b\"").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"c\"").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"d\"").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"e\"").ShouldBe(1);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"f\"").ShouldBe(1);

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
            });
            try
            {
                await respawner.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(respawner.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"a\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"b\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"c\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"d\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"e\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"f\"").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldHandleCircularRelationships()
        {
            _database.Execute("create table \"parent\" (id int primary key, childid int NULL)");
            _database.Execute("create table \"child\" (id int primary key, parentid int NULL)");
            _database.Execute("alter table \"parent\" add constraint FK_Child foreign key (ChildId) references \"child\" (Id)");
            _database.Execute("alter table \"child\" add constraint FK_Parent foreign key (ParentId) references \"parent\" (Id)");

            for (int i = 0; i < 100; i++)
            {
                _database.Execute("INSERT INTO \"parent\" VALUES (@0, null)", i);
                _database.Execute("INSERT INTO \"child\" VALUES (@0, null)", i);
            }

            _database.Execute("update \"parent\" set childid = 0");
            _database.Execute("update \"child\" set parentid = 1");

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"parent\"").ShouldBe(100);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"child\"").ShouldBe(100);

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
            });
            try
            {
                await respawner.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(respawner.DeleteSql ?? string.Empty);
                throw;
            }

            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"parent\"").ShouldBe(0);
            _database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"child\"").ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTables()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
                TablesToIgnore = new[] { new Table("foo") }
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTablesWithSchema()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
                TablesToIgnore = new[] { new Table(_createdUser, "foo") }
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(0);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTables()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { _createdUser },
                TablesToInclude = new[] { new Table("foo") }
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(0);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(100);
        }

        [SkipOnCI]
        public async Task ShouldIncludeTablesWithSchema()
        {
            await _database.ExecuteAsync("create table \"foo\" (value int)");
            await _database.ExecuteAsync("create table \"bar\" (value int)");

            for (int i = 0; i < 100; i++)
            {
                await _database.ExecuteAsync("INSERT INTO \"foo\" VALUES (@0)", i);
                await _database.ExecuteAsync("INSERT INTO \"bar\" VALUES (@0)", i);
            }

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                TablesToInclude = new[] { new Table(_createdUser, "foo") }
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"foo\"")).ShouldBe(0);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"bar\"")).ShouldBe(100);
        }

        [SkipOnCI]
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

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
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
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userA + "\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userB + "\".\"bar\"")).ShouldBe(0);

            // Clean up before leaving
            await DropUser(userA);
            await DropUser(userB);
        }

        [SkipOnCI]
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

            var respawner = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.Oracle,
                SchemasToInclude = new[] { userB }
            });
            await respawner.ResetAsync(_connection);

            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userA + "\".\"foo\"")).ShouldBe(100);
            (await _database.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM \"" + userB + "\".\"bar\"")).ShouldBe(0);

            // Clean up before leaving
            await DropUser(userA);
            await DropUser(userB);
        }

        private static async Task CreateUser(string userName)
        {
            using (var connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=10521))(CONNECT_DATA=(SID=xe)));User Id=system;Password=oracle;"))
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
            using (var connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=10521))(CONNECT_DATA=(SID=xe)));User Id=system;Password=oracle;"))
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
            _database.Dispose();
            _database = null;

            OracleConnection.ClearPool(_connection);

            _connection.Close();
            _connection.Dispose();
            _connection = null;

            // Clean up our mess before leaving
            await DropUser(_createdUser);
        }
    }
}
#endif