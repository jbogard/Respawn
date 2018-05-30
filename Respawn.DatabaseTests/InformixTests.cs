#if NETCOREAPP2_0
using IBM.Data.DB2.Core;
using Respawn;
using Shouldly;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    public class InformixTests : IAsyncLifetime
    {
        private DB2Connection _connection;
        private readonly ITestOutputHelper _output;

        public InformixTests(ITestOutputHelper output)
        {
            _output = output;
        }

        public Task DisposeAsync()
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;
            return Task.FromResult(0);
        }

        public async Task InitializeAsync()
        {
            const string connString = "Server=127.0.0.1:9089;Database=dummyifx;UID=informix;PWD=in4mix;Persist Security Info=True;Authentication=Server;";
            _connection = new DB2Connection(connString);
            await _connection.OpenAsync();
        }

        [SkipOnAppVeyor]
        public async Task ShouldDeleteData()
        {
            using (var command = new DB2Command("DROP TABLE IF EXISTS Foo; CREATE TABLE Foo (Value INT);", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO Foo VALUES (?)";
                for (int i = 0; i < 100; i++)
                {
                    command.Parameters.Add(new DB2Parameter("Value", i));
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                command.CommandText = "SELECT COUNT(1) FROM Foo";
                command.ExecuteScalar().ShouldBe(100);

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { "informix" }
                };
                await checkPoint.Reset(_connection);

                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldIgnoreTables()
        {
            using (var command = new DB2Command("DROP TABLE IF EXISTS Foo; CREATE TABLE Foo (Value INT);", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = "DROP TABLE IF EXISTS Bar; CREATE TABLE Bar (Value INT);";
                command.ExecuteNonQuery();
                for (int i = 0; i < 100; i++)
                {
                    command.Parameters.Add(new DB2Parameter("Value", i));
                    command.CommandText = "INSERT INTO Foo VALUES (?);";
                    command.ExecuteNonQuery();
                    command.CommandText = "INSERT INTO Bar VALUES (?);";
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                var checkPoint = new Checkpoint()
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { "informix" },
                    TablesToIgnore = new[] { "foo" }
                };
                await checkPoint.Reset(_connection);

                command.CommandText = "SELECT COUNT(1) FROM Foo";
                command.ExecuteScalar().ShouldBe(100);
                command.CommandText = "SELECT COUNT(1) FROM Bar";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldHandleRelationships()
        {
            using (var command = new DB2Command("DROP TABLE IF EXISTS Foo; CREATE TABLE Foo (Value INT PRIMARY KEY);", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = @"DROP TABLE IF EXISTS Bar; 
                                        CREATE TABLE Bar (
                                            Value INT,
                                            FooValue INT,
                                            FOREIGN KEY (FooValue) REFERENCES Foo(Value)
                                        );";
                command.ExecuteNonQuery();
                for (int i = 0; i < 100; i++)
                {
                    command.Parameters.Add(new DB2Parameter("Value1", i));
                    command.Parameters.Add(new DB2Parameter("Value2", i));
                    command.CommandText = "INSERT INTO Foo VALUES (?);";
                    command.ExecuteNonQuery();
                    command.CommandText = "INSERT INTO Bar VALUES (?, ?);";
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                command.CommandText = "SELECT COUNT(1) FROM Foo";
                command.ExecuteScalar().ShouldBe(100);
                command.CommandText = "SELECT COUNT(1) FROM Bar";
                command.ExecuteScalar().ShouldBe(100);

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { "informix" }
                };
                try
                {
                    await checkPoint.Reset(_connection);
                }
                catch
                {
                    _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                    throw;
                }

                command.CommandText = "SELECT COUNT(1) FROM Foo";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM Bar";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldHandleCircularRelationships()
        {
            using (var command = new DB2Command(@"DROP TABLE IF EXISTS Parent; 
                                                  CREATE TABLE Parent (
                                                      Id INT PRIMARY KEY,
                                                      ChildId INT NULL
                                                  );", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = @"DROP TABLE IF EXISTS Child; 
                                        CREATE TABLE Child (
                                            Id INT PRIMARY KEY,
                                            ParentId INT NULL
                                        );";
                command.ExecuteNonQuery();
                command.CommandText = @"ALTER TABLE Parent ADD CONSTRAINT (FOREIGN KEY (ChildId) REFERENCES Child (Id) CONSTRAINT FK_Child)";
                command.ExecuteNonQuery();
                command.CommandText = @"ALTER TABLE Child ADD CONSTRAINT (FOREIGN KEY (ParentId) REFERENCES Parent (Id) CONSTRAINT FK_Parent)";
                command.ExecuteNonQuery();

                for (int i = 0; i < 100; i++)
                {
                    command.Parameters.Add(new DB2Parameter("Value1", i));
                    command.Parameters.Add(new DB2Parameter("Value2", DBNull.Value));

                    command.CommandText = "INSERT INTO Parent VALUES (?, ?);";
                    command.ExecuteNonQuery();

                    command.CommandText = "INSERT INTO Child VALUES (?, ?);";
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                command.CommandText = @"UPDATE Parent SET ChildId = 0";
                command.ExecuteNonQuery();
                command.CommandText = @"UPDATE Child SET ParentId = 1";
                command.ExecuteNonQuery();

                command.CommandText = "SELECT COUNT(1) FROM Parent";
                command.ExecuteScalar().ShouldBe(100);
                command.CommandText = "SELECT COUNT(1) FROM Child";
                command.ExecuteScalar().ShouldBe(100);

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { "informix" }
                };
                try
                {
                    await checkPoint.Reset(_connection);
                }
                catch
                {
                    _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                    throw;
                }

                command.CommandText = "SELECT COUNT(1) FROM Parent";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM Child";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldHandleSelfRelationships()
        {
            using (var command = new DB2Command(@"DROP TABLE IF EXISTS Foo; 
                                                  CREATE TABLE Foo (
                                                      Id INT PRIMARY KEY,
                                                      ParentId INT NULL
                                                  );", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE Foo ADD CONSTRAINT (FOREIGN KEY (ParentId) REFERENCES Foo (Id) CONSTRAINT FK_Parent1)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO Foo (Id) VALUES (?)";
                command.Parameters.Add(new DB2Parameter("Value", 1));
                command.ExecuteNonQuery();
                command.Parameters.Clear();

                for (int i = 1; i < 100; i++)
                {
                    command.CommandText = "INSERT INTO Foo VALUES (?, ?)";
                    command.Parameters.Add(new DB2Parameter("Value1", i + 1));
                    command.Parameters.Add(new DB2Parameter("Value2", i));
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                command.CommandText = "SELECT COUNT(1) FROM Foo";
                command.ExecuteScalar().ShouldBe(100);

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { "informix" }
                };
                try
                {
                    await checkPoint.Reset(_connection);
                }
                catch
                {
                    _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                    throw;
                }

                command.CommandText = "SELECT COUNT(1) FROM Foo";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldHandleComplexCycles()
        {
            using (var command = new DB2Command("DROP TABLE IF EXISTS A; CREATE TABLE A (Id INT PRIMARY KEY, B_Id INT NULL)", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = "DROP TABLE IF EXISTS B; CREATE TABLE B (Id INT PRIMARY KEY, A_Id INT NULL, C_Id INT NULL, D_Id INT NULL)";
                command.ExecuteNonQuery();
                command.CommandText = "DROP TABLE IF EXISTS C; CREATE TABLE C (Id INT PRIMARY KEY, D_Id INT NULL)";
                command.ExecuteNonQuery();
                command.CommandText = "DROP TABLE IF EXISTS D; CREATE TABLE D (Id INT PRIMARY KEY)";
                command.ExecuteNonQuery();
                command.CommandText = "DROP TABLE IF EXISTS E; CREATE TABLE E (Id INT PRIMARY KEY, A_Id INT NULL)";
                command.ExecuteNonQuery();
                command.CommandText = "DROP TABLE IF EXISTS F; CREATE TABLE F (Id INT PRIMARY KEY, B_Id INT NULL)";
                command.ExecuteNonQuery();

                command.CommandText = "ALTER TABLE A ADD CONSTRAINT (FOREIGN KEY (B_Id) REFERENCES B (Id) CONSTRAINT FK_A_B)";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE B ADD CONSTRAINT (FOREIGN KEY (A_Id) REFERENCES A (Id) CONSTRAINT FK_B_A)";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE B ADD CONSTRAINT (FOREIGN KEY (C_Id) REFERENCES C (Id) CONSTRAINT FK_B_C)";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE B ADD CONSTRAINT (FOREIGN KEY (D_Id) REFERENCES D (Id) CONSTRAINT FK_B_D)";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE C ADD CONSTRAINT (FOREIGN KEY (D_Id) REFERENCES D (Id) CONSTRAINT FK_C_D)";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE E ADD CONSTRAINT (FOREIGN KEY (A_Id) REFERENCES A (Id) CONSTRAINT FK_E_A)";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER TABLE F ADD CONSTRAINT (FOREIGN KEY (B_Id) REFERENCES B (Id) CONSTRAINT FK_F_B)";
                command.ExecuteNonQuery();

                command.CommandText = "INSERT INTO D (Id) VALUES (1)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO C (Id, D_Id) VALUES (1, 1)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO A (Id) VALUES (1)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO B (Id, C_Id, D_Id) VALUES (1, 1, 1)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO E (Id, A_Id) VALUES (1, 1)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO F (Id, B_Id) VALUES (1, 1)";
                command.ExecuteNonQuery();
                command.CommandText = "UPDATE A SET B_Id = 1";
                command.ExecuteNonQuery();
                command.CommandText = "UPDATE B SET A_Id = 1";
                command.ExecuteNonQuery();

                command.CommandText = "SELECT COUNT(1) FROM A";
                command.ExecuteScalar().ShouldBe(1);
                command.CommandText = "SELECT COUNT(1) FROM B";
                command.ExecuteScalar().ShouldBe(1);
                command.CommandText = "SELECT COUNT(1) FROM C";
                command.ExecuteScalar().ShouldBe(1);
                command.CommandText = "SELECT COUNT(1) FROM D";
                command.ExecuteScalar().ShouldBe(1);
                command.CommandText = "SELECT COUNT(1) FROM E";
                command.ExecuteScalar().ShouldBe(1);
                command.CommandText = "SELECT COUNT(1) FROM F";
                command.ExecuteScalar().ShouldBe(1);

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { "informix" }
                };
                try
                {
                    await checkPoint.Reset(_connection);
                }
                catch
                {
                    _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                    throw;
                }

                command.CommandText = "SELECT COUNT(1) FROM A";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM B";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM C";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM D";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM E";
                command.ExecuteScalar().ShouldBe(0);
                command.CommandText = "SELECT COUNT(1) FROM F";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldExcludeSchemas()
        {
            const string user_1 = "a";
            const string user_2 = "b";

            await ManageUser(user_1);
            await ManageUser(user_2);
            using (var command = new DB2Command($"DROP TABLE IF EXISTS {user_1}.Foo; CREATE TABLE {user_1}.Foo (Value INT)", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = $"DROP TABLE IF EXISTS {user_2}.Bar; CREATE TABLE {user_2}.Bar (Value INT)";
                command.ExecuteNonQuery();

                for (int i = 0; i < 100; i++)
                {
                    command.Parameters.Add(new DB2Parameter("Value", i));
                    command.CommandText = $"INSERT INTO {user_1}.Foo VALUES (?)";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {user_2}.Bar VALUES (?)";
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToExclude = new[] { user_1 }
                };
                try
                {
                    await checkPoint.Reset(_connection);
                }
                catch
                {
                    _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                    throw;
                }

                command.CommandText = $"SELECT COUNT(1) FROM {user_1}.Foo";
                command.ExecuteScalar().ShouldBe(100);
                command.CommandText = $"SELECT COUNT(1) FROM {user_2}.Bar";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        [SkipOnAppVeyor]
        public async Task ShouldIncludeSchemas()
        {
            const string user_1 = "a";
            const string user_2 = "b";

            await ManageUser(user_1);
            await ManageUser(user_2);
            using (var command = new DB2Command($"DROP TABLE IF EXISTS {user_1}.Fooo; CREATE TABLE {user_1}.Fooo (Value INT)", _connection))
            {
                command.ExecuteNonQuery();
                command.CommandText = $"DROP TABLE IF EXISTS {user_2}.Baar; CREATE TABLE {user_2}.Baar (Value INT)";
                command.ExecuteNonQuery();

                for (int i = 0; i < 100; i++)
                {
                    command.Parameters.Add(new DB2Parameter("Value", i));
                    command.CommandText = $"INSERT INTO {user_1}.Fooo VALUES (?)";
                    command.ExecuteNonQuery();
                    command.CommandText = $"INSERT INTO {user_2}.Baar VALUES (?)";
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                var checkPoint = new Checkpoint
                {
                    DbAdapter = DbAdapter.Informix,
                    SchemasToInclude = new[] { user_2 }
                };
                try
                {
                    await checkPoint.Reset(_connection);
                }
                catch
                {
                    _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                    throw;
                }

                command.CommandText = $"SELECT COUNT(1) FROM {user_1}.Fooo";
                command.ExecuteScalar().ShouldBe(100);
                command.CommandText = $"SELECT COUNT(1) FROM {user_2}.Baar";
                command.ExecuteScalar().ShouldBe(0);
            }
        }

        private static async Task ManageUser(string userName)
        {
            using (var connection = new DB2Connection("Server=127.0.0.1:9089;Database=dummyifx;UID=informix;PWD=in4mix;Persist Security Info=True;Authentication=Server;"))
            {
                await connection.OpenAsync();
                using (var command = new DB2Command($@"SELECT username 
                                                      FROM sysusers
                                                      WHERE username = '{userName}';", connection))
                {
                    if (command.ExecuteScalar() != null)
                    {
                        command.CommandText = $"DROP USER {userName};";
                        command.ExecuteNonQuery();
                    }
                    command.CommandText = $"CREATE USER {userName} WITH PROPERTIES USER ifxsurr;";
                    command.ExecuteNonQuery();
                    command.CommandText = $"GRANT DBA TO {userName}";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
#endif