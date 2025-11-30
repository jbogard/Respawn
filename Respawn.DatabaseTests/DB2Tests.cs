using Shouldly;
using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Images;
using IBM.Data.Db2;
using NPoco;
using Respawn.Graph;
using Testcontainers.Db2;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.DatabaseTests
{
    public class DB2Tests : IAsyncLifetime
    {
        private Db2Container _sqlContainer;
        private DB2Connection _connection;
        private readonly ITestOutputHelper _output;

        public DB2Tests(ITestOutputHelper output)
        {
            _output = output;
        }

        public async Task InitializeAsync()
        {
            _sqlContainer = new Db2Builder()
                .WithAcceptLicenseAgreement(true)
                .Build();
            await _sqlContainer.StartAsync();
            
            _connection = new DB2Connection(_sqlContainer.GetConnectionString());

            await _connection.OpenAsync();
        }

        public async Task DisposeAsync()
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;

            await _sqlContainer.StopAsync();
            await _sqlContainer.DisposeAsync();
            _sqlContainer = null;
        }

        [SkipOnCI]
        public async Task ShouldDeleteData()
        {
            await using var command = new DB2Command("DROP TABLE IF EXISTS Foo; CREATE TABLE Foo (Value INT);", _connection);

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

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { "DB2INST1" }
            });
            await checkPoint.ResetAsync(_connection);

            command.ExecuteScalar().ShouldBe(0);

            // Cleanup
            command.CommandText = "DROP TABLE IF EXISTS Foo;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldIgnoreTables()
        {
            await using var command = new DB2Command("DROP TABLE IF EXISTS Foo; CREATE TABLE Foo (Value INT);", _connection);
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
            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions()
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { "DB2INST1" },
                TablesToIgnore = new Table[] { "Foo" }
            });
            await checkPoint.ResetAsync(_connection);

            command.CommandText = "SELECT COUNT(1) FROM Foo";
            command.ExecuteScalar().ShouldBe(100);
            command.CommandText = "SELECT COUNT(1) FROM Bar";
            command.ExecuteScalar().ShouldBe(0);

            // Cleanup
            command.CommandText = "DROP TABLE IF EXISTS Foo;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS Bar;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldHandleRelationships()
        {
            await using var command = new DB2Command("DROP TABLE IF EXISTS Foo; CREATE TABLE Foo (Value INT NOT NULL PRIMARY KEY);", _connection);
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

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { "DB2INST1" }
            });
            try
            {
                await checkPoint.ResetAsync(_connection);
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

            // Cleanup
            command.CommandText = "DROP TABLE IF EXISTS Foo;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS Bar;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldHandleCircularRelationships()
        {
            await using var command = new DB2Command(@"DROP TABLE IF EXISTS Parent;
                                                  CREATE TABLE Parent (
                                                      Id INT NOT NULL PRIMARY KEY,
                                                      ChildId INT NULL
                                                  );", _connection);
            command.ExecuteNonQuery();
            command.CommandText = @"DROP TABLE IF EXISTS Child; 
                                        CREATE TABLE Child (
                                            Id INT NOT NULL PRIMARY KEY,
                                            ParentId INT NULL
                                        );";
            command.ExecuteNonQuery();
            command.CommandText = @"ALTER TABLE Parent ADD CONSTRAINT FK_Child FOREIGN KEY (ChildId) REFERENCES Child (Id)";
            command.ExecuteNonQuery();
            command.CommandText = @"ALTER TABLE Child ADD CONSTRAINT FK_Parent FOREIGN KEY (ParentId) REFERENCES Parent (Id)";
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

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { "DB2INST1" }
            });
            try
            {
                await checkPoint.ResetAsync(_connection);
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

            // Cleanup
            command.CommandText = "DROP TABLE IF EXISTS Parent;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS Child;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldHandleSelfRelationships()
        {
            await using var command = new DB2Command(@"DROP TABLE IF EXISTS Foo; 
                                                  CREATE TABLE Foo (
                                                      Id INT NOT NULL PRIMARY KEY,
                                                      ParentId INT NULL
                                                  );", _connection);
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE Foo ADD CONSTRAINT FK_Parent1 FOREIGN KEY (ParentId) REFERENCES Foo (Id)";
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

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { "DB2INST1" }
            });
            try
            {
                await checkPoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                throw;
            }

            command.CommandText = "SELECT COUNT(1) FROM Foo";
            command.ExecuteScalar().ShouldBe(0);

            // Cleanup
            command.CommandText = "DROP TABLE IF EXISTS Foo;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldHandleComplexCycles()
        {
            await using var command = new DB2Command("DROP TABLE IF EXISTS A; CREATE TABLE A (Id INT NOT NULL PRIMARY KEY, B_Id INT NULL)", _connection);
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS B; CREATE TABLE B (Id INT NOT NULL PRIMARY KEY, A_Id INT NULL, C_Id INT NULL, D_Id INT NULL)";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS C; CREATE TABLE C (Id INT NOT NULL PRIMARY KEY, D_Id INT NULL)";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS D; CREATE TABLE D (Id INT NOT NULL PRIMARY KEY)";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS E; CREATE TABLE E (Id INT NOT NULL PRIMARY KEY, A_Id INT NULL)";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS F; CREATE TABLE F (Id INT NOT NULL PRIMARY KEY, B_Id INT NULL)";
            command.ExecuteNonQuery();

            command.CommandText = "ALTER TABLE A ADD CONSTRAINT FK_A_B FOREIGN KEY (B_Id) REFERENCES B (Id)";
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE B ADD CONSTRAINT FK_B_A FOREIGN KEY (A_Id) REFERENCES A (Id)";
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE B ADD CONSTRAINT FK_B_C FOREIGN KEY (C_Id) REFERENCES C (Id)";
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE B ADD CONSTRAINT FK_B_D FOREIGN KEY (D_Id) REFERENCES D (Id)";
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE C ADD CONSTRAINT FK_C_D FOREIGN KEY (D_Id) REFERENCES D (Id)";
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE E ADD CONSTRAINT FK_E_A FOREIGN KEY (A_Id) REFERENCES A (Id)";
            command.ExecuteNonQuery();
            command.CommandText = "ALTER TABLE F ADD CONSTRAINT FK_F_B FOREIGN KEY (B_Id) REFERENCES B (Id)";
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

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { "DB2INST1" }
            });
            try
            {
                await checkPoint.ResetAsync(_connection);
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

            // Cleanup
            command.CommandText = "DROP TABLE IF EXISTS A;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS B;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS C;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS D;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS E;";
            command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS F;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldExcludeSchemas()
        {
            const string schema_1 = "schema1";
            const string schema_2 = "schema2";

            var database = new Database(_connection);
            await database.ExecuteAsync($"DROP TABLE IF EXISTS {schema_1}.Foo;");
            await database.ExecuteAsync($"DROP TABLE IF EXISTS {schema_2}.Bar;");

            await CreateSchema(schema_1);
            await CreateSchema(schema_2);
            await using var command = new DB2Command($"DROP TABLE IF EXISTS {schema_1}.Foo; CREATE TABLE {schema_1}.Foo (Value INT)", _connection);
            command.ExecuteNonQuery();
            command.CommandText = $"DROP TABLE IF EXISTS {schema_2}.Bar; CREATE TABLE {schema_2}.Bar (Value INT)";
            command.ExecuteNonQuery();

            for (int i = 0; i < 100; i++)
            {
                command.Parameters.Add(new DB2Parameter("Value", i));
                command.CommandText = $"INSERT INTO {schema_1}.Foo VALUES (?)";
                command.ExecuteNonQuery();
                command.CommandText = $"INSERT INTO {schema_2}.Bar VALUES (?)";
                command.ExecuteNonQuery();
                command.Parameters.Clear();
            }

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToExclude = new[] { schema_1 }
            });
            try
            {
                await checkPoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                throw;
            }

            command.CommandText = $"SELECT COUNT(1) FROM {schema_1}.Foo";
            command.ExecuteScalar().ShouldBe(100);
            command.CommandText = $"SELECT COUNT(1) FROM {schema_2}.Bar";
            command.ExecuteScalar().ShouldBe(0);

            // Cleanup
            command.CommandText = $"DROP TABLE IF EXISTS {schema_1}.Foo;";
            command.ExecuteNonQuery();
            command.CommandText = $"DROP TABLE IF EXISTS {schema_2}.Bar;";
            command.ExecuteNonQuery();

            command.CommandText = $"DROP SCHEMA {schema_1} RESTRICT;";
            command.ExecuteNonQuery();
            command.CommandText = $"DROP SCHEMA {schema_2} RESTRICT;";
            command.ExecuteNonQuery();
        }

        [SkipOnCI]
        public async Task ShouldIncludeSchemas()
        {
            const string schema_1 = "schema1";
            const string schema_2 = "schema2";

            await using var command = new DB2Command($"DROP TABLE IF EXISTS {schema_1}.Foo;", _connection);
            command.ExecuteNonQuery();
            command.CommandText = $"DROP TABLE IF EXISTS {schema_2}.Bar;";
            command.ExecuteNonQuery();

            await CreateSchema(schema_1);
            await CreateSchema(schema_2);
            command.CommandText = $"CREATE TABLE {schema_1}.Foo (Value INT)";
            command.ExecuteNonQuery();
            command.CommandText = $"CREATE TABLE {schema_2}.Bar (Value INT)";
            command.ExecuteNonQuery();

            for (int i = 0; i < 100; i++)
            {
                command.Parameters.Add(new DB2Parameter("Value", i));
                command.CommandText = $"INSERT INTO {schema_1}.Foo VALUES (?)";
                command.ExecuteNonQuery();
                command.CommandText = $"INSERT INTO {schema_2}.Bar VALUES (?)";
                command.ExecuteNonQuery();
                command.Parameters.Clear();
            }

            var checkPoint = await Respawner.CreateAsync(_connection, new RespawnerOptions
            {
                DbAdapter = DbAdapter.DB2,
                SchemasToInclude = new[] { schema_2 }
            });
            try
            {
                await checkPoint.ResetAsync(_connection);
            }
            catch
            {
                _output.WriteLine(checkPoint.DeleteSql ?? string.Empty);
                throw;
            }

            command.CommandText = $"SELECT COUNT(1) FROM {schema_1}.Foo";
            command.ExecuteScalar().ShouldBe(100);
            command.CommandText = $"SELECT COUNT(1) FROM {schema_2}.Bar";
            command.ExecuteScalar().ShouldBe(0);

            // Cleanup
            command.CommandText = $"DROP TABLE IF EXISTS {schema_1}.Foo;";
            command.ExecuteNonQuery();
            command.CommandText = $"DROP TABLE IF EXISTS {schema_2}.Bar;";
            command.ExecuteNonQuery();

            command.CommandText = $"DROP SCHEMA {schema_1} RESTRICT;";
            command.ExecuteNonQuery();
            command.CommandText = $"DROP SCHEMA {schema_2} RESTRICT;";
            command.ExecuteNonQuery();
        }

        private async Task CreateSchema(string schemaName)
        {
            var database = new Database(_connection);

            try
            {
                await database.ExecuteAsync($"DROP SCHEMA {schemaName} RESTRICT;");
            }
            catch (DB2Exception)
            {
                // Ignore
            }

            await database.ExecuteAsync($"CREATE SCHEMA {schemaName} AUTHORIZATION db2inst1;");
        }
    }
}

//#endif
