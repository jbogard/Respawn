using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Respawn;
using Respawn.Graph;
using Shouldly;
using Xunit;

namespace Respawn.DatabaseTests
{
  public class CustomSqliteAdapter : IDbAdapter
  {
    public string BuildDeleteCommandText(GraphBuilder graph, RespawnerOptions options)
        => "DELETE FROM TestTable;";
    public string BuildReseedSql(System.Collections.Generic.IEnumerable<Table> tables) => null;
    public string BuildTableCommandText(RespawnerOptions options) => "SELECT NULL, name FROM sqlite_master WHERE type='table' AND name='TestTable';";
    public string BuildRelationshipCommandText(RespawnerOptions options) => "SELECT NULL, 'TestTable', NULL, 'TestTable', 'PK_TestTable';";
    public string BuildTemporalTableCommandText(RespawnerOptions options) => "SELECT NULL, NULL, NULL, NULL;";
    public string BuildTurnOffSystemVersioningCommandText(System.Collections.Generic.IEnumerable<TemporalTable> tables) => string.Empty;
    public string BuildTurnOnSystemVersioningCommandText(System.Collections.Generic.IEnumerable<TemporalTable> tables) => string.Empty;
    public Task<bool> CheckSupportsTemporalTables(DbConnection connection) => Task.FromResult(false);
  }

  public class SqliteCustomAdapterTests
  {
    [Fact]
    public async Task ShouldResetTableWithCustomAdapter()
    {
      var connectionString = "Data Source=:memory:";
      await using var conn = new SqliteConnection(connectionString);
      await conn.OpenAsync();

      var createTable = conn.CreateCommand();
      createTable.CommandText = "CREATE TABLE TestTable (Id INTEGER PRIMARY KEY, Name TEXT);";
      await createTable.ExecuteNonQueryAsync();

      var insert = conn.CreateCommand();
      insert.CommandText = "INSERT INTO TestTable (Name) VALUES ('foo');";
      await insert.ExecuteNonQueryAsync();

      // Confirm row exists
      var countCmd = conn.CreateCommand();
      countCmd.CommandText = "SELECT COUNT(*) FROM TestTable;";
      var countBefore = (long)await countCmd.ExecuteScalarAsync();
      countBefore.ShouldBe(1);

      // Use custom adapter
      var respawner = await Respawner.CreateAsync(conn, new CustomSqliteAdapter());
      await respawner.ResetAsync(conn);

      // Confirm table is empty
      var countAfter = (long)await countCmd.ExecuteScalarAsync();
      countAfter.ShouldBe(0);
    }
  }
}
