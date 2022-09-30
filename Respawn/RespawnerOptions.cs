using Respawn.Graph;
using System;

namespace Respawn;

public class RespawnerOptions
{
    public Table[] TablesToIgnore { get; init; } = Array.Empty<Table>();
    public Table[] TablesToInclude { get; init; } = Array.Empty<Table>();
    public string[] SchemasToInclude { get; init; } = Array.Empty<string>();
    public string[] SchemasToExclude { get; init; } = Array.Empty<string>();
    public bool CheckTemporalTables { get; init; }
    public bool WithReseed { get; init; }
    public int? CommandTimeout { get; init; }
    public IDbAdapter DbAdapter { get; init; } = Respawn.DbAdapter.SqlServer;

}