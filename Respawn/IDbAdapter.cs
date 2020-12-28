using System.Collections.Generic;
using Respawn.Graph;

namespace Respawn
{
    public interface IDbAdapter
    {
        string BuildTableCommandText(Checkpoint checkpoint);
        string BuildTemporalTableCommandText(Checkpoint checkpoint);
        string BuildRelationshipCommandText(Checkpoint checkpoint);
        string BuildDeleteCommandText(GraphBuilder builder);
        string BuildReseedSql(IEnumerable<Table> tablesToDelete);
        string BuildTurnOffSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOffSystemVersioning);
        string BuildTurnOnSystemVersioningCommandText(IEnumerable<TemporalTable> tablesToTurnOnSystemVersioning);
        bool SupportsTemporalTables { get; }
    }
}