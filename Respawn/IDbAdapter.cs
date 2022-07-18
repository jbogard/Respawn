using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
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
        Task<bool> CheckSupportsTemporalTables(DbConnection connection)
        {
            return Task.FromResult(false);
        }
    }
}