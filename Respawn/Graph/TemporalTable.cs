namespace Respawn.Graph
{
    public class TemporalTable
    {
        public TemporalTable(string schema, string name, string historyTableSchema, string historyTableName)
        {
            Schema = schema;
            Name = name;
            HistoryTableSchema = historyTableSchema;
            HistoryTableName = historyTableName;
        }

        public string Schema { get; }
        public string Name { get; }
        public string HistoryTableSchema { get; }
        public string HistoryTableName { get; }
    }
}
