namespace Respawn.Graph
{
    public class Relationship
    {
        public Relationship()
        {
            
        }
        public Relationship(string primaryKeyTable, string foreignKeyTable, string name)
        {
            PrimaryKeyTable = primaryKeyTable;
            ForeignKeyTable = foreignKeyTable;
            Name = name;
        }

        public string PrimaryKeyTable { get; set; }
        public string ForeignKeyTable { get; set; }
        public string Name { get; set; }

        public override string ToString() => $"{PrimaryKeyTable} -> {ForeignKeyTable} [{Name}]";
    }
}