namespace Respawn.Graph
{
    public record Relationship(Table ParentTable, Table ReferencedTable, string Name);
}