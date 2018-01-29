using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Respawn.Graph
{
    public class GraphBuilder
    {
        public GraphBuilder(HashSet<Table> tables, HashSet<Relationship> relationships)
        {
            FillRelationships(tables, relationships);

            var cyclicTables = FindCycles(tables);

            var toDelete = BuildDeleteList(tables, cyclicTables);

            ToDelete = new ReadOnlyCollection<Table>(toDelete);
            CyclicalTables = new ReadOnlyCollection<Table>(cyclicTables.ToList());

            var cyclicalTableRelationships = (
                from relationship in relationships
                from cyclicTable in cyclicTables
                where relationship.PrimaryKeyTableName == cyclicTable.Name || relationship.ForeignKeyTableName == cyclicTable.Name
                select relationship
                ).Distinct().ToList();

            CyclicalTableRelationships = new ReadOnlyCollection<Relationship>(cyclicalTableRelationships);
        }

        public ReadOnlyCollection<Table> CyclicalTables { get; }

        public ReadOnlyCollection<Table> ToDelete { get; }

        public ReadOnlyCollection<Relationship> CyclicalTableRelationships { get; }

        private static void FillRelationships(HashSet<Table> tables, HashSet<Relationship> relationships)
        {
            foreach (var relationship in relationships)
            {
                var pkTable = tables.SingleOrDefault(t => t.Name == relationship.PrimaryKeyTableName);
                var fkTable = tables.SingleOrDefault(t => t.Name == relationship.ForeignKeyTableName);
                if (pkTable != null && fkTable != null && pkTable != fkTable)
                {
                    pkTable.Relationships.Add(fkTable);
                }
            }
        }

        private static HashSet<Table> FindCycles(HashSet<Table> allTables)
        {
            var notVisited = new HashSet<Table>(allTables);
            var visiting = new HashSet<Table>();
            var visited = new HashSet<Table>();
            var path = new Stack<Table>();
            var cyclicTables = new HashSet<Table>();
            while (notVisited.Any())
            {
                var next = notVisited.First();
                path.Clear();

                if (HasCycles(next, notVisited, visiting, visited, path))
                {
                    var detectedCycle = path.Pop();
                    var previous = path.Pop();
                    cyclicTables.Add(detectedCycle);
                    cyclicTables.Add(previous);
                    while (previous != detectedCycle)
                    {
                        previous = path.Pop();
                        cyclicTables.Add(previous);
                    }
                }

                // Remove cyclic tables
                visiting.ExceptWith(cyclicTables);

                // Add leftovers back to the fold
                notVisited.UnionWith(visiting);

                // Mark cyclic tables as visited
                visited.UnionWith(cyclicTables);
                visiting.Clear();
            }

            allTables.ExceptWith(cyclicTables);

            return cyclicTables;
        }

        private static bool HasCycles(
            Table table,
            HashSet<Table> notVisited,
            HashSet<Table> visiting,
            HashSet<Table> visited,
            Stack<Table> path)
        {
            notVisited.Remove(table);
            visiting.Add(table);
            path.Push(table);
            foreach (var relationship in table.Relationships)
            {
                if (visited.Contains(relationship))
                    continue;

                if (visiting.Contains(relationship))
                {
                    path.Push(relationship);
                    return true;
                }

                if (HasCycles(relationship, notVisited, visiting, visited, path))
                    return true;
            }

            visiting.Remove(table);
            visited.Add(table);
            path.Pop();

            return false;
        }

        private static List<Table> BuildDeleteList(HashSet<Table> tables, HashSet<Table> cyclicTables)
        {
            var toDelete = new List<Table>();

            var visited = new HashSet<Table>();
            visited.UnionWith(cyclicTables);

            BuildTableList(tables, visited, toDelete);
            return toDelete;
        }

        private static void BuildTableList(HashSet<Table> tables, HashSet<Table> visited, List<Table> toDelete)
        {
            foreach (var table in tables)
            {
                if (visited.Contains(table))
                    continue;

                BuildTableList(table.Relationships, visited, toDelete);

                toDelete.Add(table);
                visited.Add(table);
            }
        }
    }
}