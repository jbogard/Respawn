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

            var cyclicRelationships = FindAndRemoveCycles(tables);

            var toDelete = BuildDeleteList(tables);

            ToDelete = new ReadOnlyCollection<Table>(toDelete);

            CyclicalTableRelationships = new ReadOnlyCollection<Relationship>(cyclicRelationships.ToList());
        }

        public ReadOnlyCollection<Table> ToDelete { get; }
        public ReadOnlyCollection<Relationship> CyclicalTableRelationships { get; }

        private static void FillRelationships(HashSet<Table> tables, HashSet<Relationship> relationships)
        {
            foreach (var relationship in relationships)
            {
                var parentTable = tables.SingleOrDefault(t => t == relationship.ParentTable);
                var refTable = tables.SingleOrDefault(t => t == relationship.ReferencedTable);
                if (parentTable != null && refTable != null && parentTable != refTable)
                {
                    parentTable.Relationships.Add(new Relationship(parentTable, refTable, relationship.Name));
                }
            }
        }

        private static HashSet<Relationship> FindAndRemoveCycles(HashSet<Table> allTables)
        {
            var notVisited = new HashSet<Table>(allTables);
            var visiting = new HashSet<Table>();
            var visited = new HashSet<Table>();
            var path = new Stack<Relationship>();
            var cyclicRelationships = new HashSet<Relationship>();
            while (notVisited.Any())
            {
                var next = notVisited.First();
                path.Clear();

                while (HasCycles(next, notVisited, visiting, visited, path))
                {
                    var last = path.Pop();
                    cyclicRelationships.Add(last);
                    visiting.Remove(last.ParentTable);
                    last.ParentTable.Relationships.Remove(last);

                    Relationship previous;
                    do
                    {
                        previous = path.Pop();
                        cyclicRelationships.Add(previous);
                        visiting.Remove(previous.ParentTable);
                        previous.ParentTable.Relationships.Remove(previous);
                    } while (previous.ParentTable != last.ReferencedTable);

                    next = previous.ParentTable;
                }
            }

            return cyclicRelationships;
        }

        private static bool HasCycles(
            Table table,
            HashSet<Table> notVisited,
            HashSet<Table> visiting,
            HashSet<Table> visited,
            Stack<Relationship> path)
        {
            if (visited.Contains(table))
                return false;

            if (visiting.Contains(table))
                return true;

            notVisited.Remove(table);
            visiting.Add(table);

            foreach (var relationship in table.Relationships)
            {
                path.Push(relationship);

                if (HasCycles(relationship.ReferencedTable, notVisited, visiting, visited, path))
                    return true;

                path.Pop();
            }

            visiting.Remove(table);
            visited.Add(table);

            return false;
        }

        private static List<Table> BuildDeleteList(HashSet<Table> tables)
        {
            var toDelete = new List<Table>();
            var visited = new HashSet<Table>();

            foreach (var table in tables)
            {
                BuildTableList(table, visited, toDelete);
            }

            return toDelete;
        }

        private static void BuildTableList(Table table, HashSet<Table> visited, List<Table> toDelete)
        {
            if (visited.Contains(table))
                return;

            foreach (var rel in table.Relationships)
            {
                BuildTableList(rel.ReferencedTable, visited, toDelete);
            }

            toDelete.Add(table);
            visited.Add(table);
        }
    }
}