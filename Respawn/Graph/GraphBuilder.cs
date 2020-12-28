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

            var (cyclicRelationships, toDelete) = FindAndRemoveCycles(tables);

            ToDelete = new ReadOnlyCollection<Table>(toDelete.ToList());

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

        private static (HashSet<Relationship> cyclicRelationships, Stack<Table> toDelete) 
            FindAndRemoveCycles(HashSet<Table> allTables)
        {
            var notVisited = new HashSet<Table>(allTables);
            var visiting = new HashSet<Table>();
            var visited = new HashSet<Table>();
            var cyclicRelationships = new HashSet<Relationship>();
            var toDelete = new Stack<Table>();

            foreach (var table in allTables)
            {
                HasCycles(table, notVisited, visiting, visited, toDelete, cyclicRelationships);
            }

            return (cyclicRelationships, toDelete);
        }

        private static bool HasCycles(Table table,
            HashSet<Table> notVisited,
            HashSet<Table> visiting,
            HashSet<Table> visited,
            Stack<Table> toDelete,
            HashSet<Relationship> cyclicalRelationships
            )
        {
            if (visited.Contains(table))
                return false;

            if (visiting.Contains(table))
                return true;

            notVisited.Remove(table);
            visiting.Add(table);

            foreach (var relationship in table.Relationships.Where(relationship => HasCycles(relationship.ReferencedTable, notVisited, visiting, visited, toDelete, cyclicalRelationships)))
            {
                cyclicalRelationships.Add(relationship);
            }

            visiting.Remove(table);
            visited.Add(table);
            toDelete.Push(table);

            return false;
        }
    }
}