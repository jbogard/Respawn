using System;

namespace Respawn.Graph
{
    public class Relationship : IEquatable<Relationship>
    {
        public Relationship(Table parentTable, Table referencedTable, string name)
        {
            ParentTable = parentTable;
            ReferencedTable = referencedTable;
            Name = name;
        }

        public Table ParentTable { get; }
        public Table ReferencedTable { get; }
        public string Name { get; }

        public override string ToString() => $"{ParentTable} -> {ReferencedTable} [{Name}]";

        public bool Equals(Relationship other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Relationship) obj);
        }

        public override int GetHashCode() => Name.GetHashCode();

        public static bool operator ==(Relationship left, Relationship right) => Equals(left, right);

        public static bool operator !=(Relationship left, Relationship right) => !Equals(left, right);
    }
}