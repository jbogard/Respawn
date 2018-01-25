using System;
using System.Collections.Generic;

namespace Respawn.Graph
{
    public class Table : IEquatable<Table>, IComparable<Table>, IComparable
    {
        public Table(string name) => Name = name;

        public string Name { get; }

        public HashSet<Table> Relationships { get; } = new HashSet<Table>();

        public bool Equals(Table other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return String.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Table) obj);
        }

        public override int GetHashCode()
        {
            return StringComparer.OrdinalIgnoreCase.GetHashCode(Name);
        }

        public int CompareTo(Table other)
        {
            if (ReferenceEquals(this, other)) return 0;
            if (ReferenceEquals(null, other)) return 1;
            return String.Compare(Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int CompareTo(object obj)
        {
            if (ReferenceEquals(null, obj)) return 1;
            if (ReferenceEquals(this, obj)) return 0;
            if (!(obj is Table)) throw new ArgumentException($"Object must be of type {nameof(Table)}");
            return CompareTo((Table) obj);
        }

        public static bool operator ==(Table left, Table right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Table left, Table right)
        {
            return !Equals(left, right);
        }

        public override string ToString() => Name;
    }
}