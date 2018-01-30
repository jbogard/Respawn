using System;
using System.Collections.Generic;

namespace Respawn.Graph
{
    public class Table : IEquatable<Table>
    {
        public Table(string schema, string name)
        {
            Schema = schema;
            Name = name;
        }

        public string Schema { get; }
        public string Name { get; }

        public HashSet<Relationship> Relationships { get; } = new HashSet<Relationship>();

        public string GetFullName(char quoteIdentifier) =>
            Schema == null
                ? $"{quoteIdentifier}{Name}{quoteIdentifier}"
                : $"{quoteIdentifier}{Schema}{quoteIdentifier}.{quoteIdentifier}{Name}{quoteIdentifier}";

        public bool Equals(Table other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) && string.Equals(Schema, other.Schema);
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
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (Schema != null ? Schema.GetHashCode() : 0);
            }
        }

        public static bool operator ==(Table left, Table right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Table left, Table right)
        {
            return !Equals(left, right);
        }

        public override string ToString() => $"{Schema}.{Name}";
    }
}