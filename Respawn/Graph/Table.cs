using System;
using System.Collections.Generic;

namespace Respawn.Graph
{
    public class Table : IEquatable<Table>
    {
        public Table(string name) : this(null, name)
        {
            
        }

        public Table(string? schema, string name)
        {
            Schema = schema;
            Name = name;
        }

        public string? Schema { get; }
        public string Name { get; }

        public HashSet<Relationship> Relationships { get; } = new();

        public string GetFullName(char quoteIdentifier) =>
            Schema == null
                ? $"{quoteIdentifier}{Name}{quoteIdentifier}"
                : $"{quoteIdentifier}{Schema}{quoteIdentifier}.{quoteIdentifier}{Name}{quoteIdentifier}";

        public static implicit operator Table(string name) => new(name);

        public bool Equals(Table? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Schema == other.Schema && Name == other.Name;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Table) obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Schema, Name);
        }

        public static bool operator ==(Table? left, Table? right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Table? left, Table? right)
        {
            return !Equals(left, right);
        }

        public override string ToString() => $"{Schema}.{Name}";
    }
}