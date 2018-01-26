using System;

namespace Respawn.Graph
{
    public class Relationship : IEquatable<Relationship>
    {
        public Relationship(string primaryKeyTableSchema, string primaryKeyTableName, string foreignKeyTableSchema, string foreignKeyTableName, string name)
        {
            PrimaryKeyTableSchema = primaryKeyTableSchema;
            PrimaryKeyTableName = primaryKeyTableName;
            PrimaryKeyTable = new Table(primaryKeyTableSchema, primaryKeyTableName);
            ForeignKeyTableSchema = foreignKeyTableSchema;
            ForeignKeyTableName = foreignKeyTableName;
            ForeignKeyTable = new Table(foreignKeyTableSchema, foreignKeyTableName);
            Name = name;
        }

        public string PrimaryKeyTableSchema { get; }
        public string PrimaryKeyTableName { get; }
        public Table PrimaryKeyTable { get; }
        public string ForeignKeyTableSchema { get; }
        public string ForeignKeyTableName { get; }
        public Table ForeignKeyTable { get; }
        public string Name { get; }

        public override string ToString() => $"{PrimaryKeyTableSchema}.{PrimaryKeyTableName} -> {ForeignKeyTableSchema}.{ForeignKeyTableName} [{Name}]";

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

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public static bool operator ==(Relationship left, Relationship right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Relationship left, Relationship right)
        {
            return !Equals(left, right);
        }
    }
}