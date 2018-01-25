using System;
using System.Linq;
using Respawn.Graph;
using Shouldly;
using Xunit;

namespace Respawn.UnitTests
{
    public class GraphTests
    {
        [Fact]
        public void ShouldConstructDeleteListWithOneTable()
        {
            var builder = new GraphBuilder(new[] {"A"}, Enumerable.Empty<Relationship>());
            
            builder.ToDelete.ShouldBe(new [] {"A"});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleUnrelatedTables()
        {
            var builder = new GraphBuilder(new[] {"A", "B"}, Enumerable.Empty<Relationship>());
            
            builder.ToDelete.ShouldBe(new [] {"A", "B"});
        }

        [Fact]
        public void ShouldConstructDeleteListWithSingleRelatedTable()
        {
            var builder = new GraphBuilder(new[] {"A", "B"}, new [] { new Relationship("A", "B", "") });
            
            builder.ToDelete.ShouldBe(new [] {"B", "A"});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMoreComplexGraph()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C"}, new [] { new Relationship("B", "C", ""), new Relationship("A", "B", "") });
            
            builder.ToDelete.ShouldBe(new [] {"C", "B", "A"});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleRelationships()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C"}, new [] { new Relationship("A", "C", ""), new Relationship("A", "B", ""), new Relationship("B", "C", "") });
            
            builder.ToDelete.ShouldBe(new [] {"C", "B", "A"});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleEquivalentRelationships()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C"}, new [] { new Relationship("A", "C", "Foo"), new Relationship("A", "C", "Bar"), new Relationship("A", "B", ""), new Relationship("B", "C", "") });
            
            builder.ToDelete.ShouldBe(new [] {"C", "B", "A"});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleDisparateRelationships()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C", "D"}, new [] { new Relationship("A", "B", ""), new Relationship("C", "D", "") });
            
            builder.ToDelete.ShouldBe(new [] {"B", "A", "D", "C"});
        }

        [Fact]
        public void ShouldRemoveCycles()
        {
            var builder = new GraphBuilder(new[] {"A", "B" }, new [] { new Relationship("A", "B", "A.B"), new Relationship("B", "A", "B.A") });
            
            builder.ToDelete.ShouldBeEmpty();
            builder.CyclicalTables.ShouldBe(new[] {"A", "B"});
            builder.CyclicalTableRelationships.ShouldBe(new[] {"A.B", "B.A"});
        }

        [Fact]
        public void ShouldRemoveCyclesExcludingNormalRelationships()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C", "D" }, new [] { new Relationship("A", "B", "A.B"), new Relationship("B", "A", "B.A"), new Relationship("C", "D", "C.D") });
            
            builder.ToDelete.ShouldBe(new[] {"D", "C"});
            builder.CyclicalTables.ShouldBe(new[] {"A", "B"});
            builder.CyclicalTableRelationships.ShouldBe(new[] {"A.B", "B.A"});
        }

        [Fact]
        public void ShouldRemoveCyclesWithoutRemovingStart()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C" }, new [] { new Relationship("A", "B", "A.B"), new Relationship("B", "C", "B.C"), new Relationship("C", "B", "C.B") });
            
            builder.ToDelete.ShouldBe(new[] {"A"});
            builder.CyclicalTables.ShouldBe(new[] {"B", "C"});
            builder.CyclicalTableRelationships.ShouldBe(new[] {"A.B", "B.C", "C.B"});
        }

        [Fact]
        public void ShouldIsolateCycleRelationships()
        {
            var builder = new GraphBuilder(new[] {"A", "B", "C", "D"}, new[]
            {
                new Relationship("A", "B", "A.B"),
                new Relationship("B", "C", "B.C"),
                new Relationship("C", "D", "C.D"),
                new Relationship("D", "C", "D.C")
            });
            
            builder.ToDelete.ShouldBe(new[] {"B", "A"});
            builder.CyclicalTables.ShouldBe(new[] {"C", "D"});
            builder.CyclicalTableRelationships.ShouldBe(new[] {"B.C", "C.D", "D.C"});
        }
    }
}
