using System;
using System.Collections.Generic;
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
            var a = new Table("dbo", "A");
            var builder = new GraphBuilder
                (new HashSet<Table>(new[] {a}), 
                new HashSet<Relationship>(Enumerable.Empty<Relationship>()));
            
            builder.ToDelete.ToList().ShouldBe(new [] {a});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleUnrelatedTables()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var builder = new GraphBuilder(new HashSet<Table>(new[] {a, b}), new HashSet<Relationship>(Enumerable.Empty<Relationship>()));
            
            builder.ToDelete.ShouldBe(new [] {a, b});
        }

        [Fact]
        public void ShouldConstructDeleteListWithSingleRelatedTable()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var aToB = new Relationship(a, b, "A.B");
            var builder = new GraphBuilder(new HashSet<Table>(new[] {a, b}), new HashSet<Relationship>(new[] {aToB}));
            
            builder.ToDelete.ShouldBe(new [] {b, a});
        }

        [Fact]
        public void ShouldConstructDeleteListWithMoreComplexGraph()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var aToB = new Relationship(a, b, "A.B");
            var bToC = new Relationship(b, c, "B.C");
            var builder = new GraphBuilder(new HashSet<Table>(new[] {a, b, c}), new HashSet<Relationship>(new[] {aToB, bToC}));

            builder.ToDelete.ShouldBe(new[] { c, b, a });
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleRelationships()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var aToB = new Relationship(a, b, "A.B");
            var bToC = new Relationship(b, c, "B.C");
            var aToC = new Relationship(a, c, "A.C");
            var builder = new GraphBuilder(new HashSet<Table>(new[] {a, b, c}), new HashSet<Relationship>(new[] {aToB, bToC, aToC}));

            builder.ToDelete.ShouldBe(new[] { c, b, a });
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleEquivalentRelationships()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var aToB = new Relationship(a, b, "A.B");
            var bToC = new Relationship(b, c, "B.C");
            var aToC1 = new Relationship(a, c, "A.C1");
            var aToC2 = new Relationship(a, c, "A.C2");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b, c }), new HashSet<Relationship>(new[] { aToB, bToC, aToC1, aToC2 }));

            builder.ToDelete.ShouldBe(new[] { c, b, a });
        }

        [Fact]
        public void ShouldConstructDeleteListWithMultipleDisparateRelationships()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var d = new Table("dbo", "D");
            var aToB = new Relationship(a, b, "A.B");
            var cToD = new Relationship(c, d, "C.D");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b, c, d }), new HashSet<Relationship>(new[] {aToB, cToD}));

            builder.ToDelete.ShouldBe(new[] {b, a, d, c});
        }

        [Fact]
        public void ShouldRemoveCycles()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var aToB = new Relationship(a, b, "A.B");
            var bToA = new Relationship(b, a, "B.A");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b }), new HashSet<Relationship>(new[] {aToB, bToA}));

            builder.ToDelete.ShouldBe(new[] {a, b});
            builder.CyclicalTableRelationships.ShouldBe(new[] { bToA, aToB });
        }

        [Fact]
        public void ShouldIgnoreSelfReferences()
        {
            var a = new Table("dbo", "A");
            var aToA = new Relationship(a, a, "A.A");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a }), new HashSet<Relationship>(new[] {aToA}));

            builder.ToDelete.ShouldBe(new[] {a});
            builder.CyclicalTableRelationships.ShouldBeEmpty();
        }

        [Fact]
        public void ShouldRemoveCyclesExcludingNormalRelationships()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var d = new Table("dbo", "D");
            var aToB = new Relationship(a, b, "A.B");
            var bToA = new Relationship(b, a, "B.A");
            var cToD = new Relationship(c, d, "C.D");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b, c, d }), new HashSet<Relationship>(new[] { aToB, bToA, cToD }));

            builder.ToDelete.ShouldBe(new[] { a, b, d, c });
            builder.CyclicalTableRelationships.ShouldBe(new[] { bToA, aToB });
        }

        [Fact]
        public void ShouldRemoveCyclesWithoutRemovingStart()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var aToB = new Relationship(a, b, "A.B");
            var bToC = new Relationship(b, c, "B.C");
            var cToB = new Relationship(c, b, "C.B");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b, c }), new HashSet<Relationship>(new[] { aToB, bToC, cToB }));

            builder.ToDelete.ShouldBe(new[] { b, a, c });
            builder.CyclicalTableRelationships.ShouldBe(new[] { cToB, bToC });
        }

        [Fact]
        public void ShouldIsolateCycleRelationships()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var d = new Table("dbo", "D");
            var aToB = new Relationship(a, b, "A.B");
            var bToC = new Relationship(b, c, "B.C");
            var cToD = new Relationship(c, d, "C.D");
            var dToC = new Relationship(d, c, "D.C");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b, c, d }), new HashSet<Relationship>(new[] { aToB, bToC, cToD, dToC }));

            builder.ToDelete.ShouldBe(new[] { c, b, a, d });
            builder.CyclicalTableRelationships.ShouldBe(new[] { dToC, cToD });
        }

        [Fact]
        public void ShouldFindCyclicRelationships()
        {
            var a = new Table("dbo", "A");
            var b = new Table("dbo", "B");
            var c = new Table("dbo", "C");
            var d = new Table("dbo", "D");
            var e = new Table("dbo", "E");
            var f = new Table("dbo", "F");
            var aToB = new Relationship(a, b, "A.B");
            var bToA = new Relationship(b, a, "B.A");
            var bToC = new Relationship(b, c, "B.C");
            var bToD = new Relationship(b, d, "B.D");
            var cToD = new Relationship(c, d, "C.D");
            var eToA = new Relationship(e, a, "E.A");
            var fToB = new Relationship(f, b, "F.B");
            var builder = new GraphBuilder(new HashSet<Table>(new[] { a, b, c, d, e, f }), new HashSet<Relationship>(new[] { aToB, bToC, cToD, bToA, bToD, eToA, fToB }));

            builder.ToDelete.ShouldBe(new[] { a, d, c, b, e, f });
            builder.CyclicalTableRelationships.ShouldBe(new[] { bToA, aToB });
        }
    }
}
