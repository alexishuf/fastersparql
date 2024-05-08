package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PrefixMapTest {

    record D(Rope name, Rope unwrappedIri, Rope local, Rope before, Rope after) {
        public D(Object prefix, Object uri, Object local, Object before, Object after) {
            this(Rope.asRope(prefix), Rope.asRope(uri), Rope.asRope(local), Rope.asRope(before), Rope.asRope(after));
        }
        public D(String prefix, String uri) { this(prefix, uri, "", "", ""); }

        public Term complete() {
            try (var iri = PooledMutableRope.get()) {
                iri.append('<').append(unwrappedIri).append(local).append('>');
                return Term.valueOf(iri);
            }
        }
        public Term iriTerm() {
            try (var iri = PooledMutableRope.get()) {
                return Term.valueOf(iri.append('<').append(unwrappedIri).append('>'));
            }
        }
        public Rope iriPrefix() {
            return RopeFactory.make(1+unwrappedIri.len).add('<').add(unwrappedIri).take();
        }
        public Rope input() {
            return RopeFactory.make(before.len+name.len+1+local.len+after.len)
                              .add(before).add(name).add(':').add(local).add(after).take();
        }
        public int inputBegin() { return before.len(); }
        public int inputEnd() { return before.len() + name().len() + 1 + local().len(); }
    }

    static List<D> data() {
        List<D> base = List.of(
                new D("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
                new D("xsd", "http://www.w3.org/2001/XMLSchema#"),
                new D("foaf", "http://xmlns.com/foaf/0.1/"),
                new D("", "http://example.org/")
        );

        List<D> list = new ArrayList<>(base);
        for (String before : List.of("x", "\t\n")) {
            for (String after : List.of("x", ")", " ")) {
                for (String local : List.of("local", "1")) {
                    for (D b : base)
                        list.add(new D(b.name, b.unwrappedIri, local, before, after));
                }
            }
        }
        return list;
    }

    @Test
    void testEmpty() {
        try (var pmGuard = new Guard<PrefixMap>(this)) {
            var pm = pmGuard.set(PrefixMap.create());
            assertEquals(0, pm.size());
            assertNull(pm.expand(":"));
            assertNull(pm.expand("rdf:"));
            assertNull(pm.expand("rdf:int", 0, 3, 7));
        }
    }

    @Test
    void mapThenGetThenExpand() {
        for (D d : data()) {
            try (var pmGuard = new Guard<PrefixMap>(this);
                 var tmp = PooledMutableRope.get()) {
                var pm = pmGuard.set(PrefixMap.create());
                pm.add(d.name, d.iriPrefix());

                assertEquals(d.iriTerm(), pm.expand(d.name+":"));
                assertEquals(d.iriTerm(),
                             pm.expand(tmp.clear().append('x').append(d.name).append(":asd"),
                                       1, 1 + d.name.len() + 1));

                assertNull(pm.expand(d.name));
                assertNull(pm.expand(tmp.clear().append('x').append(d.name)));
                assertNull(pm.expand(tmp.clear().append(d.name).append('x')));
                assertNull(pm.expand(tmp.clear().append('x').append(d.name).append(':')));
                assertNull(pm.expand(tmp.clear().append(d.name).append("x:")));

                Rope input = d.input();
                int start = d.before.len();
                int colon = d.before.len() + d.name.len();
                int end = d.before.len() + d.name.len() + 1 + d.local.len();
                assertEquals(d.complete(), pm.expand(input, start, colon, end));

                assertNull(pm.expand(input.toString().replace(":", "::"), start, colon + 1, end + 1));
                assertNull(pm.expand(input.toString().replace(":", ""), start, colon - 1, end));
            }
        }
    }

    @Test
    void testBuiltin() {
        try (var pmGuard = new Guard<PrefixMap>(this)) {
            PrefixMap pm = pmGuard.set(PrefixMap.create()).resetToBuiltin();
            assertEquals(2, pm.size());

            assertEquals(Term.XSD_INT, pm.expand("xsd:int"));
            assertSame(Term.XSD_INT, pm.expand("xsd:int"));
            assertEquals(Term.RDF_HTML, pm.expand("rdf:HTML"));
            assertSame(Term.RDF_HTML, pm.expand("rdf:HTML"));


            pm.add("", "<http://example.org/");
            assertEquals(3, pm.size());
            assertEquals(Term.iri("http://example.org/local"),
                    pm.expand(":local ", 0, 6));
            assertEquals(Term.iri("http://example.org/local"), pm.expand(" :local ", 1, 7));

            assertSame(Term.RDF_TYPE, pm.expand("rdf:type"));
            assertSame(Term.XSD_ANYURI, pm.expand("xsd:anyURI"));

            pm.resetToBuiltin();
            assertEquals(2, pm.size());
            assertNull(pm.expand(":local", 0, 6));

            assertSame(Term.XSD_INT, pm.expand("xsd:int"));
            assertSame(Term.RDF_PROPERTY, pm.expand("rdf:Property"));
        }
    }

    @Test
    void testFill() {
        try (var pmGuard = new Guard<PrefixMap>(this);
             var tmp = PooledMutableRope.get()) {
            var pm = pmGuard.set(PrefixMap.create());
            Map<Rope, Term> prefix2uri = new HashMap<>();
            List<D> data = data();
            for (int row = 0; row < data.size(); row++) {
                D d = data.get(row);
                var ctx = "data[" + row + "]=" + d;
                pm.add(d.name, d.iriPrefix());
                prefix2uri.put(d.name, d.iriTerm());

                assertEquals(d.iriTerm(), pm.expand(tmp.clear().append(d.name).append(':')), ctx);
                assertEquals(d.complete(), pm.expand(d.input(), d.inputBegin(), d.inputEnd()), ctx);
            }
            for (var e : prefix2uri.entrySet()) {
                assertEquals(e.getValue(), pm.expand(tmp.clear().append(e.getKey()).append(':')));
                Term expected = Term.valueOf(
                        tmp.clear().append(e.getValue(), 0, e.getValue().length() - 1)
                                .append('1').append('>'));
                tmp.clear().append(',').append(e.getKey()).append(":1$");
                int start = 1, colon = e.getKey().len()+1, end = tmp.len()-1;
                assertEquals(e.getValue(), pm.expand(tmp, start, colon, colon+1));
                assertEquals(expected, pm.expand(tmp, start, colon, end));
            }
        }
    }

    @Test
    void testAddAll() {
        try (var leftGuard = new Guard<PrefixMap>(this);
             var rightGuard = new Guard<PrefixMap>(this)) {
            var left = leftGuard.set(PrefixMap.create()).resetToBuiltin();
            var right = rightGuard.set(PrefixMap.create());
            right.add("", "<http://example.org/");
            right.add("rdf", "<http://rdf.org/");
            left.addAll(right);
            assertEquals(Term.iri("http://example.org/"), left.expand(":"));
            assertEquals(Term.iri("http://rdf.org/"), left.expand("rdf:"));
            assertEquals(Term.XSD, left.expand("xsd:"));
            assertEquals(3, left.size());

            assertEquals(Term.iri("http://example.org/"), right.expand(":"));
            assertEquals(Term.iri("http://rdf.org/"), right.expand("rdf:"));
            assertEquals(2, right.size());
        }
    }

    @Test
    void testAngleBracketsAndMutationTolerance() {
        try (var pmGuard = new Guard<PrefixMap>(this);
             var termName = PooledMutableRope.get().append("term");
             var unwrappedName = PooledMutableRope.get().append("unwrapped");
             var wrappedName = PooledMutableRope.get().append("wrapped");
             var leadingName = PooledMutableRope.get().append("leading");
             var trailingName = PooledMutableRope.get().append("trailing");
             var wrapped = PooledMutableRope.get().append("<http://wrapped.example.org/>");
             var leading = PooledMutableRope.get().append("<http://leading.example.org/")) {
            var pm = pmGuard.set(PrefixMap.create());
            Term term = Term.iri("http://term.example.org/");
            assertEquals('<', term.get(0));
            assertEquals('>', term.get(term.len()-1));

            pm.add(termName, term);
            pm.add(wrappedName, wrapped);
            pm.add(leadingName, leading);
            assertEquals(pm.size(), 3);

            assertEquals(term, pm.expand("term:"));
            assertSame(term, pm.expand("term:"));
            assertEquals(Term.iri(wrapped), pm.expand("wrapped:"));
            assertEquals(Term.iri(leading), pm.expand("leading:"));

            // changes to ByteRopes fed to add() should not affect PrefixMap
            Arrays.fill(unwrappedName.u8(), (byte)0);
            Arrays.fill(wrappedName.u8(), (byte)0);
            Arrays.fill(leadingName.u8(), (byte)0);
            Arrays.fill(trailingName.u8(), (byte)0);
            Arrays.fill(wrapped.u8(), (byte)0);
            Arrays.fill(leading.u8(), (byte)0);

            assertEquals(Term.iri("http://wrapped.example.org/"), pm.expand("wrapped:"));
            assertEquals(Term.iri("http://leading.example.org/"), pm.expand("leading:"));

            assertEquals(Term.iri("http://wrapped.example.org/local"), pm.expand("wrapped:local"));
            assertEquals(Term.iri("http://leading.example.org/local"), pm.expand("leading:local"));
        }

    }
}