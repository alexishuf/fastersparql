package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PrefixMapTest {

    record D(Rope name, Rope iri, Rope local, Rope before, Rope after) {
        public D(Object prefix, Object uri, Object local, Object before, Object after) {
            this(new ByteRope(prefix), new ByteRope(uri), new ByteRope(local),
                 new ByteRope(before), new ByteRope(after));
        }
        public D(String prefix, String uri) { this(prefix, uri, "", "", ""); }

        public Term complete() { return Term.valueOf(Rope.of('<', iri, local, '>')); }
        public Term iriTerm() { return Term.valueOf(Rope.of('<', iri, '>')); }
        public Rope input() { return Rope.of(before, name, ":", local, after); }
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
                        list.add(new D(b.name, b.iri, local, before, after));
                }
            }
        }
        return list;
    }

    @Test
    void testEmpty() {
        PrefixMap pm = new PrefixMap();
        assertEquals(0, pm.size());
        assertNull(pm.expand(":"));
        assertNull(pm.expand("rdf:"));
        assertNull(pm.expand("rdf:int", 0, 3, 7));
    }

    @Test
    void mapThenGetThenExpand() {
        for (D d : data()) {
            PrefixMap pm = new PrefixMap();
            pm.add(d.name, d.iri);

            assertEquals(d.iriTerm(), pm.expand(Rope.of(d.name, ":")));
            assertEquals(d.iriTerm(), pm.expand(Rope.of('x', d.name, ":asd"), 1, 1+d.name.len()+1));

            assertNull(pm.expand(d.name));
            assertNull(pm.expand(Rope.of('x', d.name)));
            assertNull(pm.expand(Rope.of(d.name, 'x')));
            assertNull(pm.expand(Rope.of('x', d.name, ':')));
            assertNull(pm.expand(Rope.of(d.name, "x:")));

            Rope input = d.input();
            int start = d.before.len();
            int colon = d.before.len()+d.name.len();
            int end = d.before.len()+d.name.len()+1+d.local.len();
            assertEquals(d.complete(), pm.expand(input, start, colon, end));

            assertNull(pm.expand(input.toString().replace(":", "::"), start, colon+1, end+1));
            assertNull(pm.expand(input.toString().replace(":", ""), start, colon-1, end));
        }
    }

    @Test
    void testBuiltin() {
        PrefixMap pm = new PrefixMap().resetToBuiltin();
        assertEquals(2, pm.size());

        assertEquals(Term.XSD_INT, pm.expand("xsd:int"));
        assertSame(Term.XSD_INT, pm.expand("xsd:int"));
        assertEquals(Term.RDF_HTML, pm.expand("rdf:HTML"));
        assertSame(Term.RDF_HTML, pm.expand("rdf:HTML"));


        pm.add("", "http://example.org/");
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

    @Test
    void testFill() {
        Map<Rope, Term> prefix2uri = new HashMap<>();
        var pm = new PrefixMap();
        List<D> data = data();
        for (int row = 0; row < data.size(); row++) {
            D d = data.get(row);
            var ctx = "data[" + row + "]=" + d;
            pm.add(d.name, d.iri);
            prefix2uri.put(d.name, d.iriTerm());

            assertEquals(d.iriTerm(), pm.expand(Rope.of(d.name, ':')), ctx);
            assertEquals(d.complete(), pm.expand(d.input(), d.inputBegin(), d.inputEnd()), ctx);
        }
        for (var e : prefix2uri.entrySet()) {
            assertEquals(e.getValue(), pm.expand(Rope.of(e.getKey(), ':')));
            Rope in = Rope.of(',', e.getKey(), ":1$");
            Term expected = Term.valueOf(
                    new ByteRope().append(e.getValue(), 0, e.getValue().length() - 1)
                                  .append('1').append('>'));
            int start = 1, colon = e.getKey().len()+1, end = in.len()-1;
            assertEquals(e.getValue(), pm.expand(in, start, colon, colon+1));
            assertEquals(expected, pm.expand(in, start, colon, end));
        }
    }

    @Test
    void testAddAll() {
        PrefixMap left = new PrefixMap().resetToBuiltin(), right = new PrefixMap();
        right.add("", "http://example.org/");
        right.add("rdf", "http://rdf.org/");
        left.addAll(right);
        assertEquals(Term.iri("http://example.org/"), left.expand(":"));
        assertEquals(Term.iri("http://rdf.org/"), left.expand("rdf:"));
        assertEquals(Term.XSD, left.expand("xsd:"));
        assertEquals(3, left.size());

        assertEquals(Term.iri("http://example.org/"), right.expand(":"));
        assertEquals(Term.iri("http://rdf.org/"), right.expand("rdf:"));
        assertEquals(2, right.size());
    }

    @Test
    void testAngleBracketsAndMutationTolerance() {
        PrefixMap pm = new PrefixMap();

        ByteRope termName = new ByteRope("term");
        Term term = Term.iri("http://term.example.org/");
        assertEquals('<', term.get(0));
        assertEquals('>', term.get(term.len()-1));

        ByteRope unwrappedName = new ByteRope("unwrapped");
        ByteRope wrappedName = new ByteRope("wrapped");
        ByteRope leadingName = new ByteRope("leading");
        ByteRope trailingName = new ByteRope("trailing");
        ByteRope unwrapped = new ByteRope("http://unwrapped.example.org/");
        ByteRope   wrapped = new ByteRope("<http://wrapped.example.org/>");
        ByteRope   leading = new ByteRope("<http://leading.example.org/");
        ByteRope  trailing = new ByteRope("http://trailing.example.org/>");

        pm.add(termName, term);
        pm.add(unwrappedName, unwrapped);
        pm.add(wrappedName, wrapped);
        pm.add(leadingName, leading);
        pm.add(trailingName, trailing);
        assertEquals(pm.size(), 5);

        assertEquals(term, pm.expand("term:"));
        assertSame(term, pm.expand("term:"));
        assertEquals(Term.iri(unwrapped), pm.expand("unwrapped:"));
        assertEquals(Term.iri(wrapped), pm.expand("wrapped:"));
        assertEquals(Term.iri(leading), pm.expand("leading:"));
        assertEquals(Term.iri(trailing), pm.expand("trailing:"));

        // changes to ByteRopes fed to add() should not affect PrefixMap
        Arrays.fill(unwrappedName.utf8, (byte)0);
        Arrays.fill(wrappedName.utf8, (byte)0);
        Arrays.fill(leadingName.utf8, (byte)0);
        Arrays.fill(trailingName.utf8, (byte)0);
        Arrays.fill(unwrapped.utf8, (byte)0);
        Arrays.fill(wrapped.utf8, (byte)0);
        Arrays.fill(leading.utf8, (byte)0);
        Arrays.fill(trailing.utf8, (byte)0);

        assertEquals(Term.iri("http://unwrapped.example.org/"), pm.expand("unwrapped:"));
        assertEquals(Term.iri("http://wrapped.example.org/"), pm.expand("wrapped:"));
        assertEquals(Term.iri("http://leading.example.org/"), pm.expand("leading:"));
        assertEquals(Term.iri("http://trailing.example.org/"), pm.expand("trailing:"));

        assertEquals(Term.iri("http://unwrapped.example.org/local"), pm.expand("unwrapped:local"));
        assertEquals(Term.iri("http://wrapped.example.org/local"), pm.expand("wrapped:local"));
        assertEquals(Term.iri("http://leading.example.org/local"), pm.expand("leading:local"));
        assertEquals(Term.iri("http://trailing.example.org/local"), pm.expand("trailing:local"));
    }
}