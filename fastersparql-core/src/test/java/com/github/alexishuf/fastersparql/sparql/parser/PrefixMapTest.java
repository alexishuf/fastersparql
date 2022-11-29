package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.sparql.RDF;
import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PrefixMapTest {

    record D(String prefix, String uri, String local, String before, String after) {
        public D(String prefix, String uri) { this(prefix, uri, "", "", ""); }
    }

    static List<D> data() {
        List<D> base = List.of(
                new D("rdf", RDF.NS),
                new D("xsd", RDFTypes.XSD),
                new D("foaf", "http://xmlns.com/foaf/0.1/"),
                new D("", "http://example.org/")
        );

        List<D> list = new ArrayList<>(base);
        for (String before : List.of("x", "\t\n")) {
            for (String after : List.of("x", ")", " ")) {
                for (String local : List.of("local", "1")) {
                    for (D b : base)
                        list.add(new D(b.prefix, b.uri, local, before, after));
                }
            }
        }
        return list;
    }

    @Test
    void testEmpty() {
        PrefixMap pm = new PrefixMap();
        assertEquals(0, pm.size());
        assertNull(pm.uri(""));
        assertNull(pm.uri("rdf:"));
        assertNull(pm.expandPrefixed("rdf:int", 0, 3, 7));
    }

    @Test
    void mapThenGetThenExpand() {
        for (D d : data()) {
            PrefixMap pm = new PrefixMap();
            pm.add(d.prefix, d.uri);

            assertEquals(d.uri, pm.uri(d.prefix));
            assertEquals(d.uri, pm.uri('x'+d.prefix+":asd", 1, 1+d.prefix.length()));
            assertNull(pm.uri("x"+d.prefix));
            assertNull(pm.uri(d.prefix+"x"));
            assertNull(pm.uri(d.prefix+":"));

            String str = d.before + d.prefix + ":" + d.local + d.after;
            int start = d.before.length();
            int colon = d.before.length()+d.prefix.length();
            int end = d.before.length()+d.prefix.length()+1+d.local.length();
            assertEquals(d.uri+d.local, pm.expandPrefixed(str, start, colon, end));

            assertNull(pm.expandPrefixed(str.replace(":", "::"), start, colon+1, end+1));
            assertNull(pm.expandPrefixed(str.replace(":", ""), start, colon-1, end));
        }
    }

    @Test
    void testBuiltin() {
        PrefixMap pm = new PrefixMap();
        pm.resetToBuiltin();
        assertEquals(2, pm.size());
        assertEquals(RDFTypes.INT, pm.expandPrefixed("xsd:int", 0, 7));
        assertEquals(RDFTypes.HTML, pm.expandPrefixed("rdf:HTML", 0, 8));

        pm.add("", "http://example.org/");
        assertEquals(3, pm.size());
        assertEquals("http://example.org/local", pm.expandPrefixed(" :local ", 1, 7));

        pm.resetToBuiltin();
        assertEquals(2, pm.size());
        assertNull(pm.expandPrefixed(":local", 0, 0, 6));
        assertEquals(RDFTypes.INT, pm.expandPrefixed(" xsd:int,", 1, 8));
        assertEquals(RDFTypes.HTML, pm.expandPrefixed(" rdf:HTML,", 1, 9));
    }

    @Test
    void testFill() {
        Map<String, String> prefix2uri = new HashMap<>();
        var pm = new PrefixMap();
        List<D> data = data();
        for (D d : data) {
            pm.add(d.prefix, d.uri);
            prefix2uri.put(d.prefix, d.uri);

            assertEquals(d.uri, pm.uri(d.prefix));
            String str = d.prefix + ":local";
            String expanded = d.uri + "local";
            assertEquals(expanded, pm.expandPrefixed(str, 0,  str.length()));
        }
        for (var e : prefix2uri.entrySet()) {
            assertEquals(e.getValue(), pm.uri(e.getKey()));
            String str = ","+e.getKey()+":1 ";
            String expanded = e.getValue()+"1";
            int start = 1, colon = e.getKey().length()+1, end = str.length()-1;
            assertEquals(expanded, pm.expandPrefixed(str, start, colon, end));
        }
    }

    @Test
    void testAddAll() {
        PrefixMap left = new PrefixMap().resetToBuiltin(), right = new PrefixMap();
        right.add("", "http://example.org/");
        right.add("rdf", "http://rdf.org/");
        left.addAll(right);
        assertEquals("http://example.org/", left.uri(""));
        assertEquals("http://rdf.org/", left.uri("rdf"));
        assertEquals(RDFTypes.XSD, left.uri("xsd"));
        assertEquals(3, left.size());

        assertEquals("http://example.org/", right.uri(""));
        assertEquals("http://rdf.org/", right.uri("rdf"));
        assertEquals(2, right.size());


    }

}