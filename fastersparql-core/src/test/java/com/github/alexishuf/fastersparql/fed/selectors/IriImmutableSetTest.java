package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;

class IriImmutableSetTest {

    static List<List<Term>> lists() {
        List<List<Term>> list = new ArrayList<>(List.of(
                List.of(),
                termList("<a>"), // no ids, singleton
                termList("<a>", "<b>"), // no ids, short list
                termList("<a>", "<a>", "<b>", "<a>", "<b>"), // no ids, with duplicates
                termList("exns:a"), // single id, singleton
                termList("exns:a", "exns:b"), // single id, short
                termList("exns:a", "exns:b", "exns:a", "exns:c"), // single id, short with duplicates
                termList("exns:a", "owl:Class", "rdf:type", "exns:b", "owl:ObjectProperty") // multiple ids
        ));

        List<Term> big = new ArrayList<>();
        for (String prefix : List.of("<http://www.example.org/ns#", "<", "<http://www.w3.org/2002/07/owl#"))
            range(0, 512).mapToObj(i -> Term.valueOf(prefix+i+">")).forEach(big::add);
        list.add(big);
        return list;
    }

    @Test void testHas() {
        for (List<Term> terms : lists()) {
            var set = new IriImmutableSet(terms);
            assertEquals(new HashSet<>(terms).size(), set.size(), "terms="+terms);
            for (Term term : terms) {
                assertTrue(set.has(term), "term="+term+", terms="+terms);
                Term bogus = Term.valueOf(term.toString().replace(">", "x>"));
                assertFalse(set.has(bogus), "bogus="+bogus+", terms="+terms);
            }
        }
    }

    private static final class ByteOS extends ByteArrayOutputStream {
        boolean closed;

        @Override public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static final class ByteIS extends ByteArrayInputStream {
        boolean closed;
        public ByteIS(byte[] buf) { super(buf); }

        @Override public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    @Test void testSaveAndLoad() throws IOException {
        for (List<Term> terms : lists()) {
            IriImmutableSet set = new IriImmutableSet(terms), loaded;
            try (var out = new ByteOS()) {
                set.save(out);
                assertFalse(out.closed);
                try (var in = new ByteIS(out.toByteArray())) {
                    loaded = IriImmutableSet.load(in);
                    assertFalse(in.closed);
                    for (Term term : terms) {
                        assertTrue(loaded.has(term), "term="+term+", terms="+terms);
                        var tmp = term.toString();
                        var bogus = Term.valueOf(tmp.substring(0, tmp.length()-2)+"x>");
                        assertFalse(loaded.has(bogus), "bogus="+bogus+", terms="+terms);
                    }
                }
            }
        }
    }

}