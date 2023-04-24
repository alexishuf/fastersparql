package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.alexishuf.fastersparql.model.rope.RopeDict.*;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RopeDictTest {

    @BeforeAll static void beforeAll() { reset(); }
    @AfterAll  static void  afterAll() { reset(); }
    @BeforeEach       void     setUp() { reset(); }
    @AfterEach        void  tearDown() { reset(); }

    @ParameterizedTest @ValueSource(ints = {16, 8, 7, 0})
    void testReset(int bits) {
        if (bits > 0)
            resetWithCapacity(bits);
        else
            reset();
        assertEquals(P_XSD, (int)internIri(SegmentRope.of("<http://www.w3.org/2001/XMLSchema#>")));
        assertEquals(P_RDF, (int)internIri(SegmentRope.of("<http://www.w3.org/1999/02/22-rdf-syntax-ns#>")));

        assertEquals(DT_string,       internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#string>")));
        assertEquals(DT_integer,      internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
        assertEquals(DT_decimal,      internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
        assertEquals(DT_duration,     internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#duration>")));
        assertEquals(DT_date,         internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#date>")));
        assertEquals(DT_dateTime,     internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")));
        assertEquals(DT_INT,          internDatatype(SegmentRope.of("\"^^<http://www.w3.org/2001/XMLSchema#int>")));
        assertEquals(DT_PlainLiteral, internDatatype(SegmentRope.of("\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral>")));

        assertEquals(DT_integer, (int)internLit(SegmentRope.of("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>")));
        assertEquals(DT_decimal, (int)internLit(SegmentRope.of("\"23\"^^<http://www.w3.org/2001/XMLSchema#decimal>")));
        assertEquals(DT_INT,     (int)internLit(SegmentRope.of("\"23\"^^<http://www.w3.org/2001/XMLSchema#int>")));
    }


    @ParameterizedTest @ValueSource(ints = {16, 32, 8192})
    void testRace(int rounds) throws ExecutionException, InterruptedException {
        int threads = Runtime.getRuntime().availableProcessors() * 4;
        resetWithCapacity(7);
        disableFloodProtection();

        String[] iris = new String[rounds];
        for (int i = 0; i < rounds; i++)
            iris[i] = format("<http://www.example.org/%04d/Bob>", i);
        long[][] thread2round2localAndId = new long[threads][];
        for (int i = 0; i < threads; i++)
            thread2round2localAndId[i] = new long[rounds];

        try (var ex = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int thread = i;
                tasks.add(ex.submit(() -> {
                    for (int round = 0; round < rounds; round++)
                        thread2round2localAndId[thread][round] = internIri(SegmentRope.of(iris[round]));
                }));
            }
            for (Future<?> t : tasks)
                t.get();
        }

        for (int thread = 0; thread < threads; thread++) {
            for (int round = 0; round < rounds; round++) {
                var ctx = " at thread " + thread + ", round " + round;
                long localAndId = thread2round2localAndId[thread][round];
                assertEquals(29, localAndId>>>32, ctx);
                assertTrue((int)localAndId > 0, ctx);
                assertEquals(iris[round].substring(0, 29), get((int)localAndId).toString(), ctx);
            }
        }
        for (int round = 0; round < rounds; round++) {
            long expected = thread2round2localAndId[0][round];
            for (int thread = 0; thread < threads; thread++) {
                var ctx = "at thread " + thread + ", round " + round;
                assertEquals(expected, thread2round2localAndId[thread][round], ctx);
            }
        }

    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void testGrow(boolean parallel) throws ExecutionException, InterruptedException {
        resetWithCapacity(7);
        disableFloodProtection();

        String template = "<http://www.%08d.org/ns#%d>";
        String[] iris = new String[544]; // grow() on 256 and 512
        int[] locals  = new int[iris.length];
        int[] ids  = new int[iris.length];
        for (int i = 0; i < iris.length; i++)
            iris[i] = format(template, (int) (Math.random() * MAX_VALUE) % 100_000_000, i);
        int threads = parallel ? (int)(1.5*Runtime.getRuntime().availableProcessors()) : 1;
        int chunk = iris.length/threads;


        try (var ex = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int repetition = 0; repetition < 500; repetition++) {
                tasks.clear();
                //start threads to intern all IRIs in iris.
                for (int tIdx = 0; tIdx < threads; tIdx++) {
                    int t = tIdx;
                    tasks.add(ex.submit(() -> {
                        for (int i = t * chunk, e = t == threads - 1 ? iris.length : i + chunk; i < e; i++) {
                            long localAndId = internIri(SegmentRope.of(iris[i]));
                            locals[i] = (int) (localAndId >>> 32);
                            ids[i] = (int) localAndId;
                        }
                    }));
                }
                // wait threads
                for (Future<?> t : tasks) t.get();

                // check results reported by all threads
                for (int i = 0; i < iris.length; i++) {
                    var ctx = "repetition=" + repetition + ", i=" + i;
                    assertEquals(28, locals[i], ctx);
                    assertTrue(ids[i] > 0, ctx);
                    assertEquals(iris[i].substring(0, 28), get(ids[i]).toString(), ctx);
                    assertEquals((28L << 32) | ids[i], internIri(SegmentRope.of(iris[i])), ctx);
                }
            }
        }
    }

    @Test
    void testGetBuiltInsById() {
        String xsd = "\"^^<http://www.w3.org/2001/XMLSchema#";
        String rdf = "\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        assertEquals(xsd+"duration>", RopeDict.get(DT_duration).toString());
        assertEquals(xsd+"dateTime>", RopeDict.get(DT_dateTime).toString());
        assertEquals(xsd+"gYearMonth>", RopeDict.get(DT_gYearMonth).toString());
        assertEquals(xsd+"boolean>", RopeDict.get(DT_BOOLEAN).toString());
        assertEquals(xsd+"base64Binary>", RopeDict.get(DT_base64Binary).toString());
        assertEquals(xsd+"string>", RopeDict.get(DT_string).toString());
        assertEquals(xsd+"int>", RopeDict.get(DT_INT).toString());
        assertEquals(rdf+"langString>", RopeDict.get(DT_langString).toString());
        assertEquals(rdf+"JSON>", RopeDict.get(DT_JSON).toString());
        assertEquals(rdf+"HTML>", RopeDict.get(DT_HTML).toString());
    }

    @Test
    void testInternBuiltinDataypes() {
        record D(String iri, int expected) {
            void test(int row) {
                for (String prefix : List.of("\"^^<", "\"asd\"^^<", "\"\\\"^^<...>\"^^<")) {
                    var ctx = "prefix="+prefix+" at data["+row+"]="+this;
                    var r = SegmentRope.of(prefix, iri, '>');
                    int begin = prefix.length()-4, end = r.len();
                    assertEquals(expected, RopeDict.internDatatype(r, begin, end), ctx);
                }
                List<String> bogus = List.of("", "@", "\"^^<");
                for (String bogusPrefix : bogus) {
                    for (String bogusSuffix : bogus) {
                        for (String litPrefix : List.of("\"\"^^<",
                                                        "\"asd\"^^<",
                                                        "\"\\\"^^<...>\"^^<")) {
                            var ctx = "bogusPrefix="+bogusPrefix+", bogusSuffix="+bogusSuffix+", litPrefix="+litPrefix+" at data["+row+"]="+this;
                            var r = SegmentRope.of(bogusPrefix, litPrefix, iri, '>', bogusSuffix);
                            int begin = bogusPrefix.length(), end = r.len-bogusSuffix.length();
                            long suffixAndId = internLit(r, begin, end);
                            int expectedSuffix = bogusPrefix.length() + litPrefix.length() - 4;
                            assertEquals(expectedSuffix, (int)(suffixAndId>>>32), ctx);
                            assertEquals(expected,       (int) suffixAndId,       ctx);
                        }
                    }
                }
            }
        }
        String xsd = "http://www.w3.org/2001/XMLSchema#";
        String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        List<D> data = List.of(
                new D(xsd+"duration", DT_duration),
                new D(xsd+"dateTime", DT_dateTime),
                new D(xsd+"date", DT_date),
                new D(xsd+"decimal", DT_decimal),
                new D(xsd+"double", DT_DOUBLE),
                new D(xsd+"gMonthDay", DT_gMonthDay),
                new D(xsd+"int", DT_INT),
                new D(xsd+"integer", DT_integer),
                new D(xsd+"unsignedByte", DT_unsignedByte),
                new D(xsd+"unsignedInt", DT_unsignedInt),
                new D(xsd+"unsignedShort", DT_unsignedShort),
                new D(rdf+"langString", DT_langString),
                new D(rdf+"XMLLiteral", DT_XMLLiteral),
                new D(rdf+"JSON", DT_JSON)
        );
        for (int i = 0; i < data.size(); i++)
            data.get(i).test(i);
    }

    @Test
    void testInternShortIri() {
        assertEquals(0L, internIri(SegmentRope.of("<>")));
        assertEquals(0L, internIri(SegmentRope.of("<http://www.123.de/1>")));
        assertEquals(1L << 32, internIri(SegmentRope.of("<<http://www.123.de/1>"), 1, 22));
    }

    @Test
    void testInternWellBehavedIri() {
        long alice = internIri(SegmentRope.of("<http://example.org/123456/Alice>"));
        assertEquals(27, alice>>>32);
        assertEquals("<http://example.org/123456/", get((int)alice).toString());

        assertEquals(alice, internIri(SegmentRope.of("<http://example.org/123456/Alice>")));
        assertEquals(alice, internIri(SegmentRope.of("<http://example.org/123456/Bob>")));
        assertEquals(alice, internIri(SegmentRope.of("<http://example.org/123456/>")));

        long shifted = alice + (1L << 32);
        assertEquals(shifted, internIri(SegmentRope.of(".<http://example.org/123456/Bob>."), 1, 32));
        assertEquals(shifted, internIri(SegmentRope.of("<<http://example.org/123456/Bob>>"), 1, 32));
        assertEquals(shifted, internIri(SegmentRope.of("!<http://example.org/123456/Bob>/"), 1, 32));
        assertEquals(shifted, internIri(SegmentRope.of("#<http://example.org/123456/Bob>#"), 1, 32));
        assertEquals(shifted, internIri(SegmentRope.of("/<http://example.org/123456/Bob>>/#."), 1, 32));
    }

    @Test
    void testMoveSplit() {
        List<String> templates = List.of("<http://example.org/data/res/xxx>",          //LAST
                                         "<http://example.org/data/res/xxx#self>",      //PENULTIMATE
                                         "<http://example.org/data/res/xxx/info#self>" //FIRST
        );
        for (String tpl : templates) {
            RopeDict.resetFloodProtection();
            int minSplit = MAX_VALUE, blockedAt = MAX_VALUE;
            for (int i = 0; i < 512 && blockedAt == MAX_VALUE; i++) {
                String iri = tpl.replace("xxx", format("%03d", i));
                long localAndId = internIri(SegmentRope.of(iri));
                minSplit = (int) Math.min(localAndId>>>32, minSplit);
                if ((int)localAndId == 0) {
                    blockedAt = i;
                } else {
                    String local = iri.substring((int) (localAndId >>> 32));
                    assertEquals(iri, get((int) localAndId) + local);
                }
            }
            assertEquals(29, minSplit, "tpl="+tpl);
            assertEquals(MAX_VALUE, blockedAt, "tpl="+tpl);
        }
    }

    @Test
    void testBlockAlwaysInsert() {
        String tpl = "<http://example.org/data/xxx/res/info#self>";
        int minSplit = MAX_VALUE, blockedAt = MAX_VALUE;
        for (int i = 0; i < 512 && blockedAt == MAX_VALUE; i++) {
            String iri = tpl.replace("xxx", format("%03d", i));
            long localAndId = internIri(SegmentRope.of(iri));
            minSplit = (int) Math.min(minSplit, localAndId>>>32);
            if ((int)localAndId == 0) {
                blockedAt = i;
            } else {
                String local = iri.substring((int) (localAndId >>> 32));
                assertEquals(iri, get((int) localAndId) + local);
            }
        }
        assertEquals(0, minSplit);
        assertTrue(blockedAt < 512, "did not stop");
        assertTrue(blockedAt > 32, "blocked too fast");
    }

    @Test
    void testInternLit() {
        record D(String lit, int suffix) {
            public D(String lit) { this(lit, lit.length()); }

            void test(int row) {
                test(new ByteRope(lit), 0, lit.length(), row);
                test(new SegmentRope(ByteBuffer.wrap(lit.getBytes(UTF_8))), 0, lit.length(), row);

                String padded = "\"^^<" + lit + "\"^^</#";
                int begin = 4, end = begin+lit.length();
                test(new ByteRope(padded), begin, end, row);
                test(new SegmentRope(ByteBuffer.wrap(padded.getBytes(UTF_8))), begin, end, row);
            }

            void test(SegmentRope rope, int begin, int end, int row) {
                var ctx = "at data["+row+"]="+this;
                long suffixAndId = internLit(rope, begin, end);
                assertEquals(suffix+begin, suffixAndId>>>32, ctx);
                if (suffix < lit.length())
                    assertEquals(lit.substring(suffix), get((int)suffixAndId).toString());
                else
                    assertEquals(0, (int)suffixAndId);
            }
        }
        List<D> data = List.of(
                new D("\"asd\""),
                new D("\"a\""),
                new D("\"\""),
                new D("\"\\\"\""),
                new D("\".................................\""),
                new D("\"\\\"^^<....................................>\""),

                new D("\"asd\"@en"),
                new D("\"a\"@en-US"),
                new D("\"\"@en"),
                new D("\"\\\"\"@en"),
                new D("\".................................\"@en"),
                new D("\"\\\"^^<....................................>\"@en-US"),

                new D("\"32\"^^<http://www.w3.org/2001/XMLSchema#integer>", 3),
                new D("\"-1\"^^<http://www.w3.org/2001/XMLSchema#integer>", 3),
                new D("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", 4),
                new D("\"a\"^^<http://www.w3.org/2001/XMLSchema#string>", 2),
                new D("\"\"^^<http://www.w3.org/2001/XMLSchema#string>", 1),
                new D("\"\\\"^^<\"^^<http://www.w3.org/2001/XMLSchema#string>", 6)
        );
        for (int i = 0; i < data.size(); i++)
            data.get(i).test(i);
    }

    @Test void testInternFoaf() {
        var foaf = SegmentRope.of("<http://xmlns.com/foaf/0.1/>");
        long localAndId = internIri(foaf, 0, foaf.len());
        assertEquals(27,  localAndId>>>32);
        assertTrue((int)localAndId > 0);
        assertEquals("<http://xmlns.com/foaf/0.1/", RopeDict.get((int)localAndId).toString());
    }
}