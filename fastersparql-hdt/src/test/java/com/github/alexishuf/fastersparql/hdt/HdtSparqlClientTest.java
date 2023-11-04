package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.hdt.cardinality.HdtEstimatorPeek;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.hdt.FSHdtProperties.ESTIMATOR_PEEK;
import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType.HDT;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.waitStage;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;

class HdtSparqlClientTest {

    private static File hdtFile;
    private static SparqlEndpoint endpoint;

    private static final String PRL = """
                PREFIX    : <http://example.org/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                """;
    private static final String SEL_ALL = PRL+"SELECT * WHERE ";

    @BeforeAll
    static void beforeAll() throws IOException {
        hdtFile = Files.createTempFile("fs-HdtSparqlClientTest", ".hdt").toFile();
        InputStream in = HdtSparqlClient.class.getResourceAsStream("data.hdt");
        assertNotNull(in);
        try (var out = new FileOutputStream(hdtFile)) {
            in.transferTo(out);
        }
        var path = hdtFile.getAbsolutePath().replace(" ", "%20");
        endpoint = SparqlEndpoint.parse("file://"+path);
    }

    @AfterAll
    static void afterAll() {
        assertTrue(hdtFile == null || !hdtFile.exists() || hdtFile.delete());
    }

    @BeforeEach void    setUp() { FSHdtProperties.refresh(); }
    @AfterEach  void tearDown() { FSHdtProperties.refresh(); }

    @Test public void selfTest() throws IOException {
        assertTrue(hdtFile.exists());
        assertTrue(hdtFile.isFile());
        assertTrue(hdtFile.canRead());
        assertTrue(hdtFile.length() > 0);
        try (HDT hdt = HDTManager.mapIndexedHDT(hdtFile.getAbsolutePath())) {
            assertTrue(hdt.getTriples().getNumberOfElements() > 0);
        }
    }

    @Test public void testCreateFromFactory() {
        try (var client = FS.clientFor(endpoint)) {
            assertTrue(client instanceof HdtSparqlClient);
        }
    }

    @Test public void testWaitEstimator() {
        try (var c = (HdtSparqlClient)FS.clientFor(endpoint)) {
            var e = c.estimator();
            assertSame(e, assertTimeout(ofMillis(100), () -> waitStage(e.ready())));
        }
    }

    @Test public void testQueryTPWithModifier() {
        try (var client = (HdtSparqlClient)FS.clientFor(endpoint)) {
            results("?n", ":n1", ":n2", ":n3")
                    .query("SELECT DISTINCT ?n WHERE { ?s :next ?n }")
                    .check(client);
            results("?s ?n", ":n0", ":n1")
                    .query("SELECT DISTINCT ?n WHERE { ?s :next ?n }")
                    .bindings("?s", ":n0")
                    .check(client, HDT, HDT.converter(client.dictId));
        }
    }

    static Stream<Arguments> test() {
        List<Results> list = new ArrayList<>(List.of(
                results("?x", ":o1").query(SEL_ALL+"{ :s1 :p1 ?x }"),
                results("?x", ":p1").query(SEL_ALL+"{ :s1 ?x  :o1 }"),
                results("?x", ":s1").query(SEL_ALL+"{ ?x  :p1 :o1 }"),
                results("?var0", ":o1").query(SEL_ALL+"{ :s1 :p1 $var0 }"),
                results("?var0", ":p1").query(SEL_ALL+"{ :s1 $var0  :o1 }"),
                results("?var0", ":s1").query(SEL_ALL+"{ $var0  :p1 :o1 }"),

                results("?x ?y", ":p1", ":o1").query(SEL_ALL+"{ :s1 ?x ?y }"),
                results("?x ?y", ":s1", ":p1").query(SEL_ALL+"{ ?x ?y :o1 }"),

                results("?x", ":p1").query(PRL+"SELECT $x WHERE { :s1 ?x ?y }"),
                results("?x", ":s1").query(PRL+"SELECT $x WHERE { ?x ?y :o1 }"),

                results("?x", 2).query(SEL_ALL+"{ :s2 :p1 ?x }"),
                results("?x", ":s3").query(SEL_ALL+"{ ?x :p1 3 }"),
                results("?x", ":p1").query(SEL_ALL+"{ :s3 ?x 3 }"),
                results("?x", 4, 5).query(SEL_ALL+"{ :s4 :p1 ?x }"),
                results("?x", ":s4").query(SEL_ALL+"{ ?x :p1 4 }"),
                results("?x", ":s4").query(SEL_ALL+"{ ?x :p1 5 }"),
                results("?x", ":s4").query(SEL_ALL+"{ ?x :p1 \"5\"^^xsd:integer }"),

                results("?x", "1", "_:B2").query(SEL_ALL+"{ _:B1 :p2 ?x }"),

                results("?x", ":s3").query(PRL+ """
                        SELECT ?x WHERE {
                          ?x :p1 ?o FILTER(?o = 3)
                        }"""),

                results("?x ?o1 ?o2", ":s4", 4, 5).query(PRL+ """
                        SELECT ?x ?o1 ?o2 WHERE {
                          ?x :p1 ?o1.
                          ?x :p1 ?o2 FILTER(?o2 > ?o1).
                        }"""),

                results("?a    ?b",
                        ":n0", ":n1",
                        ":n1", ":n2",
                        ":n2", ":n3").query(SEL_ALL+"{ ?a :next ?b }"),

                results("?a    ?b",
                        ":n0", ":n2",
                        ":n1", ":n3"
                        ).query(PRL+ """
                        SELECT ?a ?b WHERE {
                          ?a :next ?c.
                          ?c :next ?b.
                        }"""),

                results("?a    ?b",
                        ":n0", ":n3"
                ).query(PRL+ """
                        SELECT ?a ?b WHERE {
                          ?a :next ?c.
                          ?c :next ?d.
                          ?d :next ?b.
                        }""")
        ));

        // retry all test cases with already parsed queries
        addWithParsedOrOpaque(list);
        return list.stream().map(Arguments::arguments);
    }

    private static void addWithParsedOrOpaque(List<Results> list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            var r = list.get(i);
            if (r.query() instanceof Plan p)
                list.add(r.query(new OpaqueSparqlQuery(p.sparql())));
            else
                list.add(r.query(SparqlParser.parse(r.query())));
        }
    }

    @ParameterizedTest @MethodSource void test(Results results) {
        for (HdtEstimatorPeek peek : HdtEstimatorPeek.values()) {
            System.setProperty(ESTIMATOR_PEEK, peek.name());
            FSHdtProperties.refresh();
            try (var client = new HdtSparqlClient(endpoint)) {
                var estimator = client.estimator();
                assertSame(estimator, waitStage(estimator.ready()));
                results.check(client);
                results.check(client, HDT);
            } finally {
                System.clearProperty(ESTIMATOR_PEEK);
                FSHdtProperties.refresh();
            }
        }
    }

    @ParameterizedTest @MethodSource("test") void testWithDummyBinding(Results results) {
        try (var client = new HdtSparqlClient(endpoint)) {
            results.bindings(List.of(List.of())).check(client);
        }
    }

    static Stream<Arguments> testBinding() {
        List<Results> list = new ArrayList<>(List.of(
                results("?x ?y", ":s1", ":o1")
                        .query(SEL_ALL+"{ ?x :p1 ?y }")
                        .bindings("?x", ":s1"),
                results("?x ?y")
                        .query(SEL_ALL+"{ ?x :p1 ?y }")
                        .bindings("?x"),
                results("?x ?y", ":s1", ":o1", ":s2", 2)
                        .query(SEL_ALL+"{ ?x :p1 ?y }")
                        .bindings("?x", ":s1", ":s2"),
                results("?y ?x", 2, ":s2", 4, ":s4")
                        .query(SEL_ALL+"{ ?x :p1 ?y }")
                        .bindings("?y", 2, 4),

                results("?p    ?x    ?y",
                        ":p1", ":s3", 3,
                        ":p1", ":s4", 4)
                        .query(SEL_ALL+ """
                                {
                                  { ?x ?p ?y FILTER (?y > 2 && ?y < 4) }
                                  UNION
                                  { ?x ?p ?y FILTER (?y < 5 &&  ?y > 3) }
                                }""")
                        .bindings("?p", ":p1")
        ));

        addWithParsedOrOpaque(list);
        return list.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource public void testBinding(Results r) {
        try (var client = FS.clientFor(endpoint)) {
            r.check(client);
            r.check(client, HDT, HDT.converter(((HdtSparqlClient) client).dictId));
        }
    }
}