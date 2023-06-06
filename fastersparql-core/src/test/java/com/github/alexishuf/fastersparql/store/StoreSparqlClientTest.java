package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.store.index.dict.*;
import com.github.alexishuf.fastersparql.store.index.triples.Triples;
import com.github.alexishuf.fastersparql.store.index.triples.TriplesSorter;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.client.util.TestTaskSet.platformRepeatAndWait;
import static com.github.alexishuf.fastersparql.client.util.TestTaskSet.platformTaskSet;
import static com.github.alexishuf.fastersparql.store.batch.StoreBatch.TYPE;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class StoreSparqlClientTest {
    private static final int REPS = Runtime.getRuntime().availableProcessors()*2;
    private static final Logger log = LoggerFactory.getLogger(StoreSparqlClientTest.class);
    private static final String PRL = """
                PREFIX    : <http://example.org/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                """;
    private static final String SEL_ALL = PRL+"SELECT * WHERE ";
    private static final List<TriplePattern> TRIPLES = List.of(
            Results.parseTP(":s1 :p1 :o1"),
            Results.parseTP(":s2 :p1 2"),
            Results.parseTP(":s3 :p1 3"),
            Results.parseTP(":s4 :p1 4"),
            Results.parseTP(":s4 :p1 5"),
            Results.parseTP("_:b1 :p2 1"),
            Results.parseTP("_:b1 :p2 _:b2"),
            Results.parseTP(":n0 :next :n1"),
            Results.parseTP(":n1 :next :n2"),
            Results.parseTP(":n2 :next :n3"),

            Results.parseTP(":l1 :lit \"bob\""),
            Results.parseTP(":l1 :lit \"\""),
            Results.parseTP(":l1 :lit \"alice\"@en"),
            Results.parseTP(":l1 :lit \"\"@en-US"),
            Results.parseTP(":l1 :lit \"new\\nline\""),
            Results.parseTP(":l1 :lit \"quo\\\"ted\"@en-US"),
            Results.parseTP(":l1 :lit \"quoted\\\"\""),
            Results.parseTP(":l1 :lit \"\\\"quoted\\\"\"")
    );
    private static EnumMap<Splitter.Mode, SparqlEndpoint> stores;
    private static final String CLS_NAME = StoreSparqlClientTest.class.getSimpleName();
    private boolean innerConcurrency = true;

    private static SparqlEndpoint makeStore(Splitter.Mode mode) throws IOException {
        Path tmp = Files.createTempDirectory("fastersparql");
        tmp.toFile().deleteOnExit();
        try (var b = new CompositeDictBuilder(tmp, tmp, mode, true)) {
            visitStrings(b);
            var secondPass = b.nextPass();
            visitStrings(secondPass);
            secondPass.write();
        }

        try (var strings = (LocalityCompositeDict) Dict.load(tmp.resolve("strings"));
             var sorter = new TriplesSorter(tmp)) {
            strings.validate();
            var l = strings.lookup();
            for (TriplePattern t : TRIPLES) {
                long s = l.find(t.s), p = l.find(t.p), o = l.find(t.o);
                assertTrue(s >= Dict.MIN_ID);
                assertTrue(p >= Dict.MIN_ID);
                assertTrue(o >= Dict.MIN_ID);
                sorter.addTriple(s, p, o);
            }
            sorter.write(tmp);
            try (var spo = new Triples(tmp.resolve("spo"));
                 var pso = new Triples(tmp.resolve("pso"));
                 var ops = new Triples(tmp.resolve("ops"))) {
                spo.validate();
                pso.validate();
                ops.validate();
            }
        }
        return new SparqlEndpoint("file://"+tmp.toAbsolutePath().toString().replace(" ", "%20"));
    }

    private static void visitStrings(NTVisitor b) {
        for (TriplePattern tp : TRIPLES) {
            b.visit(SegmentRope.of(tp.s));
            b.visit(SegmentRope.of(tp.p));
            b.visit(SegmentRope.of(tp.o));
        }
    }

    @BeforeAll static void beforeAll() throws IOException {
        stores = new EnumMap<>(Splitter.Mode.class);
        for (Splitter.Mode mode : Splitter.Mode.values())
            stores.put(mode, makeStore(mode));
    }

    @AfterAll static void afterAll() {
        for (var e : stores.entrySet()) {
            File file = e.getValue().asFile();
            if (file.exists()) {
                File[] contents = file.listFiles();
                if (contents != null) {
                    for (File child : contents) {
                        if (child.exists() && !child.delete())
                            log.error("Failed to delete {}", child);
                    }
                }
                assertTrue(file.delete(), "Failed to delete "+file+", for stores["+e.getKey()+"]");
            }
        }
    }

    record D(boolean useeBindingAwareProtocol, Splitter.Mode split, Results results) {
        public StoreSparqlClient createClient() {
            StoreSparqlClient client = (StoreSparqlClient) FS.clientFor(stores.get(split));
            client.usesBindingAwareProtocol(useeBindingAwareProtocol);
            return client;
        }

        public D withParsedOrOpaque() {
            boolean ubp = useeBindingAwareProtocol;
            if (results.query() instanceof Plan p)
                return new D(ubp, split, results.query(new OpaqueSparqlQuery(p.sparql())));
            else if (results.query() instanceof OpaqueSparqlQuery q)
                return new D(ubp, split, results.query(new SparqlParser().parse(q)));
            else
                return this;
        }

        public static void addParsedOrOpaque(List<D> list) {
            for (int i = 0, n = list.size(); i < n; i++)
                list.add(list.get(i).withParsedOrOpaque());
        }

        public static List<D> fromResultsList(List<Results> resultsList) {
            List<D> list = new ArrayList<>();
            for (boolean ubp : List.of(false, true)) {
                for (Splitter.Mode split : Splitter.Mode.values()) {
                    for (Results r : resultsList)
                        list.add(new D(ubp, split, r));
                }
            }
            D.addParsedOrOpaque(list);
            return list;
        }

    }

    static Stream<Arguments> test() {
        List<Results> resultsList = new ArrayList<>(List.of(
                results("?x", ":o1").query(SEL_ALL+"{ :s1 :p1 ?x }"),
                results("?x", ":p1").query(SEL_ALL+"{ :s1 ?x  :o1 }"),
                results("?x", ":s1").query(SEL_ALL+"{ ?x  :p1 :o1 }"),
                results("?var0", ":o1").query(SEL_ALL+"{ :s1 :p1 $var0 }"),
                results("?var0", ":p1").query(SEL_ALL+"{ :s1 $var0  :o1 }"),
                results("?var0", ":s1").query(SEL_ALL+"{ $var0  :p1 :o1 }"),

                results("?x",
                        "\"bob\"",
                        "\"\"",
                        "\"alice\"@en",
                        "\"\"@en-US",
                        "\"new\\nline\"",
                        "\"quo\\\"ted\"@en-US",
                        "\"quoted\\\"\"",
                        "\"\\\"quoted\\\"\""
                ).query(SEL_ALL+"{ :l1 :lit ?x }"),

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

                results("?x", "1", "_:b2").query(SEL_ALL+"{ _:b1 :p2 ?x }"),

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

        return D.fromResultsList(resultsList).stream().map(Arguments::arguments);
    }


    @ParameterizedTest @MethodSource void test(D d) {
        try (var client = d.createClient()) {
            d.results.check(client);
            d.results.check(client); // test client works more than once
            d.results.check(client, TYPE);
            if (innerConcurrency) {
                try (var tasks = platformTaskSet(CLS_NAME+".test")) {
                    tasks.repeat(REPS, () -> d.results.check(client));
                    tasks.repeat(REPS, () -> d.results.check(client, TYPE));
                } catch (Throwable t) {fail(t);}
            }
        }
    }

    @ParameterizedTest @MethodSource("test")
    void testWithDummyBinding(D d) {
        Results results = d.results.bindings(List.of(List.of()));
        try (var client = d.createClient()) {
            results.check(client, TERM);
            results.check(client, COMPRESSED);
            if (innerConcurrency) {
                try {
                    platformRepeatAndWait(CLS_NAME + ".testWithDummyBinding", REPS,
                            () -> {
                                d.results.check(client, TERM);
                                d.results.check(client, COMPRESSED);
                            });
                } catch (Throwable t) {fail(t);}
            }
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

                results("?p    ?y",
                        ":p1", 3,
                        ":p1", 4)
                        .query(SEL_ALL+ """
                                {
                                  { :s3 ?p ?y FILTER (?y > 2 && ?y < 4) }
                                  UNION
                                  { :s4 ?p ?y FILTER (?y < 5 &&  ?y > 3) }
                                }""")
                        .bindings("?p", ":p1")
        ));

        return D.fromResultsList(list).stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource public void testBinding(D d) {
        Results r = d.results;
        try (var client = d.createClient()) {
            r.check(client);
            r.check(client, COMPRESSED);
            r.check(client, TYPE, it -> TYPE.convert(it, client.dictId()));
            if (innerConcurrency) {
                try {
                    platformRepeatAndWait(CLS_NAME + ".testBinding", REPS,
                            () -> {
                                d.results.check(client);
                                d.results.check(client, COMPRESSED);
                                d.results.check(client, TYPE, it -> TYPE.convert(it, client.dictId()));
                            });
                } catch (Throwable t) {fail(t);}
            }
        }
    }

    @Test void testConcurrentQuery() throws Exception {
        List<D> queryData   = test()       .map(a -> (D) a.get()[0]).toList();
        List<D> bindingData = testBinding().map(a -> (D) a.get()[0]).toList();
        innerConcurrency = false;
        int cpus = 2*Runtime.getRuntime().availableProcessors();
        try (var tasks = new TestTaskSet(CLS_NAME+"testConcurrentQuery",
                                          newFixedThreadPool(cpus))) {
            for (int rep = 0; rep < REPS; rep++) {
                for (D d : queryData) tasks.add(() -> test(d));
                for (D d : queryData) tasks.add(() -> testWithDummyBinding(d));
                for (D d : bindingData) tasks.add(() -> testBinding(d));
            }
        } finally {
            innerConcurrency = true;
        }
    }
}