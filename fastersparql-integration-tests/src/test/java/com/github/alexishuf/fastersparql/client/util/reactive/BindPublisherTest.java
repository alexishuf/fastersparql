package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.FasterSparql;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientTest;
import com.github.alexishuf.fastersparql.client.model.row.impl.ListOperations;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.row.StringListRowParser;
import com.github.alexishuf.fastersparql.client.util.bind.BindPublisher;
import com.github.alexishuf.fastersparql.client.util.bind.Binder;
import com.github.alexishuf.fastersparql.client.util.bind.SparqlClientBinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
class BindPublisherTest {
    private static final Logger log = LoggerFactory.getLogger(BindPublisherTest.class);

    private static final int THREADS = Runtime.getRuntime().availableProcessors()*4;
    @Container private static final HdtssContainer hdtss
            = new HdtssContainer(SparqlClientTest.class, "data.hdt", log);
    private static final AtomicInteger testRun = new AtomicInteger();
    private static ExecutorService executor;
    private static SparqlClient<List<String>, byte[]> client;

    @BeforeAll
    static void beforeAll() {
        executor = Executors.newCachedThreadPool();
        client = FasterSparql.clientFor(hdtss.asEndpoint("tsv,post"),
                                        StringListRowParser.INSTANCE,
                                        ByteArrayFragmentParser.INSTANCE);
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        List<Runnable> leftovers = executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
                   "executor did not terminate in 5s");
        assertEquals(emptyList(), leftovers);
        client.close();
    }

    private static String expandPrefixes(String s) {
        if (s == null)
            return null;
        return s.replace("<:", "<http://example.org/")
                .replace("<xsd:", "<http://www.w3.org/2001/XMLSchema#");
    }

    private static List<String> expandPrefixesInRow(List<String> list) {
        return list.stream().map(BindPublisherTest::expandPrefixes).collect(toList());
    }
    private static List<List<String>> expandPrefixes(List<List<String>> list) {
        return list.stream().map(BindPublisherTest::expandPrefixesInRow).collect(toList());
    }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        String prefix = "PREFIX : <http://example.org/>\n" +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n";
        class D {
            final List<String> bindingVars;
            final List<List<String>> bindings;
            final String sparql;
            final BindType bindType;
            final List<List<String>> expected;

            public D(List<String> bindingVars, List<List<String>> bindings, String sparql, BindType bindType, List<List<String>> expected) {
                this.bindingVars = bindingVars;
                this.bindings = bindings;
                this.sparql = sparql;
                this.bindType = bindType;
                this.expected = expected;
            }
        }

        String i23 = "\"23\"^^<xsd:integer>";
        String i25 = "\"25\"^^<xsd:int>";
        String i51 = "\"51\"^^<xsd:integer>";
        List<D> base = new ArrayList<>(asList(
                /* no binding produces no results */
        /*  1 */new D(emptyList(), emptyList(), "SELECT * WHERE {?x foaf:age 23}",
                      BindType.JOIN, emptyList()),

                /* single empty binding produces 2 results */
        /*  2 */new D(emptyList(), singletonList(emptyList()),
                     "SELECT * WHERE {?x foaf:age 23}", BindType.JOIN,
                      asList(singletonList("<:Alice>"), singletonList("<:Eric>"))),

                /* single binding produces 1~2 rows + join/left join */
        /*  3 */new D(singletonList("y"), singletonList(singletonList(i23)),
                      "SELECT * WHERE { ?x foaf:age ?y}", BindType.JOIN,
                      asList(asList(i23, "<:Alice>"), asList(i23, "<:Eric>"))),
        /*  4 */new D(singletonList("y"), singletonList(singletonList(i25)),
                        "SELECT * WHERE { ?x foaf:age ?y }", BindType.LEFT_JOIN,
                        singletonList(asList(i25, "<:Bob>"))),

                /* single binding produces no rows + left join */
        /*  5 */new D(singletonList("y"), singletonList(singletonList(i51)),
                      "SELECT * WHERE { ?x foaf:age ?y }", BindType.LEFT_JOIN,
                      singletonList(asList(i51, null))),

                /* single binding produces no rows + (not) exists */
        /*  6 */new D(singletonList("y"), singletonList(singletonList(i51)),
                        "SELECT * WHERE { ?x foaf:age ?y }", BindType.EXISTS,
                        emptyList()),
        /*  7 */new D(singletonList("y"), singletonList(singletonList(i51)),
                        "SELECT * WHERE { ?x foaf:age ?y }", BindType.NOT_EXISTS,
                        singletonList(singletonList(i51))),
                /* single binding produces two rows + (not) exists */
        /*  8 */new D(singletonList("y"), singletonList(singletonList(i23)),
                      "SELECT * WHERE { ?x foaf:age ?y }", BindType.EXISTS,
                      singletonList(singletonList(i23))),
        /*  9 */new D(singletonList("y"), singletonList(singletonList(i23)),
                     "SELECT * WHERE { ?x foaf:age ?y }", BindType.NOT_EXISTS,
                      emptyList()),

                /* multiple bindings produce multiple results */
        /* 10 */new D(singletonList("y"),
                      asList(singletonList(i23),
                             singletonList(i25)),
                     "SELECT * WHERE { ?x foaf:age ?y }", BindType.JOIN,
                      asList(asList(i23, "<:Alice>"), asList(i23, "<:Eric>"),
                             asList(i25, "<:Bob>"))),
        /* 11 */new D(singletonList("x"),
                      asList(singletonList("<:Alice>"),
                             singletonList("<:Bob>"),
                             singletonList("<:Charlie>"),
                             singletonList("<:Dave>"),
                             singletonList("<:Eric>")),
                        "SELECT * WHERE { ?x foaf:age ?y }", BindType.LEFT_JOIN,
                      asList(asList("<:Alice>", i23), //alice
                              asList("<:Bob>", i25), //bob
                              asList("<:Charlie>", null), //charlie
                              asList("<:Dave>", null), //dave
                              asList("<:Eric>", i23) //eric
                      ))
        ));

        List<Arguments> argumentsList = new ArrayList<>();
        for (int bindConcurrency : asList(1, 2, 16)) {
            for (D d : base) {
                String sparql = prefix + d.sparql;
                SparqlClientBinder<List<String>> binder = new SparqlClientBinder<>(ListOperations.get(), d.bindingVars,
                        client, sparql, null, d.bindType);
                List<List<String>> bindings = expandPrefixes(d.bindings);
                List<List<String>> expected = expandPrefixes(d.expected);
                argumentsList.add(arguments(bindings, bindConcurrency, binder, expected));
            }
        }
        return argumentsList.stream();
    }

    @ParameterizedTest @MethodSource
    void test(List<List<String>> bindings, int bindConcurrency, Binder<List<String>> binder,
              List<List<String>> expected) throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            int thread = i;
            futures.add(executor.submit(() -> {
                String name = "BindPublisherTest-"+testRun.getAndIncrement()+"-"+thread;
                Thread.currentThread().setName(name);
                FSPublisher<List<String>> bindingsPublisher
                        = FSPublisher.bindToAny(Flux.fromIterable(bindings)
                        .subscribeOn(Schedulers.boundedElastic())
                        .publishOn(Schedulers.boundedElastic()));
                BindPublisher<List<String>> bp = new BindPublisher<>(
                        bindingsPublisher, bindConcurrency, binder.copyIfNotShareable(),
                        name, null);
                List<List<String>> actual = Flux.from(bp).collectList().block();
                assertNotNull(actual);
                assertEquals(new HashSet<>(expected), new HashSet<>(actual));
            }));
        }
        for (Future<?> f : futures)
            f.get();
    }
}