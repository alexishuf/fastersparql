package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FusekiContainer;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.FSProperties.*;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.client.model.SparqlEndpoint.parse;
import static com.github.alexishuf.fastersparql.util.Results.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameParameterValue")
@Testcontainers
public class SparqlClientTest {
    private static final Logger log = LoggerFactory.getLogger(SparqlClientTest.class);

    @Container private static final HdtssContainer HDTSS =
            new HdtssContainer(SparqlClientTest.class, "data.hdt", log);
    @Container private static final FusekiContainer FUSEKI =
            new FusekiContainer(SparqlClientTest.class, "data.ttl", log);

    private static final int REPETITIONS_PER_CLIENT = 2;
    private static final int THREADS_PER_CLIENT = Runtime.getRuntime().availableProcessors();

    private static final List<BatchType<? extends Batch<?>>> BATCH_TYPES
            = List.of(TERM, COMPRESSED);

    private static Stream<Arguments> resultsData() {
        return Stream.of(
                /* single-column, single-row results */
                //fetch a single URI
        /*  1 */results(Vars.of("x"), ":Bob")
                        .query("SELECT ?x WHERE { ?x foaf:knows :Charlie. }"),
                //fetch a single lang-tagged literal
        /*  2 */results(Vars.of("x"), "\"alice\"@en-US")
                        .query("SELECT ?x WHERE { :Alice foaf:name ?x. }"),
                //fetch a single explicit xsd:string
        /*  3 */results(Vars.of("x"), "\"bob\"")
                        .query("SELECT ?x WHERE { :Bob foaf:name ?x. }"),
                //fetch a single implicit xsd:string
        /*  4 */results(Vars.of("x"), "\"charlie\"")
                        .query("SELECT ?x WHERE { :Charlie foaf:name ?x. }"),
                //fetch a single xsd:integer
        /*  5 */results(Vars.of("x"), "23")
                        .query("SELECT ?x WHERE { :Alice foaf:age ?x. }"),
                //fetch a single xsd:int
        /*  6 */results(Vars.of("x"), "\"25\"^^xsd:int")
                        .query("SELECT ?x WHERE { :Bob foaf:age ?x. }"),
                // fetch a null
        /*  7 */results(Vars.of("x"), new Object[]{null})
                        .query("SELECT ?x WHERE { :Alice foaf:knows ?bob. OPTIONAL {?bob a ?x.}}"),

                /* zero results */
                //no results, single var
        /*  8 */results(Vars.of("x")).query("SELECT ?x WHERE { ?x foaf:knows :William. }"),
                //no results, two vars
        /*  9 */results(Vars.of("x", "y"))
                        .query("SELECT ?x ?y WHERE { ?x foaf:knows ?y. ?y foaf:knows :William }"),

                /* ASK queries */
                // positive ASK
        /* 10 */positiveResult().query("ASK { ?x foaf:knows :Bob }"),
                //negative ASK
        /* 11 */negativeResult().query("ASK { ?x foaf:knows :Williams }"),

                /* 2 columns, single row */
                // get URI and literal on single row
        /* 12 */results(Vars.of("age", "knows"), "23", ":Bob")
                        .query( "SELECT ?age ?knows WHERE {:Alice foaf:age ?age; foaf:knows ?knows. }"),
                // get string and number on single row
        /* 13 */results(Vars.of("name", "age"), "\"alice\"@en-US", "23")
                        .query("SELECT ?name ?age WHERE {:Alice foaf:age ?age; foaf:name ?name. }"),

                /* 1 column, 2 rows */
                // get two URIs
        /* 14 */results(Vars.of("x"), ":Bob", ":Charlie")
                        .query("SELECT ?x WHERE {:Bob foaf:knows ?x}"),
                // get 2 strings with newlines
        /* 15 */results(Vars.of("x"), "\"Dave\\nNewline\"", "\"Dave\\r\\nWindows\"@en-US")
                        .query("SELECT ?x WHERE {:Dave foaf:name ?x}"),

                /* 3 columns, 2 rows with nulls */
        /* 16 */results(Vars.of("name", "age"),
                            "\"Eric\\r\\nNewline\"@en", "23",  null,
                            "\"Harry\"",                 null, ":Springfield")
                        .query("""
                                SELECT ?name ?age ?where WHERE {
                                  ?x foaf:knows :Dave.
                                  OPTIONAL {?x foaf:age ?age }
                                  OPTIONAL {?x foaf:name ?name}
                                  OPTIONAL {?x foaf:based_near ?where}
                                }""")
        ).map(Arguments::arguments);
    }

    private static class ClientSet extends AutoCloseableSet<SparqlClient> {
        public ClientSet() { }

        public static ClientSet forSelect(String tag) {
            ClientSet set = new ClientSet();
            for (var fmt : List.of(SparqlResultFormat.TSV, SparqlResultFormat.JSON)) {
                for (SparqlMethod method : SparqlMethod.VALUES) {
                    if (method == SparqlMethod.WS && fmt != SparqlResultFormat.TSV)
                         continue;
                    var cfg = SparqlConfiguration.builder()
                            .clearResultsAccepts().resultsAccept(fmt)
                            .clearMethods().method(method).build();
                    List<SparqlEndpoint> eps = new ArrayList<>();
                    eps.add(HDTSS.asEndpoint(cfg));
                    if (method != SparqlMethod.WS)
                        eps.add(FUSEKI.asEndpoint(cfg));
                    for (SparqlEndpoint ep : eps)
                        set.add(FS.clientFor(ep, tag));
                }
            }
            return set;
        }

        public SparqlClient first() { return get(0); }
    }

    @ParameterizedTest @MethodSource("resultsData")
    void testResultsNetty(Results data) throws Exception {
        test(data, "netty");
    }

    private void testUnreachable(String tag) throws Exception {
        int port = -1;
        while (port == -1) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                port = serverSocket.getLocalPort();
            } catch (IOException e) {
                log.debug("Ignoring {} and retrying in 1s...", e.toString());
                Thread.sleep(1_000);
            }
        }
        try (var tasks = TestTaskSet.virtualTaskSet("testUnreachable(" + tag + ")")) {
            for (SparqlMethod meth : SparqlMethod.values()) {
                if (meth == SparqlMethod.WS) {
                    String uri = "ws://127.0.0.1:"+port+"/sparql";
                    tasks.add(() -> testUnreachable(tag, uri));
                } else {
                    for (SparqlResultFormat fmt : SparqlResultFormat.values()) {
                        if (ResultsParser.supports(fmt)) {
                            String uri = meth + "," + fmt + "@http://127.0.0.1:" + port + "/sparql";
                            tasks.add(() -> testUnreachable(tag, uri));
                        }
                    }
                }
            }
        }
    }

    private void testUnreachable(String tag, String uri) {
        try (var client = FS.clientFor(parse(uri), tag)) {
            var results = client.query(TERM, new OpaqueSparqlQuery("SELECT * WHERE { ?x a <http://example.org/Dummy>}"));
            assertEquals(List.of("x"), results.vars());
            assertThrows(FSException.class,
                    () -> assertNull(results.nextBatch(null)));
        }
    }

    @Test
    void testUnreachableNetty() throws Exception {
        testUnreachable("netty");
    }

    private void testServerEarlyClose(String tag, String uri) {
        try (var client = FS.clientFor(parse(uri), tag)) {
            var it = client.query(TERM, new OpaqueSparqlQuery("SELECT * WHERE { ?s ?p ?o}"));
            assertEquals(List.of("s", "p", "o"), it.vars());
            assertThrows(FSException.class, () -> assertNull(it.nextBatch(null)));
        }
    }

    private void testServerEarlyClose(String tag) throws Exception {
        String connRetries = System.getProperty(CLIENT_CONN_RETRIES);
        String connRetryWaitMs = System.getProperty(CLIENT_CONN_RETRY_WAIT_MS);
        String connTimeoutMs = System.getProperty(CLIENT_CONN_TIMEOUT_MS);
        System.setProperty(CLIENT_CONN_RETRIES, "2");
        System.setProperty(CLIENT_CONN_RETRY_WAIT_MS, "10");
        System.setProperty(CLIENT_CONN_TIMEOUT_MS, "100");
        try (ServerSocket server = new ServerSocket(0)) {
            int port = server.getLocalPort();
            Thread.ofVirtual().name("EarlyCloseServerHandler").start(() -> {
                try {
                    while (!server.isClosed())
                        server.accept().close();
                } catch (Throwable e) {
                    if (!server.isClosed())
                        log.error("Error before server close()", e);
                }
            });
            try (var tasks = TestTaskSet.virtualTaskSet("testServerEarlyClose(" + tag + ")")) {
                for (SparqlMethod meth : SparqlMethod.values()) {
                    if (meth == SparqlMethod.WS) {
                        String uri = "ws://127.0.0.1:"+port+"/sparql";
                        tasks.add(() -> testServerEarlyClose(tag, uri));
                    } else {
                        for (SparqlResultFormat fmt : SparqlResultFormat.values()) {
                            if (ResultsParser.supports(fmt)) {
                                String uri = meth + "," + fmt + "@http://127.0.0.1:" + port + "/sparql";
                                tasks.add(() -> testServerEarlyClose(tag, uri));
                            }
                        }
                    }
                }
            }
        } finally {
            if (connRetries == null)
                System.clearProperty(CLIENT_CONN_RETRIES);
            else
                System.setProperty(CLIENT_CONN_RETRIES, connRetries);

            if (connRetryWaitMs == null)
                System.clearProperty(CLIENT_CONN_RETRY_WAIT_MS);
            else
                System.setProperty(CLIENT_CONN_RETRY_WAIT_MS, connRetryWaitMs);

            if (connTimeoutMs == null)
                System.clearProperty(CLIENT_CONN_TIMEOUT_MS);
            else
                System.setProperty(CLIENT_CONN_TIMEOUT_MS, connTimeoutMs);
        }
    }

    @Test
    void testServerEarlyCloseNetty() throws Exception {
        testServerEarlyClose("netty");
    }

    private static Stream<Arguments> bindData() {
        List<Results> reused = new ArrayList<>();
        resultsData().forEach(args -> {
            var plain = (Results) args.get()[0];
            reused.add(plain.bindings(Vars.EMPTY).bindType(BindType.JOIN));
            if (!plain.isEmpty())
                reused.add(plain.bindType(BindType.LEFT_JOIN));
        });

        List<Results> own = new ArrayList<>();

        // expose bound values
        own.add(results("name", "x",
                        "\"alice\"@en-US", ":Alice",
                        "\"charlie\"", ":Charlie")
                .query("SELECT * WHERE {?x foaf:name ?name}")
                .bindings("?name", "\"alice\"@en-US", "\"charlie\""));
        // do not omit bound values even if query asks
        own.add(results("?name", "?x",
                        "\"alice\"@en-US", ":Alice",
                        "\"charlie\"", ":Charlie")
                .query("SELECT ?x WHERE {?x foaf:name ?name}")
                .bindings("?name", "\"alice\"@en-US", "\"charlie\""));
        // repeat 2 previous tests using LEFT_JOIN
        //noinspection ConstantConditions
        own.add(own.get(own.size()-2).bindType(BindType.LEFT_JOIN));
        own.add(own.get(own.size()-2).bindType(BindType.LEFT_JOIN));
        // exists (charlie does not match)
        own.add(results("?name", "\"bob\"")
                .query("SELECT * WHERE { ?x foaf:name ?name; foaf:knows :Bob}").
                bindings("?name", "\"bob\"", "\"charlie\"")
                .bindType(BindType.EXISTS));
        // not exists
        own.add(results("?name", "\"charlie\"")
                .query("SELECT * WHERE { ?x foaf:name ?name; foaf:knows :Bob}")
                .bindings("?name", "\"bob\"", "\"charlie\"")
                .bindType(BindType.NOT_EXISTS));
        // minus
        own.add(own.get(own.size()-1).bindType(BindType.MINUS));

        // join with two-column bindings
        own.add(results("?name", "?age", "?who",
                        "\"alice\"@en-US", "23",             ":Bob",
                        "\"bob\"",         "\"25\"^^xsd:int", ":Bob",
                        "\"bob\"",         "\"25\"^^xsd:int", ":Charlie")
                .query("SELECT ?who WHERE {?x foaf:age ?age; foaf:name ?name; foaf:knows ?who}")
                .bindType(BindType.JOIN)
                .bindings("\"alice\"@en-US", "23",
                         "\"alice\"@en-US", "25",
                         "\"bob\"",         "\"25\"^^xsd:int",
                         "\"bob\"",         "\"23\"^^xsd:int"));
        // now with a left join...
        own.add(results("?name",           "?age",            "?who",
                        "\"alice\"@en-US", "23",              ":Bob",
                        "\"alice\"@en-US", "25",              null,
                        "\"bob\"",         "\"25\"^^xsd:int", ":Bob",
                        "\"bob\"",         "\"25\"^^xsd:int", ":Charlie",
                        "\"bob\"",         "23",              null)
                .query("SELECT ?who WHERE {?x foaf:age ?age; foaf:name ?name; foaf:knows ?who}")
                .bindType(BindType.LEFT_JOIN)
                .bindings("?name",           "?age",
                          "\"alice\"@en-US", "23",
                          "\"alice\"@en-US", "25",
                          "\"bob\"",         "\"25\"^^xsd:int",
                          "\"bob\"",         "23"));
        // preserve useless binding column
        own.add(results("?x",       "?age",           "?who",
                        "\"row1\"", "23",              ":Alice",
                        "\"row1\"", "23",              ":Eric",
                        "\"row3\"", "\"25\"^^xsd:int", ":Bob")
                .query("SELECT ?who WHERE {?who foaf:age ?age.}")
                .bindType(BindType.JOIN)
                .bindings("?x",       "?age",
                          "\"row1\"", "23",
                          "\"row2\"", "\"24\"",
                          "\"row3\"", "\"25\"^^xsd:int"));
        // same but as a left join...
        own.add(results("?x",       "?age",            "?who",
                        "\"row1\"", "23",              ":Alice",
                        "\"row1\"", "23",              ":Eric",
                        "\"row2\"", "\"24\"",          null,
                        "\"row3\"", "\"25\"^^xsd:int", ":Bob")
                .query("SELECT ?who WHERE {?who foaf:age ?age.}")
                .bindType(BindType.LEFT_JOIN)
                .bindings("?x",      "?age",
                          "\"row1\"", "23",
                          "\"row2\"", "\"24\"",
                          "\"row3\"", "\"25\"^^xsd:int"));
        // preserve useless binding column (reverse order)
        own.add(results("?age",            "?x",       "?who",
                        "23",              "\"row1\"", ":Alice",
                        "23",              "\"row1\"", ":Eric",
                        "\"25\"^^xsd:int", "\"row3\"", ":Bob")
                .query("SELECT ?who WHERE {?who foaf:age ?age.}")
                .bindType(BindType.JOIN)
                .bindings("?age",            "?x",
                          "23",              "\"row1\"",
                          "\"24\"",          "\"row2\"",
                          "\"25\"^^xsd:int", "\"row3\""));
        //same as a left join
        own.add(results("?age",           "?x",       "?who",
                        "23",             "\"row1\"", ":Alice",
                        "23",             "\"row1\"", ":Eric",
                        "\"24\"",         "\"row2\"", null,
                        "\"25\"^^xsd:int","\"row3\"", ":Bob")
                .query("SELECT ?who WHERE {?who foaf:age ?age.}")
                .bindType(BindType.LEFT_JOIN)
                .bindings("?age",            "?x",
                          "23",              "\"row1\"",
                          "\"24\"",          "\"row2\"",
                          "\"25\"^^xsd:int", "\"row3\""));

        //join with long own bindings
        own.add(results("?age",           "?x",
                        "23",              ":Alice",
                        "23",              ":Eric",
                        "\"25\"^^xsd:int", ":Bob")
                .query("SELECT * WHERE {?x foaf:age ?age}")
                .bindType(BindType.JOIN)
                .bindings("?age",
                          "22",
                          "23",
                          "24",
                          "\"25\"^^xsd:int",
                          "26",
                          "27",
                          "28",
                          "29",
                          "30",
                          "31",
                          "32"));
        // same as a left join
        own.add(results("?age",            "?x",
                        "22",              null,
                        "23",              ":Alice",
                        "23",              ":Eric",
                        "24",              null,
                        "\"25\"^^xsd:int", ":Bob",
                        "26",              null,
                        "27",              null,
                        "28",              null,
                        "29",              null,
                        "30",              null,
                        "31",              null,
                        "32",              null)
                .query("SELECT * WHERE {?x foaf:age ?age}")
                .bindType(BindType.LEFT_JOIN)
                .bindings("?age",
                          "22",
                          "23",
                          "24",
                          "\"25\"^^xsd:int",
                          "26",
                          "27",
                          "28",
                          "29",
                          "30",
                          "31",
                          "32"));
        return Stream.concat(reused.stream(), own.stream()).map(Arguments::arguments);
    }

    private void test(Results d, String clientTag) throws Exception {
        try (var clients = ClientSet.forSelect(clientTag);
             var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName() + ".testBind")) {
            d.check(clients.first());
            for (SparqlClient client : clients) {
                for (var rt : BATCH_TYPES)
                    d.check(client, rt);
            }
            for (var client : clients) {
                tasks.repeat(THREADS_PER_CLIENT, thread -> {
                    for (int repetition = 0; repetition < REPETITIONS_PER_CLIENT; repetition++)
                        for (var rt : BATCH_TYPES)
                            d.check(client, rt);
                });
            }
        }
    }

    @ParameterizedTest @MethodSource("bindData")
    void testBindNetty(Results data) throws Exception {
         test(data, "netty");
    }

//    private void testGraph(GraphData data, String tag) throws Exception {
//        try (var clients = ClientSet.forGraph(tag);
//             var tasks = new VThreadTaskSet(getClass().getSimpleName()+".testGraph")) {
//            data.assertExpected(clients.first());
//            for (SparqlClient client : clients) {
//                tasks.repeat(THREADS_PER_CLIENT, thread -> {
//                    for (int repetition = 0; repetition < REPETITIONS_PER_CLIENT; repetition++)
//                        data.assertExpected(client);
//                });
//            }
//        }
//    }
//
//    static Stream<Arguments> graphData() {
//        return Stream.of(
//                // only URIs
//                graph("CONSTRUCT {:Graph :hasPerson ?x} WHERE {?x ?p ?o}",
//                      ":Graph  :hasPerson  :Alice, :Eric, :Dave, :Harry, :Bob, :Charlie."),
//                //list names
//                graph("CONSTRUCT {:Graph :hasName ?x} WHERE {?s foaf:name ?x}",
//                      """
//                          :Graph  :hasName  "alice"@en-US, "bob"^^xsd:string, "charlie",
//                          "Dave\\nNewline", "Dave\\r\\nWindows"@en-US,
//                          "Eric\\r\\nNewline"@en, "Harry"."""),
//                //list ages
//                graph("CONSTRUCT {:Graph :hasAge ?x} WHERE {?s foaf:age ?x}",
//                      ":Graph  :hasAge  23, \"25\"^^xsd:int.")
//        ).map(Arguments::arguments);
//    }
//
//    @ParameterizedTest @MethodSource("graphData")
//    void testGraphNetty(GraphData data) throws Exception {
//        testGraph(data, "netty");
//    }
}
