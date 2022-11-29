package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.FusekiContainer;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.client.model.row.types.CharSequencesRow;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.StringFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.ResultsParserRegistry;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
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

import static com.github.alexishuf.fastersparql.client.GraphData.graph;
import static com.github.alexishuf.fastersparql.client.ResultsData.results;
import static com.github.alexishuf.fastersparql.client.util.FSProperties.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    private static Stream<Arguments> resultsData() {
        return Stream.of(
                /* single-column, single-row results */
                //fetch a single URI
        /*  1 */results("SELECT ?x WHERE { ?x foaf:knows :Charlie. }", "<$:Bob>"),
                //fetch a single lang-tagged literal
        /*  2 */results("SELECT ?x WHERE { :Alice foaf:name ?x. }", "\"alice\"@en-US"),
                //fetch a single explicit xsd:string
        /*  3 */results("SELECT ?x WHERE { :Bob foaf:name ?x. }", "\"bob\""),
                //fetch a single implicit xsd:string
        /*  4 */results("SELECT ?x WHERE { :Charlie foaf:name ?x. }", "\"charlie\""),
                //fetch a single xsd:integer
        /*  5 */results("SELECT ?x WHERE { :Alice foaf:age ?x. }", "\"23\"^^<$xsd:integer>"),
                //fetch a single xsd:int
        /*  6 */results("SELECT ?x WHERE { :Bob foaf:age ?x. }", "\"25\"^^<$xsd:int>"),
                // fetch a null
        /*  7 */results("SELECT ?x WHERE { :Alice foaf:knows ?bob. OPTIONAL {?bob a ?x.}}", "$null"),

                /* zero results */
                //no results, single var
        /*  8 */results("SELECT ?x WHERE { ?x foaf:knows :William. }"),
                //no results, two vars
        /*  9 */results("SELECT ?x ?y WHERE { ?x foaf:knows ?y. ?y foaf:knows :William }"),

                /* ASK queries */
                // positive ASK
        /* 10 */results("ASK { ?x foaf:knows :Bob }", true),
                //negative ASK
        /* 11 */results("ASK { ?x foaf:knows :Williams }", false),

                /* 2 columns, single row */
                // get URI and literal on single row
        /* 12 */results("SELECT ?age ?knows WHERE {:Alice foaf:age ?age; foaf:knows ?knows. }",
                        "\"23\"^^<$xsd:integer>", "<$:Bob>"),
                // get string and number on single row
        /* 13 */results("SELECT ?name ?age WHERE {:Alice foaf:age ?age; foaf:name ?name. }",
                        "\"alice\"@en-US", "\"23\"^^<$xsd:integer>"),

                /* 1 column, 2 rows */
                // get two URIs
        /* 14 */results("SELECT ?x WHERE {:Bob foaf:knows ?x}", "<$:Bob>", "<$:Charlie>"),
                // get 2 strings with newlines
        /* 15 */results("SELECT ?x WHERE {:Dave foaf:name ?x}",
                        "\"Dave\\nNewline\"", "\"Dave\\r\\nWindows\"@en-US"),

                /* 3 columns, 2 rows with nulls */
        /* 16 */results("""
                        SELECT ?name ?age ?where WHERE {
                          ?x foaf:knows :Dave.
                          OPTIONAL {?x foaf:age ?age }
                          OPTIONAL {?x foaf:name ?name}
                          OPTIONAL {?x foaf:based_near ?where}
                        }""",
                        "\"Eric\\r\\nNewline\"@en", "\"23\"^^<$xsd:integer>", "$null",
                        "\"Harry\"",                "$null",                  "<$:Springfield>")
        ).map(Arguments::arguments);
    }

    private record ClientSet(List<SparqlClient<?, ?, ?>> list) implements AutoCloseable {
        public static ClientSet forSelect(String tag) {
            List<SparqlClient<?,?,?>> list = new ArrayList<>();
            SparqlClientFactory fac = FS.factory(tag);
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
                    for (SparqlEndpoint ep : eps) {
                        for (var rt : List.of(ArrayRow.STRING, ListRow.STRING,
                                              CharSequencesRow.INSTANCE)) {
                            list.add(fac.createFor(ep, rt));
                        }
                    }
                }
            }
            return new ClientSet(list);
        }

        public static ClientSet forGraph(String tag) {
            List<SparqlClient<?,?,?>> list = new ArrayList<>();
            var fac = FS.factory(tag);
            for (MediaType mt : RDFMediaTypes.DEFAULT_ACCEPTS) {
                for (SparqlMethod method : SparqlMethod.VALUES) {
                    if (method == SparqlMethod.WS) continue;
                    var cfg = SparqlConfiguration.builder()
                            .clearRdfAccepts().rdfAccept(mt)
                            .clearMethods().method(method).build();
                    var ep = FUSEKI.asEndpoint(cfg);
                    list.add(fac.createFor(ep, ByteArrayFragmentParser.INSTANCE));
                    list.add(fac.createFor(ep, StringFragmentParser.INSTANCE));
                }
            }
            return new ClientSet(list);
        }

        public SparqlClient<?,?,?> first() { return list.get(0); }

        @Override public void close() {
            for (SparqlClient<?, ?, ?> client : list)
                client.close();
        }
    }

    private void testResults(ResultsData d, String clientTag) throws Exception {
        try (ClientSet clients = ClientSet.forSelect(clientTag);
             var tasks = new VThreadTaskSet(getClass().getSimpleName() + ".testResults")) {
            d.assertExpected(clients.first().query(d.sparql()));
            for (SparqlClient<?, ?, ?> client : clients.list) {
                tasks.repeat(THREADS_PER_CLIENT, thread -> {
                    for (int repetition = 0; repetition < REPETITIONS_PER_CLIENT; repetition++)
                        d.assertExpected(client.query(d.sparql()));
                });
            }
        }
    }

    @ParameterizedTest @MethodSource("resultsData")
    void testResultsNetty(ResultsData data) throws Exception {
        testResults(data, "netty");
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
        try (var tasks = new VThreadTaskSet("testUnreachable(" + tag + ")")) {
            for (SparqlMethod meth : SparqlMethod.values()) {
                if (meth == SparqlMethod.WS) {
                    String uri = "ws://127.0.0.1:"+port+"/sparql";
                    tasks.add(() -> testUnreachable(tag, uri));
                } else {
                    for (SparqlResultFormat fmt : SparqlResultFormat.values()) {
                        if (ResultsParserRegistry.get().canParse(fmt.asMediaType())) {
                            String uri = meth + "," + fmt + "@http://127.0.0.1:" + port + "/sparql";
                            tasks.add(() -> testUnreachable(tag, uri));
                        }
                    }
                }
            }
        }
    }

    private void testUnreachable(String tag, String uri) {
        try (var client = FS.factory(tag).createFor(SparqlEndpoint.parse(uri))) {
            var results = client.query(new OpaqueSparqlQuery("SELECT * WHERE { ?x a <http://example.org/Dummy>}"));
            assertEquals(List.of("x"), results.vars());
            assertThrows(SparqlClientException.class,
                    () -> assertEquals(0, results.nextBatch().size));
        }
    }

    @Test
    void testUnreachableNetty() throws Exception {
        testUnreachable("netty");
    }

    private void testServerEarlyClose(String tag, String uri) {
        try (var client = FS.factory(tag).createFor(SparqlEndpoint.parse(uri))) {
            var it = client.query(new OpaqueSparqlQuery("SELECT * WHERE { ?s ?p ?o}"));
            assertEquals(List.of("s", "p", "o"), it.vars());
            assertThrows(SparqlClientException.class, () -> assertEquals(0, it.nextBatch().size));
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
            try (var tasks = new VThreadTaskSet("testServerEarlyClose(" + tag + ")")) {
                for (SparqlMethod meth : SparqlMethod.values()) {
                    if (meth == SparqlMethod.WS) {
                        String uri = "ws://127.0.0.1:"+port+"/sparql";
                        tasks.add(() -> testServerEarlyClose(tag, uri));
                    } else {
                        for (SparqlResultFormat fmt : SparqlResultFormat.values()) {
                            if (ResultsParserRegistry.get().canParse(fmt.asMediaType())) {
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
        List<BindData> reused = new ArrayList<>();
        resultsData().forEach(args -> {
            BindData d = new BindData((ResultsData) args.get()[0]);
            reused.add(d);
            if (!d.expected().isEmpty())
                reused.add(new BindData(d).bindType(BindType.LEFT_JOIN));
        });

        List<BindData> own = new ArrayList<>();
        // expose bound values
        own.add(BindData.join("SELECT * WHERE {?x foaf:name ?name}", "name")
                .to("\"alice\"@en-US", "\"charlie\"")
                .expecting("\"alice\"@en-US", "<$:Alice>", "\"charlie\"", "<$:Charlie>"));
        // do not omit bound values even if query asks
        own.add(BindData.join("SELECT ?x WHERE {?x foaf:name ?name}", "name")
                .to("\"alice\"@en-US", "\"charlie\"")
                .expecting("\"alice\"@en-US", "<$:Alice>", "\"charlie\"", "<$:Charlie>"));
        // repeat 2 previous tests using LEFT_JOIN
        //noinspection ConstantConditions
        own.add(new BindData(own.get(own.size()-2)).bindType(BindType.LEFT_JOIN));
        own.add(new BindData(own.get(own.size()-2)).bindType(BindType.LEFT_JOIN));
        // exists (charlie does not match)
        own.add(BindData.exists("SELECT * WHERE { ?x foaf:name ?name; foaf:knows :Bob}", "name")
                .to("\"bob\"", "\"charlie\"").expecting("\"bob\""));
        // not exists
        own.add(BindData.notExists("SELECT * WHERE { ?x foaf:name ?name; foaf:knows :Bob}", "name")
                .to("\"bob\"", "\"charlie\"").expecting("\"charlie\""));
        // minus
        own.add(new BindData(own.get(own.size()-1)).bindType(BindType.MINUS));

        // join with two-column bindings
        own.add(BindData.join("SELECT ?who WHERE {?x foaf:age ?age; foaf:name ?name; foaf:knows ?who}",
                               "name", "age")
                         .to("\"alice\"@en-US", "\"23\"^^<$xsd:integer>",
                                    "\"alice\"@en-US", "\"25\"^^<$xsd:integer>",
                                    "\"bob\"", "\"25\"^^<$xsd:int>",
                                    "\"bob\"", "\"23\"^^<$xsd:int>")
                         .expecting("\"alice\"@en-US", "\"23\"^^<$xsd:integer>", "<$:Bob>",
                                 "\"bob\"", "\"25\"^^<$xsd:int>", "<$:Bob>",
                                 "\"bob\"", "\"25\"^^<$xsd:int>", "<$:Charlie>"));
        // now with a left join...
        own.add(BindData.leftJoin("SELECT ?who WHERE {?x foaf:age ?age; foaf:name ?name; foaf:knows ?who}",
                        "name", "age")
                .to("\"alice\"@en-US", "\"23\"^^<$xsd:integer>",
                        "\"alice\"@en-US", "\"25\"^^<$xsd:integer>",
                        "\"bob\"", "\"25\"^^<$xsd:int>",
                        "\"bob\"", "\"23\"^^<$xsd:integer>")
                .expecting("\"alice\"@en-US", "\"23\"^^<$xsd:integer>", "<$:Bob>",
                        "\"alice\"@en-US", "\"25\"^^<$xsd:integer>", null,
                        "\"bob\"", "\"25\"^^<$xsd:int>", "<$:Bob>",
                        "\"bob\"", "\"25\"^^<$xsd:int>", "<$:Charlie>",
                        "\"bob\"", "\"23\"^^<$xsd:integer>", null));
        // preserve useless binding column
        own.add(BindData.join("SELECT ?who WHERE {?who foaf:age ?age.}",
                               "x", "age")
                .to("\"row1\"", "\"23\"^^<$xsd:integer>",
                    "\"row2\"", "\"24\"",
                    "\"row3\"", "\"25\"^^<$xsd:int>")
                .expecting("\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Alice>",
                           "\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Eric>",
                           "\"row3\"", "\"25\"^^<$xsd:int>", "<$:Bob>"));
        // same but as a left join...
        own.add(BindData.leftJoin("SELECT ?who WHERE {?who foaf:age ?age.}",
                        "x", "age")
                .to("\"row1\"", "\"23\"^^<$xsd:integer>",
                    "\"row2\"", "\"24\"",
                    "\"row3\"", "\"25\"^^<$xsd:int>")
                .expecting("\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Alice>",
                           "\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Eric>",
                           "\"row2\"", "\"24\"",                 null,
                           "\"row3\"", "\"25\"^^<$xsd:int>",     "<$:Bob>"));
        // preserve useless binding column (reverse order)
        own.add(BindData.join("SELECT ?who WHERE {?who foaf:age ?age.}",
                        "age", "x")
                .to("\"23\"^^<$xsd:integer>", "\"row1\"",
                    "\"24\"",                    "\"row2\"",
                    "\"25\"^^<$xsd:int>",        "\"row3\"")
                .expecting("\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Alice>",
                           "\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Eric>",
                           "\"25\"^^<$xsd:int>",     "\"row3\"", "<$:Bob>"));
        //same as a left join
        own.add(BindData.leftJoin("SELECT ?who WHERE {?who foaf:age ?age.}",
                        "age", "x")
                .to("\"23\"^^<$xsd:integer>",    "\"row1\"",
                    "\"24\"",                    "\"row2\"",
                    "\"25\"^^<$xsd:int>",        "\"row3\"")
                .expecting("\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Alice>",
                           "\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Eric>",
                           "\"24\"",                 "\"row2\"", null,
                           "\"25\"^^<$xsd:int>",     "\"row3\"", "<$:Bob>"));

        //join with long own bindings
        own.add(BindData.join("SELECT * WHERE {?x foaf:age ?age}", "age")
                         .to("\"22\"^^<$xsd:integer>",
                                 "\"23\"^^<$xsd:integer>",
                                 "\"24\"^^<$xsd:integer>",
                                 "\"25\"^^<$xsd:int>",
                                 "\"26\"^^<$xsd:integer>",
                                 "\"27\"^^<$xsd:integer>",
                                 "\"28\"^^<$xsd:integer>",
                                 "\"29\"^^<$xsd:integer>",
                                 "\"30\"^^<$xsd:integer>",
                                 "\"31\"^^<$xsd:integer>",
                                 "\"32\"^^<$xsd:integer>")
                         .expecting("\"23\"^^<$xsd:integer>", "<$:Alice>",
                                 "\"23\"^^<$xsd:integer>", "<$:Eric>",
                                 "\"25\"^^<$xsd:int>", "<$:Bob>"));
        // same as a left join
        own.add(BindData.leftJoin("SELECT * WHERE {?x foaf:age ?age}", "age")
                .to("\"22\"^^<$xsd:integer>",
                        "\"23\"^^<$xsd:integer>",
                        "\"24\"^^<$xsd:integer>",
                        "\"25\"^^<$xsd:int>",
                        "\"26\"^^<$xsd:integer>",
                        "\"27\"^^<$xsd:integer>",
                        "\"28\"^^<$xsd:integer>",
                        "\"29\"^^<$xsd:integer>",
                        "\"30\"^^<$xsd:integer>",
                        "\"31\"^^<$xsd:integer>",
                        "\"32\"^^<$xsd:integer>")
                .expecting("\"22\"^^<$xsd:integer>", null,
                        "\"23\"^^<$xsd:integer>", "<$:Alice>",
                        "\"23\"^^<$xsd:integer>", "<$:Eric>",
                        "\"24\"^^<$xsd:integer>", null,
                        "\"25\"^^<$xsd:int>", "<$:Bob>",
                        "\"26\"^^<$xsd:integer>", null,
                        "\"27\"^^<$xsd:integer>", null,
                        "\"28\"^^<$xsd:integer>", null,
                        "\"29\"^^<$xsd:integer>", null,
                        "\"30\"^^<$xsd:integer>", null,
                        "\"31\"^^<$xsd:integer>", null,
                        "\"32\"^^<$xsd:integer>", null));
        return Stream.concat(reused.stream(), own.stream()).map(Arguments::arguments);
    }

    private void testBind(BindData d, String clientTag) throws Exception {
        try (var clients = ClientSet.forGraph(clientTag);
             var tasks = new VThreadTaskSet(getClass().getSimpleName() + ".testBind")) {
            d.assertExpected(d.query(clients.first()));
            for (var client : clients.list) {
                tasks.repeat(THREADS_PER_CLIENT, thread -> {
                    for (int repetition = 0; repetition < REPETITIONS_PER_CLIENT; repetition++)
                        d.assertExpected(d.query(client));
                });
            }
        }
    }

    @ParameterizedTest @MethodSource("bindData")
    void testBindNetty(BindData data) throws Exception {
         testBind(data, "netty");
    }

    private void testGraph(GraphData data, String tag) throws Exception {
        try (var clients = ClientSet.forGraph(tag);
             var tasks = new VThreadTaskSet(getClass().getSimpleName()+".testGraph")) {
            data.assertExpected(clients.first());
            for (SparqlClient<?, ?, ?> client : clients.list) {
                tasks.repeat(THREADS_PER_CLIENT, thread -> {
                    for (int repetition = 0; repetition < REPETITIONS_PER_CLIENT; repetition++)
                        data.assertExpected(client);
                });
            }
        }
    }

    static Stream<Arguments> graphData() {
        return Stream.of(
                // only URIs
                graph("CONSTRUCT {:Graph :hasPerson ?x} WHERE {?x ?p ?o}",
                      ":Graph  :hasPerson  :Alice, :Eric, :Dave, :Harry, :Bob, :Charlie."),
                //list names
                graph("CONSTRUCT {:Graph :hasName ?x} WHERE {?s foaf:name ?x}",
                      """
                          :Graph  :hasName  "alice"@en-US, "bob"^^xsd:string, "charlie",
                          "Dave\\nNewline", "Dave\\r\\nWindows"@en-US,
                          "Eric\\r\\nNewline"@en, "Harry"."""),
                //list ages
                graph("CONSTRUCT {:Graph :hasAge ?x} WHERE {?s foaf:age ?x}",
                      ":Graph  :hasAge  23, \"25\"^^xsd:int.")
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("graphData")
    void testGraphNetty(GraphData data) throws Exception {
        testGraph(data, "netty");
    }
}
