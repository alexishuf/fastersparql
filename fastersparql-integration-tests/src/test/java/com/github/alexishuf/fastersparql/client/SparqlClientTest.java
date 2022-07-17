package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.FusekiContainer;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.CharSequenceFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.StringFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.ResultsParserRegistry;
import com.github.alexishuf.fastersparql.client.parser.row.*;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.GraphData.graph;
import static com.github.alexishuf.fastersparql.client.ResultsData.results;
import static com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties.*;
import static com.github.alexishuf.fastersparql.client.util.async.Async.async;
import static com.github.alexishuf.fastersparql.client.util.async.Async.asyncThrowing;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("SameParameterValue")
@Testcontainers
@Slf4j
public class SparqlClientTest {

    @Container private static final HdtssContainer HDTSS =
            new HdtssContainer(SparqlClientTest.class, "data.hdt", log);
    @Container private static final FusekiContainer FUSEKI =
            new FusekiContainer(SparqlClientTest.class, "data.ttl", log);

    private static final List<RowParser<?>> ROW_PARSERS = asList(StringListRowParser.INSTANCE,
            CharSequenceListRowParser.INSTANCE, CharSequenceArrayRowParser.INSTANCE);
    private static final ByteArrayFragmentParser BYTES_P = ByteArrayFragmentParser.INSTANCE;

    private static Stream<Arguments> resultsData() {
        List<ResultsData> seed = asList(
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
        /* 16 */results("SELECT ?name ?age ?where WHERE {\n" +
                        "  ?x foaf:knows :Dave.\n" +
                        "  OPTIONAL {?x foaf:age ?age }\n" +
                        "  OPTIONAL {?x foaf:name ?name}\n" +
                        "  OPTIONAL {?x foaf:based_near ?where}\n" +
                        "}",
                        "\"Eric\\r\\nNewline\"@en", "\"23\"^^<$xsd:integer>", "$null",
                        "\"Harry\"",                "$null",                  "<$:Springfield>")
        );
        List<ResultsData> expanded = new ArrayList<>();
        for (SparqlResultFormat format : asList(SparqlResultFormat.TSV, SparqlResultFormat.JSON)) {
            for (SparqlMethod method : SparqlMethod.VALUES) {
                if (method == SparqlMethod.WS && format != SparqlResultFormat.TSV)
                    continue;
                for (ResultsData base : seed) {
                    expanded.add(base.with(b -> b.clearResultsAccepts().resultsAccept(format)
                                                 .clearMethods().method(method)));
                }
            }
        }
        return expanded.stream().map(Arguments::arguments);
    }

    private List<SparqlEndpoint> endpoints(boolean isWs, boolean allowFuseki) {
        List<SparqlEndpoint> list = new ArrayList<>();
        list.add(isWs ? asWs(HDTSS.asEndpoint()) : HDTSS.asEndpoint());
        if (allowFuseki && !isWs)
            list.add(FUSEKI.asEndpoint());
        return list;
    }

    private boolean hasMethod(SparqlClient<?, ?> client, SparqlConfiguration request) {
        List<SparqlMethod> allowed = client.endpoint().configuration().methods();
        return request.methods().stream().anyMatch(allowed::contains);
    }

    private @Nullable SparqlEndpoint asWs(SparqlEndpoint ep) {
        if (!ep.uri().equals(HDTSS.asEndpoint().uri()))
            return null;
        String uri = ep.uri().replaceAll("http://", "ws://");
        val wsConfig = ep.configuration().toBuilder().clearMethods().method(SparqlMethod.WS).build();
        return new SparqlEndpoint(uri, wsConfig);
    }

    private void testResults(ResultsData d, String clientTag) throws ExecutionException {
        int threads = Runtime.getRuntime().availableProcessors() + 1;
        List<AsyncTask<?>> futures = new ArrayList<>();
        SparqlClientFactory factory = FasterSparql.factory(clientTag);
        List<SparqlClient<?, ?>> clients = new ArrayList<>();
        try {
            for (SparqlEndpoint ep : endpoints(d.isWs(), true)) {
                SparqlClient<String[], byte[]> client = factory.createFor(ep);
                if (hasMethod(client, d.config())) {
                    clients.add(client);
                    d.assertExpected(client.query(d.sparql(), d.config()));
                    // a second query should work
                    d.assertExpected(client.query(d.sparql(), d.config()));
                }
            }
            // concurrent queries using no row parser
            for (SparqlClient<?, ?> client : clients) {
                if (client.endpoint().uri().equals(FUSEKI.asEndpoint().uri()))
                    continue;
                for (int i = 0; i < threads; i++)
                    futures.add(async(() -> d.assertExpected(client.query(d.sparql(), d.config()))));
            }

            // concurrent queries for each (endpoint, rowParser) pair
            for (SparqlEndpoint ep : endpoints(d.isWs(), false)) {
                for (RowParser<?> rowParser : ROW_PARSERS) {
                    futures.add(async(() -> {
                        try (SparqlClient<?, ?> client = factory.createFor(ep, rowParser, BYTES_P)) {
                            if (hasMethod(client, d.config()))
                                d.assertExpected(client.query(d.sparql(), d.config()));
                        }
                    }));
                }
            }
            // check all concurrent queries
            for (AsyncTask<?> future : futures)
                future.get();
        } finally {
            for (SparqlClient<?, ?> client : clients) client.close();
        }
    }

    @ParameterizedTest @MethodSource("resultsData")
    void testResultsNetty(ResultsData data) throws ExecutionException {
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
        List<AsyncTask<?>> tasks = new ArrayList<>();
        for (SparqlMethod meth : SparqlMethod.values()) {
            if (meth == SparqlMethod.WS) {
                String uri = "ws://127.0.0.1:"+port+"/sparql";
                tasks.add(async(() -> testUnreachable(tag, uri)));
            } else {
                for (SparqlResultFormat fmt : SparqlResultFormat.values()) {
                    if (ResultsParserRegistry.get().canParse(fmt.asMediaType())) {
                        String uri = meth + "," + fmt + "@http://127.0.0.1:" + port + "/sparql";
                        tasks.add(Async.async(() -> testUnreachable(tag, uri)));
                    }
                }
            }
        }
        for (AsyncTask<?> task : tasks) task.get();
    }

    private void testUnreachable(String tag, String uri) {
        try (val client = FasterSparql.factory(tag).createFor(SparqlEndpoint.parse(uri))) {
            val results = client.query("SELECT * WHERE { ?x a <http://example.org/Dummy>}");
            assertEquals(singletonList("x"), results.vars());
            val adapter = new IterableAdapter<>(results.publisher());
            boolean empty = !adapter.iterator().hasNext();
            assertTrue(empty);
            assertTrue(adapter.hasError());
        }
    }

    @Test
    void testUnreachableNetty() throws Exception {
        testUnreachable("netty");
    }

    private void testServerEarlyClose(String tag, String uri) {
        try (val client = FasterSparql.factory(tag).createFor(SparqlEndpoint.parse(uri))) {
            Results<String[]> results = client.query("SELECT * WHERE { ?s ?p ?o}");
            assertEquals(asList("s", "p", "o"), results.vars());
            val adapter = new IterableAdapter<>(results.publisher());
            boolean empty = !adapter.iterator().hasNext();
            assertTrue(empty);
            assertTrue(adapter.hasError());
        }
    }

    private void testServerEarlyClose(String tag) throws Exception {
        String connRetries = System.getProperty(CLIENT_CONN_RETRIES);
        String connRetryWaitMs = System.getProperty(CLIENT_CONN_RETRY_WAIT_MS);
        String connTimeoutMs = System.getProperty(CLIENT_CONN_TIMEOUT_MS);
        System.setProperty(CLIENT_CONN_RETRIES, "2");
        System.setProperty(CLIENT_CONN_RETRY_WAIT_MS, "10");
        System.setProperty(CLIENT_CONN_TIMEOUT_MS, "100");
        AsyncTask<?> serverTask;
        try (ServerSocket server = new ServerSocket(0)) {
            int port = server.getLocalPort();
            serverTask = asyncThrowing(() -> {
                try {
                    while (!server.isClosed())
                        server.accept().close();
                } catch (SocketException e) {
                    if (!server.isClosed()) // do not publish "Socket closed" exceptions
                        throw e;
                }
            });
            List<AsyncTask<?>> tasks = new ArrayList<>();
            for (SparqlMethod meth : SparqlMethod.values()) {
                if (meth == SparqlMethod.WS) {
                    String uri = "ws://127.0.0.1:"+port+"/sparql";
                    tasks.add(async(() -> testServerEarlyClose(tag, uri)));
                } else {
                    for (SparqlResultFormat fmt : SparqlResultFormat.values()) {
                        if (ResultsParserRegistry.get().canParse(fmt.asMediaType())) {
                            String uri = meth + "," + fmt + "@http://127.0.0.1:" + port + "/sparql";
                            tasks.add(async(() -> testServerEarlyClose(tag, uri)));
                        }
                    }
                }
            }
            for (AsyncTask<?> task : tasks)
                task.get();
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
        if (serverTask != null)
            serverTask.get();
    }

    @Test
    void testServerEarlyClose() throws Exception {
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

        List<BindData> base = new ArrayList<>();
        // expose bound values
        base.add(BindData.join("SELECT * WHERE {?x foaf:name ?name}", "name")
                .to("\"alice\"@en-US", "\"charlie\"")
                .expecting("\"alice\"@en-US", "<$:Alice>", "\"charlie\"", "<$:Charlie>"));
        // do not omit bound values even if query asks
        base.add(BindData.join("SELECT ?x WHERE {?x foaf:name ?name}", "name")
                .to("\"alice\"@en-US", "\"charlie\"")
                .expecting("\"alice\"@en-US", "<$:Alice>", "\"charlie\"", "<$:Charlie>"));
        // repeat 2 previous tests using LEFT_JOIN
        //noinspection ConstantConditions
        base.add(new BindData(base.get(base.size()-2)).bindType(BindType.LEFT_JOIN));
        base.add(new BindData(base.get(base.size()-2)).bindType(BindType.LEFT_JOIN));
        // exists (charlie does not match)
        base.add(BindData.exists("SELECT * WHERE { ?x foaf:name ?name; foaf:knows :Bob}", "name")
                .to("\"bob\"", "\"charlie\"").expecting("\"bob\""));
        // not exists
        base.add(BindData.notExists("SELECT * WHERE { ?x foaf:name ?name; foaf:knows :Bob}", "name")
                .to("\"bob\"", "\"charlie\"").expecting("\"charlie\""));
        // minus
        base.add(new BindData(base.get(base.size()-1)).bindType(BindType.MINUS));

        // join with two-column bindings
        base.add(BindData.join("SELECT ?who WHERE {?x foaf:age ?age; foaf:name ?name; foaf:knows ?who}",
                               "name", "age")
                         .to("\"alice\"@en-US", "\"23\"^^<$xsd:integer>",
                                    "\"alice\"@en-US", "\"25\"^^<$xsd:integer>",
                                    "\"bob\"", "\"25\"^^<$xsd:int>",
                                    "\"bob\"", "\"23\"^^<$xsd:int>")
                         .expecting("\"alice\"@en-US", "\"23\"^^<$xsd:integer>", "<$:Bob>",
                                 "\"bob\"", "\"25\"^^<$xsd:int>", "<$:Bob>",
                                 "\"bob\"", "\"25\"^^<$xsd:int>", "<$:Charlie>"));
        // now with a left join...
        base.add(BindData.leftJoin("SELECT ?who WHERE {?x foaf:age ?age; foaf:name ?name; foaf:knows ?who}",
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
        base.add(BindData.join("SELECT ?who WHERE {?who foaf:age ?age.}",
                               "x", "age")
                .to("\"row1\"", "\"23\"^^<$xsd:integer>",
                    "\"row2\"", "\"24\"",
                    "\"row3\"", "\"25\"^^<$xsd:int>")
                .expecting("\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Alice>",
                           "\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Eric>",
                           "\"row3\"", "\"25\"^^<$xsd:int>", "<$:Bob>"));
        // same but as a left join...
        base.add(BindData.leftJoin("SELECT ?who WHERE {?who foaf:age ?age.}",
                        "x", "age")
                .to("\"row1\"", "\"23\"^^<$xsd:integer>",
                    "\"row2\"", "\"24\"",
                    "\"row3\"", "\"25\"^^<$xsd:int>")
                .expecting("\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Alice>",
                           "\"row1\"", "\"23\"^^<$xsd:integer>", "<$:Eric>",
                           "\"row2\"", "\"24\"",                 null,
                           "\"row3\"", "\"25\"^^<$xsd:int>",     "<$:Bob>"));
        // preserve useless binding column (reverse order)
        base.add(BindData.join("SELECT ?who WHERE {?who foaf:age ?age.}",
                        "age", "x")
                .to("\"23\"^^<$xsd:integer>", "\"row1\"",
                    "\"24\"",                    "\"row2\"",
                    "\"25\"^^<$xsd:int>",        "\"row3\"")
                .expecting("\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Alice>",
                           "\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Eric>",
                           "\"25\"^^<$xsd:int>",     "\"row3\"", "<$:Bob>"));
        //same as a left join
        base.add(BindData.leftJoin("SELECT ?who WHERE {?who foaf:age ?age.}",
                        "age", "x")
                .to("\"23\"^^<$xsd:integer>",    "\"row1\"",
                    "\"24\"",                    "\"row2\"",
                    "\"25\"^^<$xsd:int>",        "\"row3\"")
                .expecting("\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Alice>",
                           "\"23\"^^<$xsd:integer>", "\"row1\"", "<$:Eric>",
                           "\"24\"",                 "\"row2\"", null,
                           "\"25\"^^<$xsd:int>",     "\"row3\"", "<$:Bob>"));

        //join with long own bindings
        base.add(BindData.join("SELECT * WHERE {?x foaf:age ?age}", "age")
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
        base.add(BindData.leftJoin("SELECT * WHERE {?x foaf:age ?age}", "age")
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
        List<BindData> own = new ArrayList<>();
        for (SparqlResultFormat fmt : asList(SparqlResultFormat.TSV, SparqlResultFormat.JSON)) {
            for (SparqlMethod method : SparqlMethod.VALUES) {
                if (method == SparqlMethod.WS && fmt != SparqlResultFormat.TSV)
                    continue;
                for (BindData d : base) {
                    own.add(d.with(b -> b.clearResultsAccepts().resultsAccept(fmt)
                                         .clearMethods().method(method)));
                }
            }
        }
        assert own.size() == base.size() * ( (3 * 2) + 1 );
        //                                    ^   ^    +--> WS/TSV
        //                                    |   +-------> TSV,JSON
        //                                    +-----------> POST,FORM,GET
        return Stream.concat(reused.stream(), own.stream()).map(Arguments::arguments);
    }

    private void testBind(BindData d, String clientTag) throws ExecutionException {
        int threads = Runtime.getRuntime().availableProcessors() + 1;
        SparqlClientFactory factory = FasterSparql.factory(clientTag);
        List<SparqlClient<String[], ?>> clients = new ArrayList<>();
        List<AsyncTask<?>> futures = new ArrayList<>();
        boolean allowFuseki = d.config().methods().equals(singletonList(SparqlMethod.POST))
                && d.config().resultsAccepts().equals(singletonList(SparqlResultFormat.TSV));
        try {
            for (SparqlEndpoint ep : endpoints(d.isWs(), allowFuseki)) {
                SparqlClient<String[], byte[]> client = factory.createFor(ep);
                if (hasMethod(client, d.config())) {
                    clients.add(client);
                    d.assertExpected(d.query(client));
                    d.assertExpected(d.query(client)); // follow-up query also works
                }
            }
            //concurrent queries without a row parser
            for (SparqlClient<String[], ?> client : clients) {
                if (client.endpoint().uri().equals(FUSEKI.asEndpoint().uri()))
                    continue;
                for (int i = 0; i < threads; i++)
                    futures.add(async(() -> d.assertExpected(d.query(client))));
            }
            //concurrent queries with a row parser
            for (SparqlEndpoint ep : endpoints(d.isWs(), false)) {
                for (RowParser<?> rp : ROW_PARSERS) {
                    futures.add(async(() -> {
                        try (SparqlClient<?, byte[]> client = factory.createFor(ep, rp, BYTES_P)) {
                            if (hasMethod(client, d.config()))
                                d.assertExpected(d.query(client, rp));
                        }
                    }));
                }
            }
            for (AsyncTask<?> f : futures)
                f.get();
        } finally {
            for (SparqlClient<?, ?> client : clients) client.close();
        }
    }

    @ParameterizedTest @MethodSource("bindData")
    void testBindNetty(BindData data) throws ExecutionException {
        testBind(data, "netty");
    }

    private void testGraph(GraphData data, String tag) throws ExecutionException {
        SparqlClientFactory factory = FasterSparql.factory(tag);
        try (SparqlClient<String[], byte[]> client = factory.createFor(FUSEKI.asEndpoint())) {
            if (!client.endpoint().configuration().methods().contains(data.method()))
                return; // skip as method is not supported
            data.assertExpected(client.queryGraph(data.sparql(), data.config()));
        }
        List<AsyncTask<?>> futures = new ArrayList<>();
        StringArrayRowParser rowP = StringArrayRowParser.INSTANCE;
        for (FragmentParser<?> fragP : asList(CharSequenceFragmentParser.INSTANCE,
                StringFragmentParser.INSTANCE)) {
            futures.add(async(() -> {
                try (SparqlClient<String[], ?> client =
                             factory.createFor(FUSEKI.asEndpoint(), rowP, fragP)) {
                    data.assertExpected(client.queryGraph(data.sparql(), data.config()));
                }
            }));
        }
        for (AsyncTask<?> task : futures)
            task.get();
    }

    static Stream<Arguments> graphData() {
        List<GraphData> seed = asList(
                // only URIs
                graph("CONSTRUCT {:Graph :hasPerson ?x} WHERE {?x ?p ?o}",
                      ":Graph  :hasPerson  :Alice, :Eric, :Dave, :Harry, :Bob, :Charlie."),
                //list names
                graph("CONSTRUCT {:Graph :hasName ?x} WHERE {?s foaf:name ?x}",
                      ":Graph  :hasName  \"alice\"@en-US, \"bob\"^^xsd:string, \"charlie\",\n" +
                            "\"Dave\\nNewline\", \"Dave\\r\\nWindows\"@en-US,\n" +
                            "\"Eric\\r\\nNewline\"@en, \"Harry\"."),
                //list ages
                graph("CONSTRUCT {:Graph :hasAge ?x} WHERE {?s foaf:age ?x}",
                      ":Graph  :hasAge  23, \"25\"^^xsd:int.")
        );
        List<GraphData> expanded = new ArrayList<>();
        for (SparqlMethod method : SparqlMethod.VALUES) {
            for (MediaType mt : RDFMediaTypes.DEFAULT_ACCEPTS) {
                for (GraphData base : seed)
                    expanded.add(base.dup().method(method).accepts(mt));
            }
        }
        return expanded.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("graphData")
    void testGraphNetty(GraphData data) throws ExecutionException {
        testGraph(data, "netty");
    }
}
