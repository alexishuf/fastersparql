package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.FusekiContainer;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.model.RDFMediaTypes;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.CharSequenceFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.StringFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.row.*;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.GraphData.graph;
import static com.github.alexishuf.fastersparql.client.ResultsData.results;
import static com.github.alexishuf.fastersparql.client.util.async.Async.async;
import static java.util.Arrays.asList;

@SuppressWarnings("SameParameterValue")
@Testcontainers
@Slf4j
public class SparqlClientTest {

    @Container private static final HdtssContainer HDTSS =
            new HdtssContainer(SparqlClientTest.class, "data.hdt", log);
    @Container private static final FusekiContainer FUSEKI =
            new FusekiContainer(SparqlClientTest.class, "data.ttl", log);

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
                for (ResultsData base : seed) {
                    expanded.add(base.with(b -> b.clearResultsAccepts().resultsAccept(format)
                                                 .clearMethods().method(method)));
                }
            }
        }
        return expanded.stream().map(Arguments::arguments);
    }

    private void testResults(ResultsData d, String clientTag) throws ExecutionException {
        int threads = Runtime.getRuntime().availableProcessors() + 1;
        List<AsyncTask<?>> futures = new ArrayList<>();
        SparqlClientFactory factory = FasterSparql.factory(clientTag);
        List<SparqlClient<?, ?>> clients = new ArrayList<>();
        try {
            for (SparqlEndpoint ep : asList(HDTSS.asEndpoint(), FUSEKI.asEndpoint())) {
                SparqlClient<String[], byte[]> client = factory.createFor(ep);
                clients.add(client);
                d.assertExpected(client.query(d.sparql(), d.config()));
                // a second query should work
                d.assertExpected(client.query(d.sparql(), d.config()));
            }
            // concurrent queries using no row parser
            for (SparqlClient<?, ?> client : clients) {
                for (int i = 0; i < threads; i++)
                    futures.add(async(() -> d.assertExpected(client.query(d.sparql(), d.config()))));
            }

            // concurrent queries for each (endpoint, rowParser) pair
            ByteArrayFragmentParser bytesP = ByteArrayFragmentParser.INSTANCE;
            List<RowParser<?>> rowParsers = asList(StringListRowParser.INSTANCE,
                    CharSequenceListRowParser.INSTANCE, CharSequenceArrayRowParser.INSTANCE);
            for (SparqlEndpoint ep : asList(HDTSS.asEndpoint(), FUSEKI.asEndpoint())) {
                for (RowParser<?> rowParser : rowParsers) {
                    futures.add(async(() -> {
                        try (SparqlClient<?, ?> client = factory.createFor(ep, rowParser, bytesP)) {
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

    private void testGraph(GraphData data, String tag) throws ExecutionException {
        SparqlClientFactory factory = FasterSparql.factory(tag);
        try (SparqlClient<String[], byte[]> client = factory.createFor(FUSEKI.asEndpoint())) {
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
