package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.FS;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientTest;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
class ClientBindingBItLiveTest {
    private static final Logger log = LoggerFactory.getLogger(ClientBindingBItLiveTest.class);

    private static final int THREADS = Runtime.getRuntime().availableProcessors()*4;
    @Container private static final HdtssContainer hdtss
            = new HdtssContainer(SparqlClientTest.class, "data.hdt", log);
    private static SparqlClient<List<String>, String, byte[]> client;

    @BeforeAll
    static void beforeAll() {
        var cfg = SparqlConfiguration.builder()
                .clearMethods().method(SparqlMethod.POST)
                .clearResultsAccepts().resultsAccept(SparqlResultFormat.TSV).build();
        client = FS.clientFor(hdtss.asEndpoint(cfg), ListRow.STRING,
                              ByteArrayFragmentParser.INSTANCE);
    }

    @AfterAll
    static void afterAll() { client.close(); }

    private static String expandPrefixes(String s) {
        if (s == null)
            return null;
        return s.replace("<:", "<http://example.org/")
                .replace("<xsd:", "<http://www.w3.org/2001/XMLSchema#");
    }

    private static List<String> expandPrefixesInRow(List<String> list) {
        return list.stream().map(ClientBindingBItLiveTest::expandPrefixes).collect(toList());
    }
    private static List<List<String>> expandPrefixes(List<List<String>> list) {
        return list.stream().map(ClientBindingBItLiveTest::expandPrefixesInRow).collect(toList());
    }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        String prefix = """
                PREFIX : <http://example.org/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                """;
        class D {
            final List<String> bindingVars;
            final List<List<String>> bindings;
            final String sparql;
            final BindType bindType;
            final List<List<String>> expected;

            public D(List<String> bindingVars, List<List<String>> bindings, String sparql,
                     BindType bindType, List<List<String>> expected) {
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
        for (D d : base) {
            SparqlQuery sparql = new SparqlQuery(prefix + d.sparql);
            var left = new IteratorBIt<>(expandPrefixes(d.bindings), List.class, Vars.EMPTY);
            var it = new ClientBindingBIt<>(left, d.bindType, ListRow.STRING, client, sparql);
            Supplier<List<List<String>>> binder = it::toList;
            argumentsList.add(arguments(binder, expandPrefixes(d.expected)));
        }
        return argumentsList.stream();
    }

    @ParameterizedTest @MethodSource
    void test(Supplier<List<ListRow<String>>> binder, List<ListRow<String>> expected) throws Exception {
        VThreadTaskSet.repeatAndWait(getClass().getSimpleName(), THREADS, i -> {
            var actual = binder.get();
            assertEquals(new HashSet<>(expected), new HashSet<>(actual));
            assertEquals(expected.size(), actual.size());
        });
    }
}