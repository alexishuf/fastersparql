package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.FSOps.query;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
class FilterTest {
    private static final Logger log = LoggerFactory.getLogger(FilterTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(ExistsTest.class, "filter.hdt", log);

    @FunctionalInterface
    interface FilterFactory {
        Modifier<List<String>, String> create(Plan<List<String>, String> in, List<String> filters);
    }

    private static final class TestData extends ResultsChecker {
        private final SparqlQuery left;
        private final List<String> filters;

        public TestData(String left, List<String> filters, String... values) {
            super(new SparqlQuery(left).publicVars, values);
            this.left = new SparqlQuery(PREFIX+left);
            this.filters = filters;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData testData)) return false;
            return left.equals(testData.left) && filters.equals(testData.filters);
        }

        @Override public int hashCode() {
            return Objects.hash(left, filters);
        }
    }

    private static TestData data(String left, List<String> filters, String... values) {
        return new TestData(left, filters, values);
    }

    static Stream<Arguments> test() {
        List<TestData> base = asList(
        /*  1 */data("SELECT * WHERE { ?s :p1 ?o}", singletonList("?o = 1"),
                     "<$:left11>", "\"1\"^^<$xsd:integer>"),
        /*  2 */data("SELECT * WHERE { ?s :p1 ?o}", singletonList("?o > 0"),
                     "<$:left11>", "\"1\"^^<$xsd:integer>"),
        /*  3 */data("SELECT * WHERE { ?s :p1 ?o}", asList("?o = 1", "?o > 0"),
                     "<$:left11>", "\"1\"^^<$xsd:integer>"),
        /*  4 */data("SELECT * WHERE { ?s :p1 ?o}", singletonList("?o > 1")),
        /*  5 */data("SELECT * WHERE { ?s :p1 ?o}", asList("?o = 1", "?o > 1")),

        /*  6 */data("SELECT * WHERE { ?s :p2 ?o}", singletonList("?o = 1"),
                     "<$:left21>", "\"1\"^^<$xsd:integer>"),
        /*  7 */data("SELECT * WHERE { ?s :p2 ?o}", singletonList("?o = 2"),
                     "<$:left22>", "\"2\"^^<$xsd:integer>"),
        /*  8 */data("SELECT * WHERE { ?s :p2 ?o}", asList("?o = 2", "?o > 1"),
                     "<$:left22>", "\"2\"^^<$xsd:integer>"),
        /*  9 */data("SELECT * WHERE { ?s :p2 ?o}", asList("?o = 2", "?o > 2")),

        /* 10 */data("SELECT * WHERE { ?s :p3 ?o}", asList("?o > 3", "?o < 7"),
                     "<$:left34>", "\"4\"^^<$xsd:integer>",
                     "<$:left35>", "\"5\"^^<$xsd:integer>",
                     "<$:left36>", "\"6\"^^<$xsd:integer>"),
        /* 11 */data("SELECT * WHERE { ?s :p3 ?o}", asList("?o != 4", "?o > 0"),
                     "<$:left31>", "\"1\"^^<$xsd:integer>",
                     "<$:left32>", "\"2\"^^<$xsd:integer>",
                     "<$:left33>", "\"3\"^^<$xsd:integer>",
                     "<$:left35>", "\"5\"^^<$xsd:integer>",
                     "<$:left36>", "\"6\"^^<$xsd:integer>",
                     "<$:left37>", "\"7\"^^<$xsd:integer>",
                     "<$:left38>", "\"8\"^^<$xsd:integer>",
                     "<$:left39>", "\"9\"^^<$xsd:integer>")
        );
        List<SparqlClientFactory> factories = TestUtils.allClientFactories();
        List<FilterFactory> filterFactories = List.of(FSOps::filter);
        List<Arguments> list = new ArrayList<>();
        for (SparqlClientFactory factory : factories) {
            for (FilterFactory filterFactory : filterFactories) {
                for (TestData d : base)
                    list.add(arguments(factory, filterFactory, d));
            }
        }
        return list.stream();
    }

    @Test
    void selfTest() throws Exception {
        Set<SparqlClientFactory> factories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<SparqlQuery> queries = test().flatMap(a -> Stream.of(((TestData) a.get()[2]).left)).collect(toSet());
        for (SparqlClientFactory factory : factories) {
            try (var client = factory.createFor(HDTSS.asEndpoint());
                 var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
                for (var sparql : queries)
                    tasks.add(() -> assertFalse(client.query(sparql).toList().isEmpty()));
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory factory, FilterFactory filterFactory,
              TestData d) throws Exception {
        try (var client = factory.createFor(HDTSS.asEndpoint(), ListRow.STRING);
             var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            var left = query(client, d.left);
            tasks.repeat(N_THREADS, () -> {
                for (int i = 0; i < N_ITERATIONS; i++)
                    d.assertExpected(filterFactory.create(left, d.filters).execute());

            });
        }
    }

}