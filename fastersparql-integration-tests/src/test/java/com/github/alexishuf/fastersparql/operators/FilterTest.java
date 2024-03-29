package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.plan.FilterPlan;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.providers.FilterProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.ALL_LARGE;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.ASYNC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
class FilterTest {
    private static final Logger log = LoggerFactory.getLogger(FilterTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(FilterExistsTest.class, "filter.hdt", log);

    private static final class TestData extends ResultsChecker {
        private final String left;
        private final List<String> filters;

        public TestData(String left, List<String> filters, String... values) {
            super(SparqlUtils.publicVars(left), values);
            this.left = PREFIX+left;
            this.filters = filters;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData)) return false;
            TestData testData = (TestData) o;
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
        List<FilterProvider> providers = new ArrayList<>();
        ServiceLoader.load(FilterProvider.class).forEach(providers::add);
        assertFalse(providers.isEmpty());
        for (FilterProvider provider : providers)
            assertNotEquals(BidCosts.UNSUPPORTED, provider.bid(0L));
        List<Long> flagsList = asList(0L, ASYNC, ASYNC | ALL_LARGE);
        return factories.stream().flatMap(fac -> providers.stream().flatMap(
                prov -> flagsList.stream().flatMap(flags -> base.stream().map(
                        d -> arguments(fac, prov, flags, d)))));
    }

    @Test
    void selfTest() throws ExecutionException {
        Set<SparqlClientFactory> factories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<String> queries = test().flatMap(a -> Stream.of(((TestData) a.get()[3]).left)).collect(toSet());
        for (SparqlClientFactory factory : factories) {
            try (SparqlClient<String[], byte[]> client = factory.createFor(HDTSS.asEndpoint())) {
                List<AsyncTask<?>> tasks = new ArrayList<>();
                for (String query : queries) {
                    tasks.add(Async.async(() -> {
                        List<String[]> list = new ArrayList<>();
                        try (IterableAdapter<String[]> a = new IterableAdapter<>(client.query(query).publisher())) {
                            a.forEach(list::add);
                            if (a.hasError())
                                fail(a.error());
                        }
                        assertFalse(list.isEmpty());
                    }));
                }
                for (AsyncTask<?> task : tasks) task.get();
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory factory, FilterProvider provider, long flags,
              TestData data) throws ExecutionException {
        try (SparqlClient<String[], byte[]> client = factory.createFor(HDTSS.asEndpoint())) {
            Filter filter = provider.create(flags, StringArrayOperations.get());
            LeafPlan<String[]> leftPlan = LeafPlan.builder(client, data.left).build();
            FilterPlan<String[]> plan = filter.asPlan(leftPlan, data.filters);
            List<AsyncTask<?>> tasks = new ArrayList<>();
            for (int threads = 0; threads < N_THREADS; threads++) {
                tasks.add(Async.async(() -> {
                    for (int i = 0; i < N_ITERATIONS; i++)
                        data.assertExpected(plan.execute());
                }));
            }
            for (AsyncTask<?> task : tasks) task.get();
        }
    }

}