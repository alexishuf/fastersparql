package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.impl.bind.BindMinus;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.providers.MinusProvider;
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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class MinusTest {
    private static final Logger log = LoggerFactory.getLogger(MinusTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(MinusTest.class, "minus.hdt", log);

    private static class TestData extends ResultsChecker {
        final String left, right;

        public TestData(String left, String right, String... values) {
            super(SparqlUtils.publicVars(left), values);
            this.left = PREFIX+left;
            this.right = PREFIX+right;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData)) return false;
            TestData testData = (TestData) o;
            return left.equals(testData.left) && right.equals(testData.right);
        }

        @Override public int hashCode() {
            return Objects.hash(left, right);
        }

        @Override public String toString() {
            return "TestData{left='" + left + '\'' +  ", right='" + right + '\'' +  '}';
        }
    }

    private static TestData data(String left, String right, String... expected) {
        return new TestData(left, right, expected);
    }

    static Stream<Arguments> test() {
        List<TestData> base = asList(
                // 1 row in left without match
        /*  1 */data("SELECT ?o WHERE {:left1 :p1 ?o}",
                     "SELECT * WHERE {?r :q1 ?o}",
                     "\"23\"^^<$xsd:integer>"),
                // 1 row in left with match
        /*  2 */data("SELECT ?o WHERE {:left2 :p1 ?o}",
                     "SELECT * WHERE {?r :q1 ?o}"),
                // 2 rows in left, first matches
        /*  3 */data("SELECT ?o WHERE {:left3 :p1 ?o}",
                     "SELECT * WHERE {?r :q1 ?o}",
                     "\"3\"^^<$xsd:integer>"),
                // 2 rows in left, second matches
        /*  4 */data("SELECT ?o WHERE {:left4 :p1 ?o}",
                     "SELECT * WHERE {?r :q1 ?o}",
                     "\"7\"^^<$xsd:integer>"),
                // 2 rows in left, none matches
        /*  5 */data("SELECT ?o WHERE {:left5 :p1 ?o}",
                     "SELECT * WHERE {?r :q1 ?o}",
                     "\"17\"^^<$xsd:integer>",
                     "\"27\"^^<$xsd:integer>"),

                // empty left, non-empty right
        /*  6 */data("SELECT * WHERE {:left13 :p2 ?o}",
                     "SELECT * WHERE {?x :q2 ?o}"),
                // empty left, non-empty right, no shared vars
        /*  7 */data("SELECT * WHERE {:left13 :p2 ?o}",
                        "SELECT * WHERE {?x :q2 ?z}"),
                // singleton left, non-empty right, no shared vars
        /*  8 */data("SELECT * WHERE {:left11 :p2 ?o}",
                     "SELECT * WHERE {?x :q2 ?y}",
                     "\"2\"^^<$xsd:integer>"),
                // 2-rows left, non-empty right, no shared vars
        /*  9 */data("SELECT * WHERE {:left12 :p2 ?o}",
                     "SELECT * WHERE {?x :q2 ?y}",
                     "\"2\"^^<$xsd:integer>",
                     "\"3\"^^<$xsd:integer>"),
                // 3-rows left with two vars, non-empty right, no shared vars
        /* 10 */data("SELECT * WHERE {?s :p2 ?o}",
                     "SELECT * WHERE {?x :q2 ?y}",
                     "<$:left11>", "\"2\"^^<$xsd:integer>",
                     "<$:left12>", "\"2\"^^<$xsd:integer>",
                     "<$:left12>", "\"3\"^^<$xsd:integer>"),

                // many left rows, half have 1+ match
        /* 11 */data("SELECT * WHERE {?l :p3 ?o}",
                     "SELECT * WHERE {?r :q3 ?o}",
                     "<$:left21>", "\"12\"^^<$xsd:integer>",
                     "<$:left21>", "\"14\"^^<$xsd:integer>",
                     "<$:left21>", "\"16\"^^<$xsd:integer>",
                     "<$:left21>", "\"18\"^^<$xsd:integer>",
                     "<$:left22>", "\"21\"^^<$xsd:integer>",
                     "<$:left22>", "\"23\"^^<$xsd:integer>",
                     "<$:left22>", "\"25\"^^<$xsd:integer>",
                     "<$:left22>", "\"27\"^^<$xsd:integer>",
                     "<$:left22>", "\"29\"^^<$xsd:integer>",
                     "<$:left23>", "\"32\"^^<$xsd:integer>",
                     "<$:left23>", "\"34\"^^<$xsd:integer>",
                     "<$:left23>", "\"36\"^^<$xsd:integer>",
                     "<$:left23>", "\"38\"^^<$xsd:integer>")
        );
        List<SparqlClientFactory> factories = TestUtils.allClientFactories();
        List<MinusProvider> providers = new ArrayList<>();
        ServiceLoader.load(MinusProvider.class).forEach(providers::add);
        assertFalse(providers.isEmpty());
        providers.add(new BindMinus.Provider() {
            @Override public Minus create(long flags, RowOperations ro) {
                return new BindMinus(ro, (flags & OperatorFlags.ASYNC) != 0 ? 32 : 1);
            }
            @Override public String toString() {
                return super.toString()+"[32-concurrency]";
            }
        });
        List<Long> flagsList = asList(0L, OperatorFlags.ASYNC);
        return factories.stream().flatMap(fac -> providers.stream().flatMap(
                prov -> flagsList.stream().flatMap(flags -> base.stream().map(
                        data -> arguments(fac, prov, flags, data)))));
    }

    @Test
    void selfTest() throws ExecutionException {
        Set<SparqlClientFactory> factories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<String> queries = test().map(a -> (TestData) a.get()[3])
                                    .flatMap(d -> Stream.of(d.left, d.right)).collect(toSet());
        for (SparqlClientFactory factory : factories) {
            try (SparqlClient<String[], byte[]> client = factory.createFor(HDTSS.asEndpoint())) {
                List<AsyncTask<?>> tasks = new ArrayList<>();
                for (String query : queries) {
                    tasks.add(Async.async(() -> {
                        try (IterableAdapter<String[]> a = new IterableAdapter<>(client.query(query).publisher())) {
                            if (a.hasError())
                                fail(a.error());
                        }
                    }));
                }
                for (AsyncTask<?> task : tasks) task.get();
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, MinusProvider provider, long flags,
              TestData testData) throws ExecutionException {
        if (provider.bid(flags) == BidCosts.UNSUPPORTED)
            return;
        try (SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint())) {
            LeafPlan<String[]> left = LeafPlan.builder(client, testData.left).build();
            LeafPlan<String[]> right = LeafPlan.builder(client, testData.right).build();
            List<AsyncTask<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < N_THREADS; thread++) {
                futures.add(Async.async(() -> {
                    for (int i = 0; i < N_ITERATIONS; i++) {
                        Minus minus = provider.create(flags, StringArrayOperations.get());
                        testData.assertExpected(minus.checkedRun(minus.asPlan(left, right)));
                    }
                }));
            }
            for (AsyncTask<?> future : futures) future.get();
        }
    }
}
