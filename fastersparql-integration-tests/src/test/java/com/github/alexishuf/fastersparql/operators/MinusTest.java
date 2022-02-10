package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.impl.bind.BindMinus;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.providers.MinusProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.impl.ArrayOperations;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
@Testcontainers
public class MinusTest {
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(MinusTest.class, "minus.hdt", log);

    @EqualsAndHashCode(callSuper = true)
    @Getter @Setter @Accessors(fluent = true, chain = true)
    private static class TestData extends ResultsChecker {
        private final String left, right;

        public TestData(String left, String right, String... values) {
            super(SparqlUtils.publicVars(left), values);
            this.left = PREFIX+left;
            this.right = PREFIX+right;
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

    @ParameterizedTest @MethodSource("test")
    void selfTest(SparqlClientFactory clientFactory, MinusProvider provider, long flags,
                  TestData testData) {
        try (SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint())) {
            checkQuery(client, testData.left);
            checkQuery(client, testData.right);
        }
    }

    private void checkQuery(SparqlClient<String[], byte[]> client, String sparql) {
        IterableAdapter<String[]> adapter = new IterableAdapter<>(client.query(sparql).publisher());
        adapter.forEach(r -> {});
        if (adapter.hasError())
            fail(adapter.error());
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, MinusProvider provider, long flags,
              TestData testData) throws ExecutionException {
        if (provider.bid(flags) == BidCosts.UNSUPPORTED)
            return;
        try (SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint())) {
            LeafPlan<String[]> left = new LeafPlan<>(testData.left, client);
            LeafPlan<String[]> right = new LeafPlan<>(testData.right, client);
            List<AsyncTask<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < N_THREADS; thread++) {
                futures.add(Async.async(() -> {
                    for (int i = 0; i < N_ITERATIONS; i++) {
                        Minus minus = provider.create(flags, ArrayOperations.INSTANCE);
                        testData.assertExpected(minus.checkedRun(left, right));
                    }
                }));
            }
            for (AsyncTask<?> future : futures) future.get();
        }
    }
}
