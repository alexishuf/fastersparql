package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.impl.LeftBindJoin;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.providers.LeftJoinProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.impl.ArrayOperations;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils.publicVars;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.ASYNC;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
@Testcontainers
class LeftJoinTest {
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(LeftJoinTest.class, "left_join.hdt", log);


    @EqualsAndHashCode(callSuper = true) @ToString
    @Getter @Setter @Accessors(fluent = true, chain = true)
    private static class TestData extends ResultsChecker {
        private String left, right;

        public TestData(String left, String right, String... values) {
            super(VarUtils.union(publicVars(left), publicVars(right)), values);
            this.left = PREFIX+left;
            this.right = PREFIX+right;
        }
    }

    private static TestData data(String left, String right, String... expected) {
        return new TestData(left, right, expected);
    }

    @SuppressWarnings("unused")
    static Stream<Arguments> test() {
        List<TestData> base = asList(
                //right always has a match
        /* 1 */ data("SELECT ?o WHERE {:left1 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"5\"^^<$xsd:integer>", "<$:right1>"),
                //right never has a match
        /* 2 */ data("SELECT ?o WHERE {:left3 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"23\"^^<$xsd:integer>", "$null"),
                //right sometimes matches
        /* 3 */ data("SELECT ?o WHERE {:left2 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"2\"^^<$xsd:integer>", "$null",
                     "\"3\"^^<$xsd:integer>", "<$:right2>"),
                //changing right projection has no effect
        /* 4 */ data("SELECT * WHERE {:left2 :p1 ?o}",
                     "SELECT ?o ?s WHERE {?s :q1 ?o}",
                     "\"2\"^^<$xsd:integer>", "$null",
                     "\"3\"^^<$xsd:integer>", "<$:right2>"),
                //the three above
        /* 5 */ data("SELECT ?o WHERE {?s :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"5\"^^<$xsd:integer>", "<$:right1>",
                     "\"2\"^^<$xsd:integer>", "$null",
                     "\"3\"^^<$xsd:integer>", "<$:right2>",
                     "\"23\"^^<$xsd:integer>", "$null"),

                /* many rows on left, each with 0-1 matches */
        /* 6 */ data("SELECT * WHERE {:lleft1 :p2 ?o}",
                     "SELECT * WHERE {?s :q2 ?o}",
                     "\"11\"^^<$xsd:integer>", "<$:lright1>",
                     "\"12\"^^<$xsd:integer>", "$null",
                     "\"13\"^^<$xsd:integer>", "<$:lright1>",
                     "\"14\"^^<$xsd:integer>", "$null",
                     "\"15\"^^<$xsd:integer>", "<$:lright1>",
                     "\"16\"^^<$xsd:integer>", "$null",
                     "\"17\"^^<$xsd:integer>", "<$:lright1>",
                     "\"18\"^^<$xsd:integer>", "$null"),
        /* 7 */ data("SELECT * WHERE {:lleft2 :p2 ?o}",
                     "SELECT * WHERE {?s :q2 ?o}",
                     "\"21\"^^<$xsd:integer>", "<$:lright2>",
                     "\"21\"^^<$xsd:integer>", "<$:lright3>",
                     "\"22\"^^<$xsd:integer>", "$null",
                     "\"23\"^^<$xsd:integer>", "<$:lright2>",
                     "\"23\"^^<$xsd:integer>", "<$:lright3>",
                     "\"24\"^^<$xsd:integer>", "$null",
                     "\"25\"^^<$xsd:integer>", "<$:lright2>",
                     "\"25\"^^<$xsd:integer>", "<$:lright3>",
                     "\"26\"^^<$xsd:integer>", "$null",
                     "\"27\"^^<$xsd:integer>", "<$:lright2>",
                     "\"27\"^^<$xsd:integer>", "<$:lright3>",
                     "\"28\"^^<$xsd:integer>", "$null"),

                /* few left rows, each with 0-8 matches */
        /* 8 */ data("SELECT ?o WHERE {?s :p3 ?o}",
                     "SELECT * WHERE {?s :q3 ?o}",
                     "\"1\"^^<$xsd:integer>", "<$:kright1>",
                     "\"1\"^^<$xsd:integer>", "<$:kright3>",
                     "\"1\"^^<$xsd:integer>", "<$:kright5>",
                     "\"1\"^^<$xsd:integer>", "<$:kright7>",
                     "\"2\"^^<$xsd:integer>", "<$:kright2>",
                     "\"2\"^^<$xsd:integer>", "<$:kright4>",
                     "\"2\"^^<$xsd:integer>", "<$:kright6>",
                     "\"2\"^^<$xsd:integer>", "<$:kright8>",
                     "\"3\"^^<$xsd:integer>", "<$:kright1>",
                     "\"3\"^^<$xsd:integer>", "<$:kright2>",
                     "\"3\"^^<$xsd:integer>", "<$:kright3>",
                     "\"3\"^^<$xsd:integer>", "<$:kright4>",
                     "\"3\"^^<$xsd:integer>", "<$:kright5>",
                     "\"3\"^^<$xsd:integer>", "<$:kright6>",
                     "\"3\"^^<$xsd:integer>", "<$:kright7>",
                     "\"3\"^^<$xsd:integer>", "<$:kright8>",
                     "\"4\"^^<$xsd:integer>", "$null")
        );
        List<SparqlClientFactory> factories = TestUtils.allClientFactories();
        List<LeftJoinProvider> providers = new ArrayList<>();
        ServiceLoader.load(LeftJoinProvider.class).forEach(providers::add);
        providers.add(new LeftBindJoin.Provider() {
            @Override public LeftJoin create(long flags, RowOperations rowOperations) {
                return new LeftBindJoin(rowOperations, (flags & ASYNC) != 0 ? 32 : 1);
            }
        });
        List<Long> flags = asList(0L, ASYNC);
        return factories.stream().flatMap(fac -> providers.stream().flatMap(prov -> flags.stream()
                        .flatMap(flag -> base.stream()
                                .map(testData -> arguments(fac, prov, flag, testData)))))
                .collect(Collectors.toList()).stream();
    }

    @ParameterizedTest @MethodSource("test")
    void selfTest(SparqlClientFactory clientFactory, LeftJoinProvider ignoredProvider,
                  long ignoredFlags, TestData testData) {
        try (SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint())) {
            checkQuery(testData.left(), client);
            checkQuery(testData.right(), client);
        }
    }

    private void checkQuery(String sparql, SparqlClient<String[], byte[]> client) {
        Results<String[]> results = client.query(sparql);
        IterableAdapter<String[]> adapter = new IterableAdapter<>(results.publisher());
        adapter.forEach(r -> {});
        assertFalse(adapter.hasError());
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, LeftJoinProvider provider, long flags,
              TestData testData) throws ExecutionException {
        if (provider.bid(flags) == BidCosts.UNSUPPORTED)
            return;
        SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint());
        LeafPlan<String[]> leftPlan = new LeafPlan<>(testData.left(), client);
        LeafPlan<String[]> rightPlan = new LeafPlan<>(testData.right(), client);
        List<AsyncTask<?>> futures = new ArrayList<>();
        for (int thread = 0; thread < N_THREADS; thread++) {
            futures.add(Async.async(() -> {
                for (int i = 0; i < N_ITERATIONS; i++) {
                    LeftJoin op = provider.create(flags, ArrayOperations.INSTANCE);
                    testData.assertExpected(op.checkedRun(leftPlan, rightPlan));
                }
            }));
        }
        for (AsyncTask<?> future : futures) future.get();
    }
}