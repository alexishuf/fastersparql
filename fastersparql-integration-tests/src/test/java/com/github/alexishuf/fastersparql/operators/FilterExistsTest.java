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
import com.github.alexishuf.fastersparql.operators.impl.bind.BindFilterExists;
import com.github.alexishuf.fastersparql.operators.plan.ExistsPlan;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.providers.FilterExistsProvider;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
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
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.ASYNC;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.LARGE_FIRST;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class FilterExistsTest {
    private static final Logger log = LoggerFactory.getLogger(FilterExistsTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(FilterExistsTest.class, "filter_exists.hdt", log);

    @EqualsAndHashCode(callSuper = true)
    @Getter @Setter @Accessors(fluent = true, chain = true)
    private static final class TestData extends ResultsChecker {
        private final String left;
        private final boolean negate;
        private final String filter;

        public TestData(String left, boolean negate, String filter, String... values) {
            super(SparqlUtils.publicVars(left), values);
            this.left = PREFIX+left;
            this.filter = PREFIX+filter;
            this.negate = negate;
        }
    }

    private static TestData exists(String left, String filter, String... expected) {
        return new TestData(left, false, filter, expected);
    }
    private static TestData notExists(String left, String filter, String... expected) {
        return new TestData(left, true, filter, expected);
    }

    static Stream<Arguments> test() {
        List<TestData> base = asList(
                // single row has match
        /*  1 */exists("SELECT * WHERE {:left1 :p1 ?o} ",
                       "SELECT * WHERE {?s :q1 ?o}",
                       "\"1\"^^<$xsd:integer>"),
        /*  2 */notExists("SELECT * WHERE {:left1 :p1 ?o} ",
                          "SELECT * WHERE {?s :q1 ?o}"),
                // repeat now using ASK
        /*  3 */exists("SELECT * WHERE {:left1 :p1 ?o} ",
                       "ASK {?s :q1 ?o}",
                       "\"1\"^^<$xsd:integer>"),
        /*  4 */notExists("SELECT * WHERE {:left1 :p1 ?o} ",
                          "ASK {?s :q1 ?o}"),

                // single row has no match
        /*  5 */exists("SELECT ?o WHERE {:left2 :p1 ?o}",
                       "SELECT ?s WHERE {?s :q1 ?o}"), // project-out ?o
        /*  6 */notExists("SELECT ?o WHERE {:left2 :p1 ?o}",
                          "SELECT ?s WHERE {?s :q1 ?o}", // project-out ?o
                          "\"2\"^^<$xsd:integer>"),
                // ... repeat using ASK
        /*  7 */exists("SELECT ?o WHERE {:left2 :p1 ?o}",
                       "ASK {?s :q1 ?o}"),
        /*  8 */notExists("SELECT ?o WHERE {:left2 :p1 ?o}",
                          "ASK {?s :q1 ?o}",
                          "\"2\"^^<$xsd:integer>"),

                // two rows, first has match
        /*  9 */exists("SELECT * WHERE {:left3 :p1 ?o}",
                        "SELECT * WHERE {?s :q1 ?o}",
                        "\"4\"^^<$xsd:integer>"),
        /* 10 */notExists("SELECT * WHERE {:left3 :p1 ?o}",
                          "SELECT * WHERE {?s :q1 ?o}",
                          "\"5\"^^<$xsd:integer>"),
                // ... repeat using ASK
        /* 11 */exists("SELECT * WHERE {:left3 :p1 ?o}",
                       "ASK {?s :q1 ?o}",
                       "\"4\"^^<$xsd:integer>"),
        /* 12 */notExists("SELECT * WHERE {:left3 :p1 ?o}",
                          "ASK {?s :q1 ?o}",
                          "\"5\"^^<$xsd:integer>"),

                // two rows, second has match
        /* 13 */exists("SELECT * WHERE {:left4 :p1 ?o}",
                        "SELECT * WHERE {?s :q1 ?o}",
                        "\"7\"^^<$xsd:integer>"),
        /* 14 */notExists("SELECT * WHERE {:left4 :p1 ?o}",
                          "SELECT * WHERE {?s :q1 ?o}",
                          "\"6\"^^<$xsd:integer>"),
                // ... repeat with ASK
        /* 15 */exists("SELECT * WHERE {:left4 :p1 ?o}",
                       "ASK {?s :q1 ?o}",
                       "\"7\"^^<$xsd:integer>"),
        /* 16 */notExists("SELECT * WHERE {:left4 :p1 ?o}",
                          "ASK {?s :q1 ?o}",
                          "\"6\"^^<$xsd:integer>"),

                // single row has two matches
        /* 17 */exists("SELECT * WHERE {:left5 :p1 ?o}",
                       "SELECT * WHERE {?s :q1 ?o}",
                       "\"8\"^^<$xsd:integer>"),
        /* 18 */notExists("SELECT * WHERE {:left5 :p1 ?o}",
                          "SELECT * WHERE {?s :q1 ?o}"),
                // ... repeat with ASK
        /* 19 */exists("SELECT * WHERE {:left5 :p1 ?o}",
                       "ASK {?s :q1 ?o}",
                       "\"8\"^^<$xsd:integer>"),
        /* 20 */notExists("SELECT * WHERE {:left5 :p1 ?o}",
                          "ASK {?s :q1 ?o}"),

                // "7-row" ask query at left with non-empty filter
        /* 21 */exists("ASK {?s :p1 ?o}", "SELECT * WHERE {?s :q1 ?o}", ""),
        /* 22 */notExists("ASK {?s :p1 ?o}", "SELECT * WHERE {?s :q1 ?o}"),

                // "7-row" ask query at left with empty filter
        /* 23 */exists("ASK {?s :p1 ?o}", "SELECT * WHERE {?s :q0 ?o}"),
        /* 24 */notExists("ASK {?s :p1 ?o}", "SELECT * WHERE {?s :q0 ?o}", ""),

                // empty ask query at left with non-empty filter
        /* 25 */exists("ASK {?s :p0 ?o}", "SELECT * WHERE {?s :q1 ?o}"),
        /* 26 */notExists("ASK {?s :p0 ?o}", "SELECT * WHERE {?s :q1 ?o}"),

                //many rows at left scenario
        /* 27 */exists("SELECT ?o WHERE {?s :p2 ?o}",
                       "SELECT ?s WHERE {?s :q2 ?o}",
                       "\"11\"^^<$xsd:integer>",
                       "\"13\"^^<$xsd:integer>",
                       "\"15\"^^<$xsd:integer>",
                       "\"17\"^^<$xsd:integer>",
                       "\"19\"^^<$xsd:integer>",
                       "\"22\"^^<$xsd:integer>",
                       "\"24\"^^<$xsd:integer>",
                       "\"26\"^^<$xsd:integer>",
                       "\"28\"^^<$xsd:integer>"),
        /* 28 */notExists("SELECT ?o WHERE {?s :p2 ?o}",
                          "SELECT ?s WHERE {?s :q2 ?o}",
                          "\"12\"^^<$xsd:integer>",
                          "\"14\"^^<$xsd:integer>",
                          "\"16\"^^<$xsd:integer>",
                          "\"18\"^^<$xsd:integer>",
                          "\"21\"^^<$xsd:integer>",
                          "\"23\"^^<$xsd:integer>",
                          "\"25\"^^<$xsd:integer>",
                          "\"27\"^^<$xsd:integer>",
                          "\"29\"^^<$xsd:integer>"),
                // ... repeat with ASK
        /* 29 */exists("SELECT ?o WHERE {?s :p2 ?o}",
                       "ASK {?s :q2 ?o}",
                       "\"11\"^^<$xsd:integer>",
                       "\"13\"^^<$xsd:integer>",
                       "\"15\"^^<$xsd:integer>",
                       "\"17\"^^<$xsd:integer>",
                       "\"19\"^^<$xsd:integer>",
                       "\"22\"^^<$xsd:integer>",
                       "\"24\"^^<$xsd:integer>",
                       "\"26\"^^<$xsd:integer>",
                       "\"28\"^^<$xsd:integer>"),
        /*  30 */notExists("SELECT ?o WHERE {?s :p2 ?o}",
                          "ASK {?s :q2 ?o}",
                          "\"12\"^^<$xsd:integer>",
                          "\"14\"^^<$xsd:integer>",
                          "\"16\"^^<$xsd:integer>",
                          "\"18\"^^<$xsd:integer>",
                          "\"21\"^^<$xsd:integer>",
                          "\"23\"^^<$xsd:integer>",
                          "\"25\"^^<$xsd:integer>",
                          "\"27\"^^<$xsd:integer>",
                          "\"29\"^^<$xsd:integer>")
        );
        List<SparqlClientFactory> factories = TestUtils.allClientFactories();
        List<FilterExistsProvider> providers = new ArrayList<>();
        ServiceLoader.load(FilterExistsProvider.class).forEach(providers::add);
        assertFalse(providers.isEmpty());
        providers.add(new BindFilterExists.Provider() {
            @Override public @NonNegative int bid(long flags) {
                if ((flags&ASYNC) == 0) return BidCosts.UNSUPPORTED;
                return BidCosts.BUILTIN_COST;
            }
            @Override public FilterExists create(long flags, RowOperations ro) {
                return new BindFilterExists(ro, (flags&ASYNC) == 0 ? 1 : 32);
            }

            @Override public String toString() {
                return "BindFilterExists[concurrency=32]";
            }
        });
        List<Long> flagsList = asList(0L, ASYNC, LARGE_FIRST|ASYNC);
        return factories.stream().flatMap(fac -> providers.stream().flatMap(
                prov -> flagsList.stream().flatMap(flags -> base.stream().map(
                        data -> arguments(fac, prov, flags, data)))));
    }

    @Test
    void selfTest() throws ExecutionException {
        val clientFactories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<String> queries = test().map(a -> (TestData) a.get()[3])
                                    .flatMap(d -> Stream.of(d.left, d.filter)).collect(toSet());

        for (SparqlClientFactory fac : clientFactories) {
            try (SparqlClient<String[], byte[]> client = fac.createFor(HDTSS.asEndpoint())) {
                List<AsyncTask<?>> tasks = new ArrayList<>();
                for (String query : queries) {
                    tasks.add(Async.async(() -> {
                        val a = new IterableAdapter<>(client.query(query).publisher());
                        a.forEach(r -> {});
                        if (a.hasError())
                            fail(a.error());
                    }));
                }
                for (AsyncTask<?> task : tasks) task.get();
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, FilterExistsProvider provider, long flags,
              TestData data) throws ExecutionException {
        if (provider.bid(flags) == BidCosts.UNSUPPORTED)
            return;
        FilterExists op = provider.create(flags, StringArrayOperations.get());
        try (SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint())) {
            LeafPlan<String[]> left = LeafPlan.builder(client, data.left).build();
            LeafPlan<String[]> filter = LeafPlan.builder(client, data.filter).build();
            ExistsPlan<String[]> plan = op.asPlan(left, data.negate, filter);
            List<AsyncTask<?>> tasks = new ArrayList<>();
            for (int thread = 0; thread < N_THREADS; thread++) {
                tasks.add(Async.async(() -> {
                    for (int i = 0; i < N_ITERATIONS; i++)
                        data.assertExpected(plan.execute());
                }));
            }
            for (AsyncTask<?> task : tasks) task.get();
        }
    }
}
