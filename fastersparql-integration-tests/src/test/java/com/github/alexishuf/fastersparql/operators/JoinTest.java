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
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.impl.bind.BindJoin;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.providers.JoinProvider;
import org.checkerframework.checker.index.qual.NonNegative;
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

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class JoinTest {
    private static final Logger log = LoggerFactory.getLogger(JoinTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(MinusTest.class, "join.hdt", log);

    private static class TestData extends ResultsChecker {
        final List<String> operands;

        public TestData(List<String> operands, String... values) {
            super(operands.stream().map(SparqlUtils::publicVars)
                                   .reduce(emptyList(), VarUtils::union),
                  values);
            this.operands = operands.stream().map(s -> PREFIX+s).collect(toList());
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData)) return false;
            TestData testData = (TestData) o;
            return operands.equals(testData.operands);
        }

        @Override public int hashCode() {
            return Objects.hash(operands);
        }

        @Override public String toString() {
            return "TestData{operands=" + operands +  ", vars=" + vars +  ", expected=" + expected +  ", checkOrder=" + checkOrder +  ", expectedError=" + expectedError +  '}';
        }
    }

    private static TestData data(String left, String right, String... expected) {
        return new TestData(asList(left, right), expected);
    }
    private static TestData nData(List<String> operands, String... expected) {
        return new TestData(operands, expected);
    }

    static Stream<Arguments> test() {
        List<TestData> base = asList(
                // 1-1 join
        /*  1 */data("SELECT * WHERE {:left1 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"1\"^^<$xsd:integer>", "<$:right1>"),
                // 2-row left, only first has a match
        /*  2 */data("SELECT * WHERE {:left2 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"2\"^^<$xsd:integer>", "<$:right2>"),
                // 2-row left, only second has a match
        /*  3 */data("SELECT * WHERE {:left3 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"5\"^^<$xsd:integer>", "<$:right3>"),
                // 1-row left without match
        /*  4 */data("SELECT * WHERE {:left4 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}"),
                // 1-row left with two matches
        /*  5 */data("SELECT * WHERE {:left5 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"7\"^^<$xsd:integer>", "<$:right5.1>",
                     "\"7\"^^<$xsd:integer>", "<$:right5.2>"),
                // 2-row left with two matches for each row
        /*  6 */data("SELECT * WHERE {:left6 :p1 ?o}",
                     "SELECT * WHERE {?s :q1 ?o}",
                     "\"8\"^^<$xsd:integer>", "<$:right6.1>",
                     "\"8\"^^<$xsd:integer>", "<$:right6.2>",
                     "\"9\"^^<$xsd:integer>", "<$:right6.3>",
                     "\"9\"^^<$xsd:integer>", "<$:right6.4>"),

                // 1-1 join with ASK right
        /*  7 */data("SELECT * WHERE {:left1 :p1 ?o}",
                     "SELECT ?o WHERE {?s :q1 ?o}",
                     "\"1\"^^<$xsd:integer>"),
                // 1-1 join with ASK left (requires swap)
        /*  8 */data("ASK {?s :q1 ?o}",
                     "SELECT * WHERE {:left1 :p1 ?o}",
                     "\"1\"^^<$xsd:integer>"),
                // 2-row left, only first has a match, ask right
        /*  9 */data("SELECT * WHERE {:left2 :p1 ?o}",
                     "SELECT ?o WHERE {?s :q1 ?o}",
                     "\"2\"^^<$xsd:integer>"),

                // many left rows, 0-2 matches per row
        /* 10 */data("SELECT * WHERE {?left :p2 ?o}",
                     "SELECT * WHERE {?right :q2 ?o}",
                     "<$:left11>", "\"12\"^^<$xsd:integer>", "<$:right11>",
                     "<$:left11>", "\"14\"^^<$xsd:integer>", "<$:right11>",
                     "<$:left11>", "\"16\"^^<$xsd:integer>", "<$:right11>",
                     "<$:left11>", "\"18\"^^<$xsd:integer>", "<$:right11>",

                     "<$:left12>", "\"21\"^^<$xsd:integer>", "<$:right12>",
                     "<$:left12>", "\"23\"^^<$xsd:integer>", "<$:right12>",
                     "<$:left12>", "\"25\"^^<$xsd:integer>", "<$:right12>",
                     "<$:left12>", "\"27\"^^<$xsd:integer>", "<$:right12>",
                     "<$:left12>", "\"29\"^^<$xsd:integer>", "<$:right12>",

                     "<$:left13>", "\"32\"^^<$xsd:integer>", "<$:right13>",
                     "<$:left13>", "\"34\"^^<$xsd:integer>", "<$:right13>",
                     "<$:left13>", "\"36\"^^<$xsd:integer>", "<$:right13>",
                     "<$:left13>", "\"38\"^^<$xsd:integer>", "<$:right13>",

                     "<$:left14>", "\"41\"^^<$xsd:integer>", "<$:right14.1>",
                     "<$:left14>", "\"41\"^^<$xsd:integer>", "<$:right14.2>",
                     "<$:left14>", "\"43\"^^<$xsd:integer>", "<$:right14.1>",
                     "<$:left14>", "\"43\"^^<$xsd:integer>", "<$:right14.2>",
                     "<$:left14>", "\"45\"^^<$xsd:integer>", "<$:right14.1>",
                     "<$:left14>", "\"45\"^^<$xsd:integer>", "<$:right14.2>",
                     "<$:left14>", "\"47\"^^<$xsd:integer>", "<$:right14.1>",
                     "<$:left14>", "\"47\"^^<$xsd:integer>", "<$:right14.2>",
                     "<$:left14>", "\"49\"^^<$xsd:integer>", "<$:right14.1>",
                     "<$:left14>", "\"49\"^^<$xsd:integer>", "<$:right14.2>"),

                //2-hop path with bound ends
        /* 11 */nData(asList("SELECT * WHERE {:n1 :p3.1 ?b}",
                             "SELECT * WHERE {?b  :p3.2 ?c}",
                             "SELECT * WHERE {?c  :p3.3 :n5}"),
                      "<$:n2>", "<$:n3>",
                      "<$:n2>", "<$:n4>"),
                //2-hop path with bound ends, reverse order
        /* 12 */nData(asList("SELECT * WHERE {?c  :p3.3 :n5}",
                             "SELECT * WHERE {?b  :p3.2 ?c}",
                             "SELECT * WHERE {:n1 :p3.1 ?b}"),
                      "<$:n3>", "<$:n2>",
                      "<$:n4>", "<$:n2>"),
                // 2-hop path
        /* 13 */nData(asList("SELECT * WHERE {?a :p3.1 ?b}",
                             "SELECT * WHERE {?b :p3.2 ?c}",
                             "SELECT * WHERE {?c :p3.3 ?d}"),
                      "<$:n1>", "<$:n2>", "<$:n3>", "<$:n5>",
                      "<$:n1>", "<$:n2>", "<$:n4>", "<$:n5>"),
                // find paths, efficient even on hash join
        /* 14 */nData(asList("SELECT * WHERE {?a :p3.1 ?b}",
                             "SELECT * WHERE {?b :p3.2 ?c}",
                             "SELECT * WHERE {?c :p3.3 ?d}",
                             "SELECT * WHERE {?d :p3.4 ?e}"),
                      "<$:n1>", "<$:n2>", "<$:n3>", "<$:n5>", "<$:n6>",
                      "<$:n1>", "<$:n2>", "<$:n4>", "<$:n5>", "<$:n6>"),

                // explore multiple paths, with mostly discarded intermediate results (if not bound)
        /* 15 */nData(asList("SELECT * WHERE {:s1 :p4 ?a}",
                             "SELECT * WHERE {?a  :p4 ?b}",
                             "SELECT * WHERE {?b  :p4 ?c}",
                             "SELECT * WHERE {?c  :p4 ?d}"),
                        "<$:s2>", "<$:s4>", "<$:s5>", "<$:s6>",
                        "<$:s2>", "<$:s4>", "<$:s5>", "<$:s7>",
                        "<$:s3>", "<$:s4>", "<$:s5>", "<$:s6>",
                        "<$:s3>", "<$:s4>", "<$:s5>", "<$:s7>"),

                // explore multiple paths, requires reorder to avoid cartesian
        /* 16 */nData(asList("SELECT * WHERE {:s1 :p4 ?a}",
                             "SELECT * WHERE {?b  :p4 ?c}",
                             "SELECT * WHERE {?a  :p4 ?b}",
                             "SELECT * WHERE {?c  :p4 ?d}"),
                      // a         b         c         d
                      "<$:s2>", "<$:s4>", "<$:s5>", "<$:s6>",
                      "<$:s2>", "<$:s4>", "<$:s5>", "<$:s7>",
                      "<$:s3>", "<$:s4>", "<$:s5>", "<$:s6>",
                      "<$:s3>", "<$:s4>", "<$:s5>", "<$:s7>")
        );
        List<SparqlClientFactory> factories = TestUtils.allClientFactories();
        List<JoinProvider> providers = new ArrayList<>();
        ServiceLoader.load(JoinProvider.class).forEach(providers::add);
        assertFalse(providers.isEmpty());
        providers.add(new BindJoin.Provider() {
            @Override public @NonNegative int bid(long flags) {
                if ((flags & ASYNC) == 0) return BidCosts.UNSUPPORTED;
                return super.bid(flags);
            }
            @Override public Join create(long flags, RowOperations ro) {
                return new BindJoin(ro, (flags & ASYNC) != 0 ? 32 : 1);
            }
            @Override public String toString() {
                return super.toString()+"[bindConcurrency=32]";
            }
        });
        List<Long> flagsList = asList(0L, ASYNC, LARGE_FIRST | SMALL_SECOND);
        return factories.stream().flatMap(fac -> providers.stream().flatMap(
                prov -> flagsList.stream().flatMap(flags -> base.stream().map(
                        data -> arguments(fac, prov, flags, data)))));
    }

    @Test
    void selfTest() throws ExecutionException {
        Set<SparqlClientFactory> factories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<String> queries = test().map(a -> (TestData) a.get()[3])
                                    .flatMap(d -> d.operands.stream()).collect(toSet());
        for (SparqlClientFactory factory : factories) {
            List<AsyncTask<?>> tasks = new ArrayList<>();
            try (SparqlClient<String[], byte[]> client = factory.createFor(HDTSS.asEndpoint())) {
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
    void test(SparqlClientFactory clientFactory, JoinProvider provider, long flags,
              TestData data) throws ExecutionException {
        if (provider.bid(flags) == BidCosts.UNSUPPORTED)
            return;
        Join join = provider.create(flags, StringArrayOperations.get());
        try (SparqlClient<String[], byte[]> client = clientFactory.createFor(HDTSS.asEndpoint())) {
            List<LeafPlan<String[]>> operands = data.operands.stream()
                    .map(s -> LeafPlan.builder(client, s).build()).collect(toList());
            ArrayList<LeafPlan<String[]>> operandsCopy = new ArrayList<>(operands);
            List<AsyncTask<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < N_THREADS; thread++) {
                futures.add(Async.async(() -> {
                    for (int i = 0; i < N_ITERATIONS; i++) {
                        data.assertExpected(join.checkedRun(join.asPlan(operands)));
                        assertEquals(operandsCopy, operands, "operands mutated");
                    }
                }));
            }
            for (AsyncTask<?> future : futures) future.get();
        }
    }
}
