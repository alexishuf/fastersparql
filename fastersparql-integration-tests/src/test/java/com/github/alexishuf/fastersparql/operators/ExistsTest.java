package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.plan.Exists;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class ExistsTest {
    private static final Logger log = LoggerFactory.getLogger(ExistsTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(ExistsTest.class, "filter_exists.hdt", log);

    @FunctionalInterface
    interface ExistsFactory {
        Exists<List<String>, String> create(Plan<List<String>, String> in, boolean negate,
                                            Plan<List<String>, String> filter);
    }

    private static final class TestData extends ResultsChecker {
        final OpaqueSparqlQuery left;
        final boolean negate;
        final OpaqueSparqlQuery filter;

        public TestData(String left, boolean negate, String filter, String... values) {
            super(new OpaqueSparqlQuery(left).publicVars, values);
            this.left = new OpaqueSparqlQuery(PREFIX+left);
            this.filter = new OpaqueSparqlQuery(PREFIX+filter);
            this.negate = negate;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData testData)) return false;
            return negate == testData.negate && left.equals(testData.left) && filter.equals(testData.filter);
        }

        @Override public int hashCode() {
            return Objects.hash(left, negate, filter);
        }

        @Override public String toString() {
            return "TestData{" +  "left='" + left + '\'' +  ", negate=" + negate +  ", filter='" + filter + '\'' +  '}';
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
        List<Arguments> list = new ArrayList<>();
        List<ExistsFactory> planFactories = List.of(FSOps::exists);
        for (var factory : TestUtils.allClientFactories()) {
            for (ExistsFactory existsFactory : planFactories) {
                for (TestData d : base)
                    list.add(arguments(factory, existsFactory, d));
            }
        }
        return list.stream();
    }

    @Test
    void selfTest() throws Exception {
        Set<SparqlClientFactory> clientFactories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<SparqlQuery> queries = test().map(a -> (TestData) a.get()[3])
                                    .flatMap(d -> Stream.of(d.left, d.filter)).collect(toSet());

        for (SparqlClientFactory fac : clientFactories) {
            try (var client = fac.createFor(HDTSS.asEndpoint());
                 var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
                for (var sparql : queries)
                    tasks.add(() -> client.query(sparql).toList());
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, ExistsFactory existsFactory, TestData data) throws Exception {
        try (var client = clientFactory.createFor(HDTSS.asEndpoint(), ListRow.STRING);
             var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            var left = FSOps.query(client, data.left);
            var filter = FSOps.query(client, data.filter);
            var plan = existsFactory.create(left, data.negate, filter);
            tasks.repeat(N_THREADS, () -> {
                for (int i = 0; i < N_ITERATIONS; i++)
                    data.assertExpected(plan.execute());
            });
        }
    }
}
