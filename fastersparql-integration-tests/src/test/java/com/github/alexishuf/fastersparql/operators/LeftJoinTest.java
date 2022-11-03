package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.plan.LeftJoin;
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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
class LeftJoinTest {
    private static final Logger log = LoggerFactory.getLogger(LeftJoinTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(LeftJoinTest.class, "left_join.hdt", log);

    interface LeftJoinFactory {
        LeftJoin<List<String>, String> create(Plan<List<String>, String> left, Plan<List<String>, String> right);
    }

    private static class TestData extends ResultsChecker {
        private final SparqlQuery left, right;

        public TestData(String left, String right, String... values) {
            super(new SparqlQuery(left).publicVars.union(new SparqlQuery(right).publicVars), values);
            this.left = new SparqlQuery(PREFIX+left);
            this.right = new SparqlQuery(PREFIX+right);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData testData)) return false;
            return left.equals(testData.left) && right.equals(testData.right);
        }

        @Override public int hashCode() {
            return Objects.hash(left, right);
        }

        @Override public String toString() {
            return "TestData{" +  "left='" + left + '\'' +  ", right='" + right + '\'' +  '}';
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
        List<Arguments> list = new ArrayList<>();
        List<LeftJoinFactory> joinFactories = List.of(FSOps::leftJoin);
        for (var factory : TestUtils.allClientFactories()) {
            for (LeftJoinFactory joinFactory : joinFactories) {
                for (var testData : base)
                    list.add(arguments(factory, joinFactory, testData));
            }
        }
        assertFalse(list.isEmpty());
        return list.stream();
    }

    @Test
    void selfTest() throws Exception {
        Set<SparqlClientFactory> factories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<SparqlQuery> queries = test().map(a -> (TestData) a.get()[2])
                                    .flatMap(d -> Stream.of(d.left, d.right)).collect(toSet());
        for (SparqlClientFactory factory : factories) {
            try (var client = factory.createFor(HDTSS.asEndpoint());
                 var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
                queries.forEach(q -> tasks.add(() -> client.query(q).toList()));
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, LeftJoinFactory joinFactory, TestData d) throws Exception {
        try (var client = clientFactory.createFor(HDTSS.asEndpoint(), ListRow.STRING);
             var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            var l = FSOps.query(client, d.left);
            var r = FSOps.query(client, d.right);
            tasks.repeat(N_THREADS, () -> {
                for (int i = 0; i < N_ITERATIONS; i++)
                    d.assertExpected(joinFactory.create(l, r).execute());
            });
        }
    }
}