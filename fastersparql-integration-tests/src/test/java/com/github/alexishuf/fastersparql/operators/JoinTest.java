package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.TestUtils;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.plan.Join;
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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class JoinTest {
    private static final Logger log = LoggerFactory.getLogger(JoinTest.class);
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(MinusTest.class, "join.hdt", log);

    interface JoinFactory {
        Join<List<String>, String> create(List<? extends Plan<List<String>, String>> operands);
    }

    private static class TestData extends ResultsChecker {
        final List<SparqlQuery> operands;

        public TestData(List<SparqlQuery> operands, String... values) {
            super(operands.stream().map(SparqlQuery::publicVars)
                                   .reduce(Vars.EMPTY, Vars::union),
                  values);
            this.operands = operands.stream().map(q -> new OpaqueSparqlQuery(PREFIX+q.sparql())).collect(toList());
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestData testData)) return false;
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
        return new TestData(asList(new OpaqueSparqlQuery(left), new OpaqueSparqlQuery(right)), expected);
    }
    private static TestData nData(List<String> operands, String... expected) {
        return new TestData(operands.stream().map(OpaqueSparqlQuery::new).collect(toList()), expected);
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
        List<Arguments> list = new ArrayList<>();
        List<JoinFactory> joinFactories = List.of(FSOps::join);
        for (SparqlClientFactory factory : TestUtils.allClientFactories()) {
            for (JoinFactory joinFactory : joinFactories) {
                for (TestData data : base)
                    list.add(arguments(factory, joinFactory, data));
            }
        }
        return list.stream();
    }

    @Test
    void selfTest() throws Exception {
        Set<SparqlClientFactory> factories = test().map(a -> (SparqlClientFactory) a.get()[0]).collect(toSet());
        Set<SparqlQuery> queries = test().map(a -> (TestData) a.get()[2])
                                    .flatMap(d -> d.operands.stream()).collect(toSet());
        for (SparqlClientFactory factory : factories) {
            try (var client = factory.createFor(HDTSS.asEndpoint());
                 var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
                for (var sparql : queries)
                    tasks.add(() -> assertFalse(client.query(sparql).toList().isEmpty()));
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(SparqlClientFactory clientFactory, JoinFactory joinFactory,
              TestData data) throws Exception {
        try (var client = clientFactory.createFor(HDTSS.asEndpoint(), ListRow.STRING);
             var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            var operands = data.operands.stream().map(s -> FSOps.query(client, s)).toList();
            tasks.repeat(N_THREADS, () -> {
                for (int i = 0; i < N_ITERATIONS; i++)
                    data.assertExpected(joinFactory.create(operands).execute());
            });
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void main(String[] args) throws Exception {
        HDTSS.start();
        System.out.println("HDTSS ready, hit [ENTER] to run tests");
        System.in.read();
        var test = new JoinTest();
        List<Arguments> list = JoinTest.test().limit(5).toList();
        for (int i = 0; i < list.size(); i++) {
            System.out.printf("test row %d...\n", i);
            Object[] a = list.get(i).get();
            test.test((SparqlClientFactory) a[0], (JoinFactory) a[1], (TestData) a[2]);
        }
        System.out.println("Tests passed, hit [ENTER] to exit");
        System.in.read();
    }
}
