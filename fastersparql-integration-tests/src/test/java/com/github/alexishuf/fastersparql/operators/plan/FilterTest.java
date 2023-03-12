package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static com.github.alexishuf.fastersparql.util.Results.results;
import static java.util.Arrays.asList;

@Testcontainers
class FilterTest  extends ResultsIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(FilterTest.class);

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(ExistsTest.class, "../filter.hdt", log);

    @Override protected AutoCloseableSet<SparqlClient> createClients() {
        return AutoCloseableSet.of(FS.clientFor(HDTSS.asEndpoint()));
    }

    private static Results data(String triplePattern, List<String> filters,
                                String... values) {
        var tp = Results.parseTP(triplePattern);
        return results(tp.publicVars(), (Object[])values).query(FS.filter(tp, filters));
    }

    @Override protected List<Results> data() {
        return List.of(
        /*  1 */data("?s :p1 ?o", List.of("?o = 1"),
                     ":left11", "\"1\""),
        /*  2 */data("?s :p1 ?o", List.of("?o > 0"),
                     ":left11", "\"1\""),
        /*  3 */data("?s :p1 ?o", asList("?o = 1", "?o > 0"),
                     ":left11", "\"1\""),
        /*  4 */data("?s :p1 ?o", List.of("?o > 1")),
        /*  5 */data("?s :p1 ?o", asList("?o = 1", "?o > 1")),

        /*  6 */data("?s :p2 ?o", List.of("?o = 1"),
                     ":left21", "\"1\""),
        /*  7 */data("?s :p2 ?o", List.of("?o = 2"),
                     ":left22", "\"2\""),
        /*  8 */data("?s :p2 ?o", List.of("?o = 2", "?o > 1"),
                     ":left22", "\"2\""),
        /*  9 */data("?s :p2 ?o", List.of("?o = 2", "?o > 2")),

        /* 10 */data("?s :p3 ?o", List.of("?o > 3", "?o < 7"),
                     ":left34", "\"4\"",
                     ":left35", "\"5\"",
                     ":left36", "\"6\""),
        /* 11 */data("?s :p3 ?o", List.of("?o != 4", "?o > 0"),
                     ":left31", "\"1\"",
                     ":left32", "\"2\"",
                     ":left33", "\"3\"",
                     ":left35", "\"5\"",
                     ":left36", "\"6\"",
                     ":left37", "\"7\"",
                     ":left38", "\"8\"",
                     ":left39", "\"9\"")
        );
    }
}