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

import static com.github.alexishuf.fastersparql.FS.minus;
import static com.github.alexishuf.fastersparql.FS.query;
import static com.github.alexishuf.fastersparql.util.Results.results;

@Testcontainers
public class MinusTest extends ResultsIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(MinusTest.class);

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(MinusTest.class, "../minus.hdt", log);

    @Override protected AutoCloseableSet<SparqlClient> createClients() {
        return AutoCloseableSet.of(FS.clientFor(HDTSS.asEndpoint()));
    }

    @Override protected List<Results> data() {
        return List.of(
                // 1 row in left without match
        /*  1 */results("?o", "23")
                        .query(minus(query("SELECT ?o WHERE {:left1 :p1 ?o}"),
                                     query("SELECT * WHERE {?r :q1 ?o}"))),
                // 1 row in left with match
        /*  2 */results("?o")
                        .query(minus(query("SELECT ?o WHERE {:left2 :p1 ?o}"),
                                     query("SELECT * WHERE {?r :q1 ?o}"))),
                // 2 rows in left, first matches
        /*  3 */results("?o", 3)
                        .query(minus(query("SELECT ?o WHERE {:left3 :p1 ?o}"),
                                     query("SELECT * WHERE {?r :q1 ?o}"))),
                // 2 rows in left, second matches
        /*  4 */results("?o", 7)
                        .query(minus(query("SELECT ?o WHERE {:left4 :p1 ?o}"),
                                     query("SELECT * WHERE {?r :q1 ?o}"))),
                // 2 rows in left, none matches
        /*  5 */results("?o", 17, 27)
                        .query(minus(query("SELECT ?o WHERE {:left5 :p1 ?o}"),
                                     query("SELECT * WHERE {?r :q1 ?o}"))),

                // empty left, non-empty right
        /*  6 */results("?o")
                        .query(minus(query("SELECT * WHERE {:left13 :p2 ?o}"),
                                     query("SELECT * WHERE {?x :q2 ?o}"))),
                // empty left, non-empty right, no shared vars
        /*  7 */results("?o")
                        .query(minus(query("SELECT * WHERE {:left13 :p2 ?o}"),
                                     query("SELECT * WHERE {?x :q2 ?z}"))),
                // singleton left, non-empty right, no shared vars
        /*  8 */results("?o", 2)
                        .query(minus(query("SELECT * WHERE {:left11 :p2 ?o}"),
                                     query("SELECT * WHERE {?x :q2 ?y}"))),
                // 2-rows left, non-empty right, no shared vars
        /*  9 */results("?o", 2, 3)
                        .query(minus(query("SELECT * WHERE {:left12 :p2 ?o}"),
                                     query("SELECT * WHERE {?x :q2 ?y}"))),
                // 3-rows left with two vars, non-empty right, no shared vars
        /* 10 */results("?s",      "?o",
                        ":left11", 2,
                        ":left12", 2,
                        ":left12", 3)
                        .query(minus(query("SELECT * WHERE {?s :p2 ?o}"),
                                     query("SELECT * WHERE {?x :q2 ?y}"))),

                // many left rows, half have 1+ match
        /* 11 */results("?l",      "?o",
                        ":left21", "12",
                        ":left21", "14",
                        ":left21", "16",
                        ":left21", "18",
                        ":left22", "21",
                        ":left22", "23",
                        ":left22", "25",
                        ":left22", "27",
                        ":left22", "29",
                        ":left23", "32",
                        ":left23", "34",
                        ":left23", "36",
                        ":left23", "38")
                        .query(minus(query("SELECT * WHERE {?l :p3 ?o}"),
                                     query("SELECT * WHERE {?r :q3 ?o}")))
        );
    }

}
