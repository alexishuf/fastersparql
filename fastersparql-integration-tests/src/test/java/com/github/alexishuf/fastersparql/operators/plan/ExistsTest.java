package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static com.github.alexishuf.fastersparql.util.Results.negativeResult;
import static com.github.alexishuf.fastersparql.util.Results.results;

@Testcontainers
public class ExistsTest extends ResultsIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ExistsTest.class);

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(ExistsTest.class, "../filter_exists.hdt", log);

    @Override protected AutoCloseableSet<SparqlClient> createClients() {
        return AutoCloseableSet.of(FS.clientFor(HDTSS.asEndpoint()));
    }

    @Override protected List<Results> data() {
        return List.of(
                // single row has match
        /*  1 */results("?o", "1")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left1 :p1 ?o} "),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
        /*  2 */results(Vars.of("o"))
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left1 :p1 ?o} "),
                                            FS.query("SELECT * WHERE {?s :q1 ?o}"))),
                // repeat now using ASK
        /*  3 */results("?o", "1")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left1 :p1 ?o} "),
                                         FS.query("ASK {?s :q1 ?o}"))),
        /*  4 */results(Vars.of("o"))
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left1 :p1 ?o} "),
                                            FS.query("ASK {?s :q1 ?o}"))),
                // single row has no match
        /*  5 */results(Vars.of("o"))
                        .query(FS.exists(FS.query("SELECT ?o WHERE {:left2 :p1 ?o}"),
                                         FS.query("SELECT ?s WHERE {?s :q1 ?o}"))),
        /*  6 */results("?o", "2")
                        .query(FS.notExists(FS.query("SELECT ?o WHERE {:left2 :p1 ?o}"),
                                            FS.query("SELECT ?s WHERE {?s :q1 ?o}"))),
                // ... repeat using ASK
        /*  7 */results(Vars.of("o"))
                        .query(FS.exists(FS.query("SELECT ?o WHERE {:left2 :p1 ?o}"),
                                         FS.query("ASK {?s :q1 ?o}"))),
        /*  8 */results("?o", "2")
                        .query(FS.notExists(FS.query("SELECT ?o WHERE {:left2 :p1 ?o}"),
                                            FS.query("ASK {?s :q1 ?o}"))),
                // two rows, first has match
        /*  9 */results(":o", "4")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left3 :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
        /* 10 */results("?o", "5")
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left3 :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
                // ... repeat using ASK
        /* 11 */results("?o", "4")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left3 :p1 ?o}"),
                                         FS.query("ASK {?s :q1 ?o}"))),
        /* 12 */results("?o", "5")
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left3 :p1 ?o}"),
                                            FS.query("ASK {?s :q1 ?o}"))),

                // two rows, second has match
        /* 13 */results("?o", "7")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left4 :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
        /* 14 */results("?o", "6")
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left4 :p1 ?o}"),
                                            FS.query("SELECT * WHERE {?s :q1 ?o}"))),
                // ... repeat with ASK
        /* 15 */results("?o", "7")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left4 :p1 ?o}"),
                                         FS.query("ASK {?s :q1 ?o}"))),
        /* 16 */results("?o", "6")
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left4 :p1 ?o}"),
                                            FS.query("ASK {?s :q1 ?o}"))),

                // single row has two matches
        /* 17 */results("?o", "8")
                        .query(FS.exists(FS.query("SELECT * WHERE {:left5 :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
        /* 18 */results(Vars.of("o"))
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left5 :p1 ?o}"),
                                            FS.query("SELECT * WHERE {?s :q1 ?o}"))),
                // ... repeat with ASK
        /* 19 */results("?o", "8").query(FS.exists(FS.query("SELECT * WHERE {:left5 :p1 ?o}"),
                                                   FS.query("ASK {?s :q1 ?o}"))),
        /* 20 */results(Vars.of("?o"))
                        .query(FS.notExists(FS.query("SELECT * WHERE {:left5 :p1 ?o}"),
                                            FS.query("ASK {?s :q1 ?o}"))),

                // "7-row" ask query at left with non-empty filter
        /* 21 */negativeResult()
                        .query(FS.exists(FS.query("ASK {?s :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
        /* 22 */negativeResult()
                        .query(FS.notExists(FS.query("ASK {?s :p1 ?o}"),
                                            FS.query("SELECT * WHERE {?s :q1 ?o}"))),

                // "7-row" ask query at left with empty filter
        /* 23 */negativeResult()
                        .query(FS.exists(FS.query("ASK {?s :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q0 ?o}"))),
        /* 24 */negativeResult()
                        .query(FS.notExists(FS.query("ASK {?s :p1 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q0 ?o}"))),

                // empty ask query at left with non-empty filter
        /* 25 */negativeResult()
                        .query(FS.exists(FS.query("ASK {?s :p0 ?o}"),
                                         FS.query("SELECT * WHERE {?s :q1 ?o}"))),
        /* 26 */negativeResult()
                        .query(FS.notExists(FS.query("ASK {?s :p0 ?o}"),
                                            FS.query("SELECT * WHERE {?s :q1 ?o}"))),

                //many rows at left scenario
        /* 27 */results("?o", "11", "13", "15", "17", "19", "22", "24", "26", "28")
                        .query(FS.exists(FS.query("SELECT ?o WHERE {?s :p2 ?o}"),
                                         FS.query("SELECT ?s WHERE {?s :q2 ?o}"))),
        /* 28 */results("?o", "12", "14", "16", "18", "21", "23", "25", "27", "29")
                        .query(FS.notExists(FS.query("SELECT ?o WHERE {?s :p2 ?o}"),
                                            FS.query("SELECT ?s WHERE {?s :q2 ?o}"))),
                // ... repeat with ASK
        /* 29 */results("?o", "11", "13", "15", "17", "19", "22", "24", "26", "28")
                        .query(FS.exists(FS.query("SELECT ?o WHERE {?s :p2 ?o}"),
                                         FS.query("ASK {?s :q2 ?o}"))),
        /*  30 */results("?o", "12", "14", "16", "18", "21", "23", "25", "27", "29")
                        .query(FS.notExists(FS.query("SELECT ?o WHERE {?s :p2 ?o}"),
                                            FS.query("ASK {?s :q2 ?o}")))
        );
    }
}
