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

import static com.github.alexishuf.fastersparql.FS.leftJoin;
import static com.github.alexishuf.fastersparql.FS.query;
import static com.github.alexishuf.fastersparql.util.Results.results;

@Testcontainers
class LeftJoinTest extends ResultsIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(LeftJoinTest.class);

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(LeftJoinTest.class, "../left_join.hdt", log);

    @Override protected AutoCloseableSet<SparqlClient> createClients() {
        return AutoCloseableSet.of(FS.clientFor(HDTSS.asEndpoint()));
    }

    @Override protected List<Results> data() {
        return List.of(
                //right always has a match
        /* 1 */ results("?o", "?s", "5", ":right1")
                        .query(leftJoin(
                                query("SELECT ?o WHERE {:left1 :p1 ?o}"),
                                query("SELECT * WHERE {?s :q1 ?o}"))),
                //right never has a match
        /* 2 */ results("?o", "?s", "23", null)
                        .query(leftJoin(query("SELECT ?o WHERE {:left3 :p1 ?o}"),
                                        query("SELECT * WHERE {?s :q1 ?o}"))),
                //right sometimes matches
        /* 3 */ results("?o", "?s", "2", null, "3", ":right2")
                        .query(leftJoin(query("SELECT ?o WHERE {:left2 :p1 ?o}"),
                                        query("SELECT * WHERE {?s :q1 ?o}"))),
                //changing right projection has no effect
        /* 4 */ results("?o", "?s", "2", null, "3", ":right2")
                        .query(leftJoin(query("SELECT * WHERE {:left2 :p1 ?o}"),
                                        query("SELECT ?o ?s WHERE {?s :q1 ?o}"))),
                //the three above
        /* 5 */ results("?o", "?s",
                        "5",  ":right1",
                        "2",  null,
                        "3",  ":right2",
                        "23", null)
                        .query(leftJoin(query("SELECT ?o WHERE {?s :p1 ?o}"),
                                        query("SELECT * WHERE {?s :q1 ?o}"))),
                /* many rows on left, each with 0-1 matches */
        /* 6 */ results("?o", "?s",
                        "11", ":right1",
                        "12", null,
                        "13", ":right1",
                        "14", null,
                        "15", ":right1",
                        "16", null,
                        "17", ":right1",
                        "18", null
                        ).query(leftJoin(query("SELECT * WHERE {:lleft1 :p2 ?o}"),
                                        query("SELECT * WHERE {?s :q2 ?o}"))),
        /* 7 */ results("?o", "?s",
                        "21", ":right2",
                        "21", ":right3",
                        "22", null,
                        "23", ":right2",
                        "23", ":right3",
                        "24", null,
                        "25", ":right2",
                        "25", ":right3",
                        "26", null,
                        "27", ":right2",
                        "27", ":right3",
                        "28", null)
                        .query(leftJoin(query("SELECT * WHERE {:lleft2 :p2 ?o}"),
                                        query("SELECT * WHERE {?s :q2 ?o}"))),

                /* few left rows, each with 0-8 matches */
        /* 8 */ results("?o", "?s",
                        "1", ":kright1",
                        "1", ":kright3",
                        "1", ":kright5",
                        "1", ":kright7",
                        "2", ":kright2",
                        "2", ":kright4",
                        "2", ":kright6",
                        "2", ":kright8",
                        "3", ":kright1",
                        "3", ":kright2",
                        "3", ":kright3",
                        "3", ":kright4",
                        "3", ":kright5",
                        "3", ":kright6",
                        "3", ":kright7",
                        "3", ":kright8",
                        "4", null)
                        .query(leftJoin(query("SELECT ?o WHERE {?s :p3 ?o}"),
                                        query("SELECT * WHERE {?s :q3 ?o}")))
        );
    }
}