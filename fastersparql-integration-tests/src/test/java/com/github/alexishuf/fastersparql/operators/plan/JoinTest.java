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

import static com.github.alexishuf.fastersparql.FS.join;
import static com.github.alexishuf.fastersparql.FS.query;
import static com.github.alexishuf.fastersparql.util.Results.results;

@Testcontainers
public class JoinTest extends ResultsIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(JoinTest.class);

    @Container
    private static final HdtssContainer HDTSS =
            new HdtssContainer(MinusTest.class, "../join.hdt", log);

    @Override protected AutoCloseableSet<SparqlClient> createClients() {
        return AutoCloseableSet.of(FS.clientFor(HDTSS.asEndpoint()));
    }

    @Override protected List<Results> data() {
        return List.of(
            // 1-1 join
            /*  1 */results("?o", "?s", "1", ":right1")
                        .query(join(query("SELECT * WHERE {:left1 :p1 ?o}"),
                                    query("SELECT * WHERE {?s :q1 ?o}"))),
            // 2-row left, only first has a match
            /*  2 */results("?o", "?s", "2", ":right2")
                        .query(join(query("SELECT * WHERE {:left2 :p1 ?o}"),
                                    query("SELECT * WHERE {?s :q1 ?o}"))),
            // 2-row left, only second has a match
            /*  3 */results("?o", "?s", "5", ":right3")
                        .query(join(query("SELECT * WHERE {:left3 :p1 ?o}"),
                                    query("SELECT * WHERE {?s :q1 ?o}"))),
            // 1-row left without match
            /*  4 */results("?o", "?s")
                        .query(join(query("SELECT * WHERE {:left4 :p1 ?o}"),
                                    query("SELECT * WHERE {?s :q1 ?o}"))),
            // 1-row left with two matches
            /*  5 */results("?o", "?s",
                            "7",  ":right51",
                            "7",  ":right52")
                        .query(join(query("SELECT * WHERE {:left5 :p1 ?o}"),
                                    query("SELECT * WHERE {?s :q1 ?o}"))),
            // 2-row left with two matches for each row
            /*  6 */results("?o", "?s",
                            "8",  ":right61",
                            "8",  ":right62",
                            "9",  ":right63",
                            "9",  ":right64")
                        .query(join(query("SELECT * WHERE {:left6 :p1 ?o}"),
                                    query("SELECT * WHERE {?s :q1 ?o}"))),

            // 1-1 join with ASK right
            /*  7 */results("?o", "1")
                        .query(join(query("SELECT * WHERE {:left1 :p1 ?o}"),
                                    query("SELECT ?o WHERE {?s :q1 ?o}"))),
            // 1-1 join with ASK left (requires swap)
            /*  8 */results("?o", "1")
                        .query(join(query("ASK {?s :q1 ?o}"),
                                    query("SELECT * WHERE {:left1 :p1 ?o}"))),
            // 2-row left, only first has a match, ask right
            /*  9 */results("?o", "2")
                        .query(join(query("SELECT * WHERE {:left2 :p1 ?o}"),
                                    query("SELECT ?o WHERE {?s :q1 ?o}"))),

            // many left rows, 0-2 matches per row
            /* 10 */results("?left",   "?o", "?right",
                            ":left11", "12", ":right11",
                            ":left11", "14", ":right11",
                            ":left11", "16", ":right11",
                            ":left11", "18", ":right11",

                            ":left12", "21", ":right12",
                            ":left12", "23", ":right12",
                            ":left12", "25", ":right12",
                            ":left12", "27", ":right12",
                            ":left12", "29", ":right12",

                            ":left13", "32", ":right13",
                            ":left13", "34", ":right13",
                            ":left13", "36", ":right13",
                            ":left13", "38", ":right13",

                            ":left14", "41", ":right141",
                            ":left14", "41", ":right142",
                            ":left14", "43", ":right141",
                            ":left14", "43", ":right142",
                            ":left14", "45", ":right141",
                            ":left14", "45", ":right142",
                            ":left14", "47", ":right141",
                            ":left14", "47", ":right142",
                            ":left14", "49", ":right141",
                            ":left14", "49", ":right142")
                        .query(join(query("SELECT * WHERE {?left :p2 ?o}"),
                                    query("SELECT * WHERE {?right :q2 ?o}"))),

            //2-hop path with bound ends
            /* 11 */results("?b", "?c",
                            ":n2", ":n3",
                            ":n2", ":n4")
                        .query(join(
                                query("SELECT * WHERE {:n1 :p3.1 ?b}"),
                                query("SELECT * WHERE {?b  :p3.2 ?c}"),
                                query("SELECT * WHERE {?c  :p3.3 :n5}"))),
            //2-hop path with bound ends, reverse order
            /* 12 */results("?c", "?b",
                            ":n3", ":n2",
                            ":n4", ":n2")
                        .query(join(
                                query("SELECT * WHERE {?c  :p3.3 :n5}"),
                                query("SELECT * WHERE {?b  :p3.2 ?c}"),
                                query("SELECT * WHERE {:n1 :p3.1 ?b}"))),
            // 2-hop path
            /* 13 */results("?a",  "?b",  "?c",  "?d",
                            ":n1", ":n2", ":n3", ":n5",
                            ":n1", ":n2", ":n4", ":n5")
                        .query(join(
                                query("SELECT * WHERE {?a :p3.1 ?b}"),
                                query("SELECT * WHERE {?b :p3.2 ?c}"),
                                query("SELECT * WHERE {?c :p3.3 ?d}"))),
            // find paths, efficient even on hash join
            /* 14 */results("?a",  "?b",  "?c",  "?d",  "?e",
                            ":n1", ":n2", ":n3", ":n5", ":n6",
                            ":n1", ":n2", ":n4", ":n5", ":n6")
                        .query(join(
                                query("SELECT * WHERE {?a :p3.1 ?b}"),
                                query("SELECT * WHERE {?b :p3.2 ?c}"),
                                query("SELECT * WHERE {?c :p3.3 ?d}"),
                                query("SELECT * WHERE {?d :p3.4 ?e}"))),

            // explore multiple paths, with mostly discarded intermediate results (if not bound)
            /* 15 */results("?a",  "?b",  "?c",  "?d",
                            ":s2", ":s4", ":s5", ":s6",
                            ":s2", ":s4", ":s5", ":s7",
                            ":s3", ":s4", ":s5", ":s6",
                            ":s3", ":s4", ":s5", ":s7")
                        .query(join(
                                query("SELECT * WHERE {:s1 :p4 ?a}"),
                                query("SELECT * WHERE {?a  :p4 ?b}"),
                                query("SELECT * WHERE {?b  :p4 ?c}"),
                                query("SELECT * WHERE {?c  :p4 ?d}"))),

            // explore multiple paths, requires reorder to avoid cartesian
            /* 16 */results("?a",  "?b",  "?c",  "?d",
                            ":s2", ":s4", ":s5", ":s6",
                            ":s2", ":s4", ":s5", ":s7",
                            ":s3", ":s4", ":s5", ":s6",
                            ":s3", ":s4", ":s5", ":s7")
                        .query(join(
                                query("SELECT * WHERE {:s1 :p4 ?a}"),
                                query("SELECT * WHERE {?b  :p4 ?c}"),
                                query("SELECT * WHERE {?a  :p4 ?b}"),
                                query("SELECT * WHERE {?c  :p4 ?d}")))
        );
    }
}
