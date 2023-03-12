package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexishuf.fastersparql.FS.crossDedupUnion;
import static com.github.alexishuf.fastersparql.util.Results.results;

public class CrossDedupUnionTest {

    static List<Results> data() {
        return Results.resultsList(
                results("?x", 1)
                        .query(crossDedupUnion(results("?x", 1).asPlan())),
                results("?x ?y", 1, 2)
                        .query(crossDedupUnion(results("?x ?y", 1, 2).asPlan())),
                results("?x", 1, 2)
                        .query(crossDedupUnion(
                                results("?x", 1).asPlan(),
                                results("?x", 2).asPlan())),
                results("?x", 1, 2, 3, 4)
                        .query(crossDedupUnion(
                                results("?x", 1, 2).asPlan(),
                                results("?x", 3, 4).asPlan())),
                results("?x", "?y", "1", null, null, "2")
                        .query(crossDedupUnion(
                                results("?x", 1).asPlan(),
                                results("?y", 2).asPlan())),
                results("?x", "?y",
                        "11", null,
                        "21", "22",
                        "23", "24",
                        null, "31")
                        .query(crossDedupUnion(
                                results("?x", 11).asPlan(),
                                results("?x ?y",
                                        21, 22,
                                        23, 24).asPlan(),
                                results("?y", 31).asPlan()))
        );
    }

    @Test void test() {
        for (Results results : data())
            results.check();
    }
}
