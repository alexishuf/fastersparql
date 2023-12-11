package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.util.Results.DuplicatesPolicy.REQUIRE_DEDUP;
import static com.github.alexishuf.fastersparql.util.Results.results;

class DistinctTest {
    @FunctionalInterface
    private interface DistinctFactory {
        Modifier create(Plan in);
    }

    public static List<Results> data() {
        List<DistinctFactory> factories = List.of(
                FS::distinct,
                FS::reduced,
                FS::dedup
        );
        List<Results> inputs = List.of(
                // no duplicates
                results(),
                results("?x0", "_:b00"),
                results("?x0     ?x1",
                        "_:b00", "_:b01"),
                results("?x0", "_:b00", "_:b10"),
                results("?x0   ?x1",
                        "_:x", "_:x0",
                        "_:x", "_:x1"),

                // two rows, one duplicate
                results("?x0", "_:x", "_:x"),
                results("?x0   ?x1",
                        "_:x", "_:y",
                        "_:x", "_:y"),

                // three rows, second is duplicate
                results("?x0   ?x1",
                        "_:x", "_:y",
                        "_:x", "_:y",
                        "_:x", "_:z"),
                results("?x0   ?x1",
                        "_:x", "_:y",
                        "_:x", "_:y",
                        "_:z", "_:y"),

                // three rows, third is duplicate
                results("?x0   ?x1",
                        "_:x", "_:y",
                        "_:x", "_:z",
                        "_:x", "_:y"),
                results("?x0   ?x1",
                        "_:x", "_:y",
                        "_:z", "_:y",
                        "_:z", "_:y")
        );
        List<Results> list = new ArrayList<>();
        for (var factory : factories) {
            for (var input : inputs)
                list.add(input.duplicates(REQUIRE_DEDUP).query(factory.create(input.asPlan())));
        }
        return Results.contextualize(list);
    }

    @Test
    void test() {
        for (Results d : data())
            d.check();
    }
}