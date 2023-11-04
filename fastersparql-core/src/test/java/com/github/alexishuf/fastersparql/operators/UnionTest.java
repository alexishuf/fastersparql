package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.FS.union;
import static com.github.alexishuf.fastersparql.util.Results.results;

public class UnionTest {

    static List<Results> data() {
        List<Results> list = new ArrayList<>(List.of(
                //empty inputs, test union of var names
        /*  1 */results(Vars.of("x")).query(union(results(Vars.of("x")).asPlan())),
        /*  2 */results("?x", "?y").query(union(results("?x", "?y").asPlan())),
        /*  3 */results("?x", "?y", "?z").query(union(
                        results("?x", "?y").asPlan(),
                        results("?y", "?z").asPlan())),
                // two inputs with single row, overlapping var names
        /*  4 */results("?x", "?y", "_:x", null, "_:y", "_:z").query(union(
                        results("?x", "_:x").asPlan(),
                        results("?x", "?y", "_:y", "_:z").asPlan())),
                //two inputs with two rows, non-overlapping var names
        /*  5 */results("?x",  "?y",  "?z",
                        "_:a", "_:b", null,
                        null,  "_:c", null,
                        null,  null,  "_:d",
                        null,  null,  null
                ).query(union(
                        results("?x",  "?y",
                                "_:a", "_:b",
                                null,  "_:c").asPlan(),
                        results("?z", "_:d", null).asPlan()
                )),
                //four inputs with two rows, all with the same single var
        /*  6 */results("?x", "_:a", "_:b", "_:c", "_:d", "_:e", "_:f", "_:g", "_:h"
                ).query(union(
                        results("?x", "_:a", "_:b").asPlan(),
                        results("?x", "_:c", "_:d").asPlan(),
                        results("?x", "_:e", "_:f").asPlan(),
                        results("?x", "_:g", "_:h").asPlan()
                )),
                // retain cross-source duplicate
        /*  7 */results("?x", "1", "2", "1").query(union(
                        results("?x", "1", "2").asPlan(),
                        results("?x", "1").asPlan())),
                // retain both intra-source and cross-source duplicates
        /*  8 */results("?x", "1", "2", "1", "3", "2").query(union(
                        results("?x", "1", "2", "1").asPlan(),
                        results("?x", "3", "2").asPlan()
                )),
                // eliminate cross-source duplicate but retain intra-source
        /*  9 */results("?x", "1", "2", "1", "3", "4").query(union(true,
                        results("?x", "1", "2", "1").asPlan(),
                        results("?x", "3", "2", "4").asPlan()
                ))
        ));

        Plan[] longInputs = new Plan[16];
        List<List<Term>> expected = new ArrayList<>();
        for (int col = 0; col < longInputs.length; col++) {
            List<List<Term>> rows = new ArrayList<>();
            for (int row = 0; row < 2048; row++)
                rows.add(List.of(Term.valueOf("_:"+col+"."+row)));
            longInputs[col] = results(Vars.of("x"), rows).asPlan();
            expected.addAll(rows);
        }
        list.add(results(Vars.of("x"), expected).query(union(longInputs)));

        return Results.contextualize(list);
    }

    @Test void test() {
        for (Results results : data())
            results.check();
    }
}
