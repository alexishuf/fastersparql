package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrongDedupTest {
    @ParameterizedTest @ValueSource(ints = {1, 8, 15, 33, 256})
    void test(int strongUntil) {
        List<TermBatch> rows = new ArrayList<>();
        for (int i = 0; i < 2*strongUntil; i++)
            rows.add(TermBatch.of(List.of(Term.valueOf("\""+i+"\""))));

        var dedup = StrongDedup.strongUntil(Batch.TERM, strongUntil, 1);
        for (int i = 0; i < strongUntil; i++) {
            for (int j = 0; j < i; j++)
                assertTrue(dedup.contains(rows.get(j), 0), "i="+i+", j="+j);
            assertFalse(dedup.contains(rows.get(i), 0));
            assertFalse(dedup.isDuplicate(rows.get(i), 0, 0));
            assertTrue(dedup.contains(rows.get(i), 0));
            assertFalse(dedup.add(rows.get(i), 0));
        }

        for (int i = strongUntil; i < 2 * strongUntil; i++) {
//            for (int j = 0; j < strongUntil; j++)
//                assertTrue(dedup.contains(rows.get(j), 0), "i="+i+", j="+j);
            assertFalse(dedup.contains(rows.get(i), 0), "i="+i);
            assertFalse(dedup.isDuplicate(rows.get(i), 0, 0), "i="+i);
            assertTrue(dedup.contains(rows.get(i), 0), "i="+i);
            assertFalse(dedup.add(rows.get(i), 0), "i="+i);
        }
    }

}