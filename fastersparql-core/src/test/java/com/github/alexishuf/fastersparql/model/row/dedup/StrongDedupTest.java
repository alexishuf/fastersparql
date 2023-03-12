package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.model.row.RowType;
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
        List<List<Term>> rows = new ArrayList<>();
        for (int i = 0; i < 2*strongUntil; i++)
            rows.add(List.of(Term.valueOf("\""+i+"\"")));

        var dedup = StrongDedup.strongUntil(RowType.LIST, strongUntil);
        for (int i = 0; i < strongUntil; i++) {
            for (int j = i < 2 ? 0 : i&1; j < i; j += 2)
                assertTrue(dedup.contains(rows.get(j)), "i="+i+", j="+j);
            assertFalse(dedup.contains(rows.get(i)));
            assertFalse(dedup.isDuplicate(rows.get(i), 0));
            assertTrue(dedup.contains(rows.get(i)));
            assertFalse(dedup.add(rows.get(i)));
        }

        for (int i = strongUntil; i < 2 * strongUntil; i++) {
            for (int j = 0; j < strongUntil; j++)
                assertTrue(dedup.contains(rows.get(j)), "i="+i+", j="+j);
            assertFalse(dedup.contains(rows.get(i)), "i="+i);
            assertFalse(dedup.isDuplicate(rows.get(i), 0), "i="+i);
            assertTrue(dedup.contains(rows.get(i)), "i="+i);
            assertFalse(dedup.add(rows.get(i)), "i="+i);
        }
    }

}