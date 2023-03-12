package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WeakDedupTest {
    @Test
    void testHasAdd() {
        Term[] row = {Term.typed("1", RopeDict.DT_integer)};
        var dedup = new WeakDedup<>(RowType.ARRAY, 10);
        assertFalse(dedup.contains(row));
        assertFalse(dedup.contains(row));
        assertTrue(dedup.add(row));
        assertTrue (dedup.contains(row));
        assertFalse(dedup.add(row));
        assertTrue (dedup.contains(row));
    }

}