package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WeakDedupTest {
    @Test
    void testHasAdd() {
        TermBatch batch = TermBatch.of(List.of(Term.valueOf("\"1"+SharedRopes.DT_integer)));
        var dedup = new WeakDedup<>(TermBatchType.TERM, 1, DistinctType.WEAK);
        assertFalse(dedup.contains(batch, 0));
        assertFalse(dedup.contains(batch, 0));
        assertTrue(dedup.add(batch, 0));
        assertTrue (dedup.contains(batch, 0));
        assertFalse(dedup.add(batch, 0));
        assertTrue (dedup.contains(batch, 0));
    }
}