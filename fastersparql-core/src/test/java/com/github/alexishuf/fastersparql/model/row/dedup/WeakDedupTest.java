package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WeakDedupTest {
    @Test
    void testHasAdd() {
        Term one = Term.valueOf("\"1" + SharedRopes.DT_integer);
        try (var batchGuard = new Guard.BatchGuard<TermBatch>(this);
             var dedupGuard = new Guard<WeakDedup<TermBatch>>(this)) {
            var batch = batchGuard.set(TermBatch.of(List.of(one)));
            var dedup = dedupGuard.set(Dedup.weak(TERM, 1, DistinctType.WEAK));
            assertFalse(dedup.contains(batch, 0));
            assertFalse(dedup.contains(batch, 0));
            assertTrue(dedup.add(batch, 0));
            assertTrue (dedup.contains(batch, 0));
            assertFalse(dedup.add(batch, 0));
            assertTrue (dedup.contains(batch, 0));
        }
    }
}