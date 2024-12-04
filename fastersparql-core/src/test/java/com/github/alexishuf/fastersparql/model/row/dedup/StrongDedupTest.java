package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Guard.BatchGuard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CABatchType.CA;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class StrongDedupTest {
    @ParameterizedTest @ValueSource(ints = {1, 8, 15, 33, 256})
    void test(int strongUntil) {
        List<TermBatch> rows = new ArrayList<>();
        for (int i = 0; i < 2*strongUntil; i++)
            rows.add(TermBatch.of(List.of(Term.valueOf("\""+i+"\""))).takeOwnership(this));
        try (var dedupGuard = new Guard<StrongDedup<TermBatch>>(this)) {
            var dedup = dedupGuard.set(StrongDedup.createUntil(TERM, strongUntil, 1));
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
        } finally {
            for (TermBatch b : rows)
                b.recycle(this);
        }

    }

    private static final String EX = "<http://www.example.org/ns#";

    static Stream<Arguments> testClear() {
        List<Arguments> list = new ArrayList<>();
        for (var bt : List.of(TERM, COMPRESSED, CA)) {
            for (int strongUntil : List.of(1, 8, 15, 63, 64, 65, 255, 256, 257))
                list.add(arguments(bt, strongUntil));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource <B extends Batch<B>>
    void testClear(BatchType<B> bt, int strongUntil) {
        try (var b0Guard = new BatchGuard<B>(this);
             var b1Guard = new BatchGuard<B>(this);
             var dedupGuard = new Guard<StrongDedup<B>>(this)) {
            B b0  = b0Guard.set(bt.create(2));
            B b1  = b1Guard.set(bt.create(2));
            b0.putRow(Term.array("\"R0C0\"", EX+"R0C1>"));
            b1.putRow(Term.array(EX+"R0C0>", "\"R0C1\""));
            b1.putRow(Term.array("\"R1C0\"", EX+"R1C1>"));
            var dedup = dedupGuard.set(StrongDedup.createUntil(bt, strongUntil, 2));
            assertFalse(dedup.isDuplicate(b0, 0, 0));
            assertFalse(dedup.isDuplicate(b1, 1, 0));
            assertTrue (dedup.isDuplicate(b0, 0, 0));
            assertTrue (dedup.isDuplicate(b1, 1, 0));
            b0 = b0Guard.set(bt.create(2));
            b0.putRow(Term.array("\"R0C0\"", EX+"R0C1>"));
            assertTrue (dedup.isDuplicate(b0, 0, 0));
            dedup.clear(2);
            assertFalse(dedup.isDuplicate(b0, 0, 0));
            assertFalse(dedup.isDuplicate(b1, 1, 0));
            assertTrue (dedup.isDuplicate(b0, 0, 0));
            assertTrue (dedup.isDuplicate(b1, 1, 0));
        }
    }

}