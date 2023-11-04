package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.PoolCleaner;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Runtime.getRuntime;
import static org.junit.jupiter.api.Assertions.*;

class BatchPoolTest {
    private static final int THREAD_BUCKETS = 8;
    private static final BatchPool.Factory<TermBatch> F
            = () -> new TermBatch(new Term[8], 0, 1, false);

    private void drain(BatchPool<TermBatch> pool) {
        for (int thread = 0; thread <= THREAD_BUCKETS; thread++) {
            for (TermBatch b; (b = pool.getFromStack(thread)) != null; )
                b.markPooled().markGarbage();
        }
    }

    @RepeatedTest(10) void testSerial() {
        var pool = new BatchPool<>(TermBatch.class, F, THREAD_BUCKETS, 100);
        for (int thread = 0; thread < THREAD_BUCKETS; thread++) {
            TermBatch b = pool.getFromStack(thread);
            assertNotNull(b, "pool not primed for thread="+thread);
            assertNull(pool.offerToStack(thread, b), "pool has a vacancy, cannot reject");
            assertSame(b, pool.getFromStack(thread), "pool is not LIFO");
            assertNull(pool.offerToStack(thread, b), "pool has vacancy, cannot reject");
        }

        TermBatch b0 = pool.getFromStack(0);
        assertNotNull(b0);
        assertNull(pool.offerToStack(0, b0));
        TermBatch b1 = pool.getFromStack(1);
        assertNotNull(b1);
        assertNotSame(b0, b1, "had a vacancy in stack 0, but store on stack 1");
        assertNull(pool.offerToStack(1, b1));
        drain(pool);
    }

    @Test void testConcurrentNoCommunication() throws Exception {
        int nRounds = 2_000;
        var pool = new BatchPool<>(TermBatch.class, F, THREAD_BUCKETS, 100);
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(THREAD_BUCKETS, thread -> {
                List<TermBatch> list = new ArrayList<>();
                int expected = -1;
                for (int round = 0; round < nRounds; round++) {
                    for (int i = 0; i < BatchPool.LOCAL_CAP; i++) {
                        TermBatch b = pool.getFromStack(thread);
                        if (b == null) break;
                        list.add(b);
                    }
                    assertFalse(list.isEmpty(), "pool not primed");
                    if (expected == -1) expected = list.size();
                    else assertEquals(expected, list.size(), "batches got lost between rounds");
                    assertNoDuplicates(list);

                    for (TermBatch b : list)
                        assertNull(pool.offerToStack(thread, b), "rejected despite local vacancy");
                    list.clear();
                }
            });
        }
        drain(pool);
    }

    @Test void testControlledConcurrent() throws Exception {
        int nThreads = Math.max(THREAD_BUCKETS, getRuntime().availableProcessors())*2;
        int rounds = 8, nOffers = 128, nGets = 216, sharedCap = rounds*nGets*nThreads;
        List<List<TermBatch>> batchLists = new ArrayList<>();
        for (int t = 0; t < nThreads; t++)
            batchLists.add(new ArrayList<>(nGets));
        var pool = new BatchPool<>(TermBatch.class, F, THREAD_BUCKETS, sharedCap);
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(nThreads, thread -> {
                var list = batchLists.get(thread);
                for (int round = 0; round < rounds; round++) {
                    for (int i = 0; i < nOffers; i++) {
                        TermBatch rejected = pool.offer(F.create());
                        if (rejected != null)
                            rejected.markPooled().markGarbage();
                        if ((i&63) == 0)
                            PoolCleaner.INSTANCE.sync();
                    }
                    for (int i = 0; i < nGets; i++) {
                        TermBatch b = pool.get();
                        assertNotNull(b);
                        list.add(b);
                        if ((i&63) == 0)
                            PoolCleaner.INSTANCE.sync();
                    }
                }
                assertNoDuplicates(list);
            });
        }
        assertNoCrossThreadDuplicates(batchLists);
        drain(pool);
    }

    private static void assertNoDuplicates(List<TermBatch> list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            TermBatch outer = list.get(i);
            assertNotNull(outer);
            outer.requireUnpooled();
            for (int j = i+1; j < n; j++) {
                if (list.get(j) == outer)
                    fail("This thread got the same batch more than once");
            }
        }
    }

    private static void assertNoCrossThreadDuplicates(List<List<TermBatch>> batchLists) {
        int nThreads = batchLists.size();
        for (int outerThread = 0; outerThread < nThreads; outerThread++) {
            var outerList = batchLists.get(outerThread);
            for (int innerThread = outerThread+1; innerThread < nThreads; innerThread++) {
                var innerList = batchLists.get(innerThread);
                for (TermBatch outer : outerList) {
                    for (TermBatch inner : innerList) {
                        if (inner == outer)
                            fail("batch unpooled twice");
                    }
                }
            }
            for (TermBatch b : outerList) {
                assertNotNull(b);
                b.requireUnpooled();
                b.markPooled().markGarbage();
            }
        }
    }

    @Test void testConcurrent() throws Exception {
        int nThreads = Math.max(THREAD_BUCKETS, getRuntime().availableProcessors())*2;
        int rounds = 16, nBatches = 150, sharedCapacity = nThreads*nBatches/2;
        var pool = new BatchPool<>(TermBatch.class, F, THREAD_BUCKETS, sharedCapacity);
        List<List<TermBatch>> lists = new ArrayList<>();
        for (int i = 0; i < nThreads; i++)
            lists.add(new ArrayList<>(nBatches));
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(nThreads, thread -> {
                var list = lists.get(thread);
                for (int round = 0; round < rounds; round++) {
                    for (int j = 0; j < nBatches; j++) {
                        var rejected = pool.offer(F.create());
                        if (rejected != null)
                            rejected.markPooled().markGarbage();
                    }
                    PoolCleaner.INSTANCE.sync();
                    for (int i = 0; i < nBatches; i++) {
                        TermBatch b = pool.get();
                        assertNotNull(b); // get() should call F.get()
                        list.add(b);
                    }
                }
                assertNoDuplicates(list);
            });
        }
        assertNoCrossThreadDuplicates(lists);
        drain(pool);
    }

}