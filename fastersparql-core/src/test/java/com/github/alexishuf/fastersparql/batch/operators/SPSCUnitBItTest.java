package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.base.CallbackBItTest;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.intsBatch;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class SPSCUnitBItTest extends CallbackBItTest {

    @Override protected CallbackBIt<TermBatch> create(int capacity) {
        return new SPSCUnitBIt<>(TERM, Vars.of("x"));
    }

    private static class QueueScenario extends Scenario {
        private final boolean copy;
        public QueueScenario(Scenario other, boolean copy) {
            super(other);
            this.copy = copy;
        }
    }

    @Override protected List<? extends Scenario> scenarios() {
        List<QueueScenario> list = new ArrayList<>();
        for (Scenario b : baseScenarios()) {
            list.add(new QueueScenario(b, false));
            list.add(new QueueScenario(b, true));

            var singleBatch = new Scenario(b.size(), b.size(), b.size(), b.drainer(), b.error());
            list.add(new QueueScenario(singleBatch, false));
            list.add(new QueueScenario(singleBatch, true));
        }

        return list;
    }

    @Override protected void run(Scenario scenario) {
        QueueScenario s = (QueueScenario) scenario;
        try (var q = new SPSCUnitBIt<>(TERM, Vars.of("x"))) {
            Thread producer = Thread.startVirtualThread(() -> {
                int chunk = s.minBatch(), i = 0;
                while (i < s.size()) {
                    TermBatch b = TERM.create(chunk, 1, 0);
                    int end = Math.min(s.size(), i + chunk);
                    while (i < end)
                        b.putRow(termList(i++));
                    if (s.copy) {
                        q.copy(b);
                        b.clear();
                        b.putRow(termList(666));
                    } else {
                        IntsBatch.offerAndInvalidate(q, b);
                    }
                }
                q.complete(s.error());
            });
            s.drainer().drainOrdered(q, s.expected(), s.error());
            producer.join();
        } catch (InterruptedException e) {
            fail(e);
        }
    }



    @Test void testBlockSecondOffer() {
        try (var it = new SPSCUnitBIt<>(TERM, Vars.of("x"))) {
            TermBatch b1 = intsBatch(1), b2 = intsBatch(2);
            assertNull(it.offer(b1));

            var f2 = new CompletableFuture<TermBatch>();
            Thread.startVirtualThread(() -> f2.complete(it.offer(b2)));
            assertThrows(TimeoutException.class, () ->f2.get(5, MILLISECONDS));

            assertSame(b1, it.nextBatch(null));
            assertTimeout(ofMillis(50), () -> assertNull(f2.get()));
        }
    }

    @Test void testConsumerBlocks() {
        try (var it = new SPSCUnitBIt<>(TERM, Vars.of("x"))) {
            TermBatch b1 = intsBatch(1), b2 = intsBatch(2);

            var f1 = new CompletableFuture<TermBatch>();
            Thread.startVirtualThread(() -> f1.complete(it.nextBatch(null)));
            assertThrows(TimeoutException.class, () -> f1.get(5, MILLISECONDS));

            it.copy(b1);
            b1.clear();
            b1.putRow(termList(23));
            assertTimeout(ofMillis(20), () -> assertEquals(intsBatch(1), f1.get()));

            var f2 = new CompletableFuture<TermBatch>();
            Thread.startVirtualThread(() -> f2.complete(it.nextBatch(b1)));

            TermBatch recycled = it.offer(b2);
            assertTrue(recycled == null || recycled == b1);
            assertTimeout(ofMillis(20), () -> assertSame(b2, f2.get()));
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void testMinBatch(boolean recycle) {
        try (var it = new SPSCUnitBIt<>(TERM, Vars.of("x"))) {
            it.minBatch(2);
            TermBatch b1 = intsBatch(1), b2 = intsBatch(2), ex = intsBatch(1, 2);
            TermBatch rec = recycle ? intsBatch(23) : null;

            CompletableFuture<TermBatch> f1 = new CompletableFuture<>();
            Thread.startVirtualThread(() -> f1.complete(it.nextBatch(rec)));
            assertThrows(TimeoutException.class, () -> f1.get(5, MILLISECONDS));

            if (recycle) {
                assertSame(b1, it.offer(b1)); // will copy bcs b1 < minBatch
                b1.clear(); b1.putRow(termList(23)); // invalidate b1
            } else {
                assertNull(it.offer(b1)); // no previous recycle(), will take b1 ownership
            }
            it.copy(b2);
            b2.clear(); b2.putRow(termList(23)); // invalidate b2

            assertTimeout(ofMillis(20), () -> assertEquals(ex, f1.get()));

            it.offer(ex);
            assertSame(ex, it.nextBatch(null));

            it.complete(null);
            assertNull(it.nextBatch(null));
        }
    }

    @Test void testConsumerStealsDueToBatchMinWait() {
        int minWaitMs = 100;
        try (var it = new SPSCUnitBIt<>(TERM, Vars.of("x"))) {
            it.minBatch(1).minWait(minWaitMs, MILLISECONDS);
            TermBatch b1 = intsBatch(1), b2 = intsBatch(2), b3 = intsBatch(3);

            assertNull(it.offer(b1));

            // minBatch satisfied, but minWait not. consumer must block
            var f1 = new CompletableFuture<TermBatch>();
            Thread.startVirtualThread(() -> f1.complete(it.nextBatch(null)));
            assertThrows(TimeoutException.class, () -> f1.get(5, MILLISECONDS));

            // consumer must wake without producer
            assertTimeout(ofMillis(minWaitMs+10), () -> assertSame(b1, f1.get()));

            // neither minBatch nor minWait satisfied
            var f2 = new CompletableFuture<TermBatch>();
            Thread.startVirtualThread(() -> f2.complete(it.nextBatch(b1)));
            assertThrows(TimeoutException.class, () -> f2.get(5, MILLISECONDS));

            it.copy(b2); // will use offered b1
            b2.clear(); b2.putRow(termList(23));
            // minWait not elapsed, consumer remains parked
            assertThrows(TimeoutException.class, () -> f2.get(5, MILLISECONDS));

            assertSame(b3, it.offer(b3)); // offer will append to filling

            // consumer still park()ed
            assertThrows(TimeoutException.class, () -> f2.get(5, MILLISECONDS));

            // consumer unparked by time
            assertTimeout(ofMillis(minWaitMs+10), () -> assertEquals(intsBatch(2, 3), f2.get()));
        }
    }

}