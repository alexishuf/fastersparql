package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItGenerator;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static org.junit.jupiter.api.Assertions.*;

public class ScatterBItTest extends AbstractBItTest {
    private static final int THREADS = Runtime.getRuntime().availableProcessors();

    @Override protected List<? extends Scenario> scenarios() {
        return baseScenarios();
    }

    @Override protected void run(Scenario s) {
        run(s, false);
        run(s, true);
    }
    private void run(Scenario s, boolean lateStart) {
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            for (var generator : BItGenerator.GENERATORS) {
                BIt<TermBatch> upstream = generator.asBIt(s.error(), s.expectedInts());
                var scatter = new ScatterBIt<>(upstream, THREADS, s.maxBatch());
                if (!lateStart)
                    scatter.start();
                tasks.repeat(THREADS, thread-> {
                    try (var q = scatter.consumer(thread)) {
                        s.drainer().drainOrdered(q, s.expectedInts(), s.error());
                    }
                });
                if (lateStart) {
                    Thread.startVirtualThread(() -> {
                        Thread.yield();
                        scatter.start();
                    });
                }
                assertDoesNotThrow(tasks::awaitAndReset);
            }
        } catch (Exception e) { fail(e); }
    }

    @Test
    void testSingleConsumerCancelIsIgnored() {
        var semaphore = new Semaphore(0);
        try (var consumerExec = Executors.newFixedThreadPool(2)) {
            for (var len : List.of(1, 2, 8, 8192)) {
                int[] expected = IntStream.range(0, len).toArray();
                for (var generator : List.of(BItGenerator.IT_GEN, BItGenerator.CB_GEN)) {
                    var upstream = generator.asBIt(expected);
                    var scatter = new ScatterBIt<>(upstream, 2, TERM.preferredTermsPerBatch());
                    scatter.start();
                    Future<?> first = consumerExec.submit(() -> {
                        try (var q = scatter.consumer(0)) {
                            semaphore.acquireUninterruptibly();
                            BItDrainer.RECYCLING.drainOrdered(q, expected, null);
                        }
                    });
                    Future<?> second = consumerExec.submit(() -> {
                        try (var q = scatter.consumer(1)) {
                            semaphore.acquireUninterruptibly();
                            q.tryCancel();
                        }
                    });
                    semaphore.release(2);
                    assertDoesNotThrow(() -> second.get());
                    assertDoesNotThrow(() -> first.get());
                }
            }
        }
    }

    @Test
    void testBothConsumersCancel() {
        var semaphore = new Semaphore(0);
        try (var consumerExec = Executors.newFixedThreadPool(2)) {
            for (var len : List.of(1, 2, 8, 8192)) {
                int[] expected = IntStream.range(0, len).toArray();
                for (var generator : List.of(BItGenerator.IT_GEN, BItGenerator.CB_GEN)) {
                    ThreadJournal.resetJournals();
                    var upstream = generator.asBIt(expected);
                    var scatter = new ScatterBIt<>(upstream, 2, TERM.preferredTermsPerBatch());
                    scatter.start();
                    IntConsumer consumer = thread -> {
                        int[] actual = new int[expected.length];
                        int actualSize = 0;
                        try (var g = new Guard.ItGuard<>(this, scatter.consumer(thread))) {
                            semaphore.acquireUninterruptibly();
                            TermBatch b = g.nextBatch();
                            if (!g.it.tryCancel()) {
                                assertTrue(g.it.state().isTerminated());
                                assertNotEquals(upstream.state(), BIt.State.ACTIVE);
                            }
                            assertNotNull(b);
                            for (; b != null; b = g.nextBatch()) {
                                for (TermBatch n = b; n != null; n = n.next) {
                                    for (int r = 0, rows = n.rows; r < rows; r++)
                                        actual[actualSize++] = IntsBatch.parse(n.get(r, 0));
                                }
                            }
                        } catch (BItReadCancelledException ignored) {
                        } catch (Throwable t) {
                            fail(t);
                        }
                        assertTrue(actualSize <= expected.length, "actualSize > expected.length");
                        assertEquals(-1, Arrays.mismatch(actual,   0, actualSize,
                                                         expected, 0, actualSize));
                    };
                    Future<?> first = consumerExec.submit(() -> consumer.accept(0));
                    Future<?> second = consumerExec.submit(() -> consumer.accept(1));
                    semaphore.release(2);
                    assertDoesNotThrow(() -> second.get(10, TimeUnit.SECONDS));
                    assertDoesNotThrow(() -> first.get(10, TimeUnit.SECONDS));
                }
            }
        } catch (Throwable t) {
            ThreadJournal.dumpAndReset(System.out, 120);
        }
    }
}
