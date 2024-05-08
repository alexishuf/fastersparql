package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.IntList;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

import java.util.*;
import java.util.function.Consumer;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsOrdered;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsUnordered;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public sealed abstract class BItDrainer {

    public static final BItDrainer NOT_RECYCLING   = new NotRecycling();
    public static final BItDrainer RECYCLING       = new Recycling();
    public static final BItDrainer RECYCLING_EAGER = new RecyclingFirstEager();

    public static final List<BItDrainer> ALL = List.of(NOT_RECYCLING, RECYCLING, RECYCLING_EAGER);

    @Override public String toString() {
        return getClass().getSimpleName();
    }

    public <B extends Batch<B>> void drainOrdered(BIt<B> it, Collection<List<Term>> expected, Throwable expectedError) {
        drain(it, expected, expectedError, true);
    }
    public <B extends Batch<B>> void drainUnordered(BIt<B> it, Collection<List<Term>> expected, Throwable expectedError) {
        drain(it, expected, expectedError, false);
    }
    public void drainOrdered(BIt<TermBatch> it, int[] expected, Throwable expectedError) {
        drain(it, expected, expectedError, true);
    }
    public void drainUnordered(BIt<TermBatch> it, int[] expected, Throwable expectedError) {
        drain(it, expected, expectedError, false);
    }
    public <B extends Batch<B>> void drainOrdered(Orphan<? extends Emitter<B, ?>> em,
                                                  Collection<List<Term>> expected,
                                                  Throwable expectedError) {
        drain(new EmitterBIt<>(em), expected, expectedError, true);
    }

    private <B extends Batch<B>> void
    drain(BIt<B> it, Collection<List<Term>> expected, Throwable expectedError, boolean ordered) {
        List<List<Term>> list = new ArrayList<>();
        Throwable error = null;
        try {
            drain(it, b -> {
                for (var node = b; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; r++)
                        list.add(node.asList(r));
                }
            });
        } catch (Throwable t) {
            error = t;
        }
        if (error != null) {
            if (expectedError == null) {
                fail(error);
            } else if (error instanceof BItReadFailedException e) {
                assertEquals(expectedError, e.rootCause());
            } else if (!error.equals(expectedError) && error.getCause() != null) {
                assertEquals(expectedError, error.getCause());
            }
        } else if (expectedError != null) {
            fail("Expected "+expectedError.getClass().getSimpleName()+" to be thrown");
        }
        if (ordered) {
            assertEquals(expected, list);
        } else if (expectedError == null) {
            assertEquals(new HashSet<>(expected), new HashSet<>(list));
            Map<List<Term>, Integer> acCount = new HashMap<>(), exCount = new HashMap<>();
            for (List<Term> row : list)
                acCount.put(row, acCount.getOrDefault(row, 0)+1);
            for (List<Term> row : expected)
                exCount.put(row, exCount.getOrDefault(row, 0)+1);
            assertEquals(exCount, acCount);
        } else {
            List<List<Term>> unexpected = list.stream().filter(i -> !expected.contains(i)).toList();
            assertEquals(List.of(), unexpected, "Unexpected items found");
        }
    }

    private static final class IntsParsingConsumer implements Consumer<TermBatch> {
        IntList ints;

        public IntsParsingConsumer(int sizeHint) {
            this.ints = new IntList(Math.min(sizeHint, 1024));
        }

        @Override public void accept(TermBatch batch) {
            for (var node = batch; node != null; node = node.next) {
                for (int r = 0; r < node.rows; r++)
                    ints.add(IntsBatch.parse(node.get(r, 0)));
            }
        }
    }

    private void drain(BIt<TermBatch> it, int[] expected, Throwable expectedError, boolean ordered) {
        var c = new IntsParsingConsumer(expected.length);
        Throwable error = null;
        try {
            drain(it, c);
        } catch (Throwable t) {
            error = t;
        }
        boolean expectsError = expectedError != null;
        if (error != null) {
            if (expectedError == null) {
                fail(error);
            } else if (error instanceof BItReadFailedException e) {
                assertEquals(expectedError, e.rootCause());
            } else if (!error.equals(expectedError) && error.getCause() != null) {
                assertEquals(expectedError, error.getCause());
            }
        } else if (expectsError) {
            fail("Expected "+expectedError.getClass().getSimpleName()+" to be thrown");
        }
        if (ordered)
            assertEqualsOrdered(expected, c.ints);
        else
            assertEqualsUnordered(expected, c.ints,  false, expectsError,  expectsError);
        c.ints.clear();
    }


    public abstract <B extends Batch<B>> void drain(BIt<B> it, Consumer<B> consumer);

    public IntList drainToInts(BIt<TermBatch> it, int sizeHint) {
        IntsParsingConsumer c = new IntsParsingConsumer(sizeHint);
        drain(it, c);
        return c.ints;
    }

    private static final class NotRecycling extends BItDrainer {
        @Override public <B extends Batch<B>>
        void drain(BIt<B> it, Consumer<B> consumer) {
            try (var g = new Guard.BatchGuard<B>(this)) {
                for (B b; (b = g.set(it.nextBatch(null))) != null;)
                    consumer.accept(b);
            }
        }
    }

    private static final class Recycling extends BItDrainer {
        @Override public <B extends Batch<B>>
        void drain(BIt<B> it, Consumer<B> consumer) {
            try (var g = new Guard.ItGuard<>(this, it)) {
                for (B b; (b = g.nextBatch()) != null;)
                    consumer.accept(b);
            }
        }
    }

    private static final class RecyclingFirstEager extends BItDrainer {
        @Override public <B extends Batch<B>>
        void drain(BIt<B> it, Consumer<B> consumer) {
            try (var g = new Guard.ItGuard<>(this, it.tempEager())) {
                while (g.advance())
                    consumer.accept(g.get());
            }
        }
    }
}
