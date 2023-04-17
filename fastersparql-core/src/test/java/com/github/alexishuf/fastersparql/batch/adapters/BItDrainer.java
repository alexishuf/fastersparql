package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.util.*;
import java.util.function.Consumer;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsOrdered;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsUnordered;
import static java.util.Arrays.copyOf;
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

    private <B extends Batch<B>> void
    drain(BIt<B> it, Collection<List<Term>> expected, Throwable expectedError, boolean ordered) {
        List<List<Term>> list = new ArrayList<>();
        Throwable error = null;
        try {
            drain(it, b -> {
                for (int r = 0; r < b.rows; r++)
                    list.add(b.asList(r));
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
        int[] ints;
        int size;

        public IntsParsingConsumer(int sizeHint) {
            this.ints = new int[sizeHint];
        }

        public int[] cropped() { return size == ints.length ? ints : copyOf(ints, size); }

        @Override public void accept(TermBatch batch) {
            for (int r = 0; r < batch.rows; r++) {
                if (size == ints.length)
                    ints = copyOf(ints, ints.length + Math.max(1, ints.length>>1));
                ints[size++] = IntsBatch.parse(batch.get(r, 0));
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
            assertEqualsOrdered(expected, c.ints, c.size);
        else
            assertEqualsUnordered(expected, c.ints, c.size, false, expectsError,  expectsError);
    }


    public abstract <B extends Batch<B>> void drain(BIt<B> it, Consumer<B> consumer);

    public int[] drainToInts(BIt<TermBatch> it, int sizeHint) {
        IntsParsingConsumer c = new IntsParsingConsumer(sizeHint);
        drain(it, c);
        return c.cropped();
    }

    protected static <B extends Batch<B>> void invalidate(B b) {
        b.clear(b.cols + 1);
        b.beginPut();
        for (int c = 0; c < b.cols; c++) b.putTerm(c, IntsBatch.INVALID_MARKER);
        b.commitPut();
    }

    protected static <B extends Batch<B>> void consumeAndInvalidate(Consumer<B> consumer, B b) {
        consumer.accept(b);
        invalidate(b);
    }

    private static final class NotRecycling extends BItDrainer {
        @Override public <B extends Batch<B>>
        void drain(BIt<B> it, Consumer<B> consumer) {
            for (B b; (b = it.nextBatch(null)) != null;)  consumeAndInvalidate(consumer, b);
        }
    }

    private static final class Recycling extends BItDrainer {
        @Override public <B extends Batch<B>>
        void drain(BIt<B> it, Consumer<B> consumer) {
            for (B b = null; (b = it.nextBatch(b)) != null;) consumeAndInvalidate(consumer, b);
        }

    }

    private static final class RecyclingFirstEager extends BItDrainer {
        @Override public <B extends Batch<B>>
        void drain(BIt<B> it, Consumer<B> consumer) {
            it.tempEager();
            for (B b = null; (b = it.nextBatch(b)) != null;) consumeAndInvalidate(consumer, b);
        }
    }
}
