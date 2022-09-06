package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public sealed abstract class BatchItDrainer {
    public static List<BatchItDrainer> all() {
        return List.of(byBatch(), byBatchRecycling(), byCollection(), byItem());
    }

    public static BatchItDrainer byBatch() { return new ByBatch(); }
    public static BatchItDrainer byBatchRecycling() { return new ByBatchRecycling(); }
    public static BatchItDrainer byCollection() { return new ByCollection(); }
    public static BatchItDrainer byItem() { return new ByItem(); }

    @Override public String toString() {
        return getClass().getSimpleName();
    }

    public <T> void drainOrdered(BIt<T> it, Collection<T> expected, Throwable expectedError) {
        drain(it, expected, expectedError, true);
    }
    public <T> void drainUnordered(BIt<T> it, Collection<T> expected, Throwable expectedError) {
        drain(it, expected, expectedError, false);
    }

    private <T> void drain(BIt<T> it, Collection<T> expected, Throwable expectedError,
                           boolean ordered) {
        List<T> list = new ArrayList<>();
        Throwable error = null;
        try {
            drainInto(it, list);
        } catch (Throwable t) {
            error = t;
        }
        if (error != null) {
            if (expectedError == null) {
                fail(error);
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
            Map<T, Integer> acCount = new HashMap<>(), exCount = new HashMap<>();
            for (T i : list)
                acCount.put(i, acCount.getOrDefault(i, 0)+1);
            for (T i : expected)
                exCount.put(i, exCount.getOrDefault(i, 0)+1);
            assertEquals(exCount, acCount);
        } else {
            List<T> unexpected = list.stream().filter(i -> !expected.contains(i)).toList();
            assertEquals(List.of(), unexpected, "Unexpected items found");
        }
    }

    protected abstract <T> void drainInto(BIt<T> it, List<T> into);

    private static final class ByBatch extends BatchItDrainer {
        @Override protected <T> void drainInto(BIt<T> it, List<T> list) {
            for (var b = it.nextBatch(); !b.empty(); b = it.nextBatch())
                b.drainTo(list);
        }
    }

    private static final class ByBatchRecycling extends BatchItDrainer {
        @Override protected <T> void drainInto(BIt<T> it, List<T> list) {
            for (var b = it.nextBatch(); !b.empty(); b = it.nextBatch()) {
                b.drainTo(list);
                it.recycle(b);
            }
        }
    }

    private static final class ByCollection extends BatchItDrainer {
        @Override protected <T> void drainInto(BIt<T> it, List<T> list) {
            //noinspection StatementWithEmptyBody
            while (it.nextBatch(list) > 0) { }
        }
    }

    private static final class ByItem extends BatchItDrainer {
        @Override protected <T> void drainInto(BIt<T> it, List<T> list) {
            while (it.hasNext())
                list.add(it.next());
        }
    }
}
