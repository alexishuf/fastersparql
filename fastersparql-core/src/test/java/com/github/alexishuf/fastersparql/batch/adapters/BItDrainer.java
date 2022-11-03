package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public sealed abstract class BItDrainer {
    public static List<BItDrainer> all() {
        return List.of(byBatch(), byBatchRecycling(), byBatchRecyclingFirstEager(),
                       byCollection(), byCollectionEager(), byItem(),
                       byEagerItemFirstThenBatchRecycling());
    }

    public static BItDrainer byBatch() { return new ByBatch(); }
    public static BItDrainer byBatchRecycling() { return new ByBatchRecycling(); }
    public static BItDrainer byBatchRecyclingFirstEager() { return new ByBatchRecyclingFirstEager(); }
    public static BItDrainer byCollection() { return new ByCollection(); }
    public static BItDrainer byCollectionEager() { return new ByCollectionEager(); }
    public static BItDrainer byItem() { return new ByItem(); }
    public static BItDrainer byEagerItemFirstThenBatchRecycling() { return new ByEagerItemFirstThenBatchRecycling(); }

    @Override public String toString() {
        return getClass().getSimpleName();
    }

    public <T> void drainOrdered(BIt<T> it, Collection<T> expected, Throwable expectedError) {
        drain(it, expected, expectedError, true);
    }
    public <T> void drainUnordered(BIt<T> it, Collection<T> expected, Throwable expectedError) {
        drain(it, expected, expectedError, false);
    }

    public <T> List<T> toList(BIt<T> it) {
        List<T> list = new ArrayList<>();
        drainTo(it, list);
        return list;
    }

    private <T> void drain(BIt<T> it, Collection<T> expected, Throwable expectedError,
                           boolean ordered) {
        List<T> list=  new ArrayList<>();
        Throwable error = null;
        try {
            drainTo(it, list);
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

    protected abstract <T> void drainTo(BIt<T> it, List<T> into);

    private static final class ByBatch extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> list) {
            for (var b = it.nextBatch(); !b.empty(); b = it.nextBatch())
                b.drainTo(list);
        }
    }

    private static final class ByBatchRecycling extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> list) {
            for (var b = it.nextBatch(); !b.empty(); b = it.nextBatch()) {
                b.drainTo(list);
                it.recycle(b);
            }
        }
    }

    private static final class ByBatchRecyclingFirstEager extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> list) {
            it.tempEager();
            for (var b = it.nextBatch(); !b.empty(); b = it.nextBatch()) {
                b.drainTo(list);
                it.recycle(b);
            }
        }
    }

    private static final class ByCollection extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> list) {
            //noinspection StatementWithEmptyBody
            while (it.nextBatch(list) > 0) { }
        }
    }

    private static final class ByCollectionEager extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> list) {
            //noinspection StatementWithEmptyBody
            while (it.tempEager().nextBatch(list) > 0) { }
        }
    }

    private static final class ByItem extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> list) {
            while (it.hasNext())
                list.add(it.next());
        }
    }

    private static final class ByEagerItemFirstThenBatchRecycling extends BItDrainer {
        @Override protected <T> void drainTo(BIt<T> it, List<T> into) {
            if (it.tempEager().hasNext())
                into.add(it.next());
            for (var b = it.nextBatch(); b.size > 0; b = it.nextBatch(b))
                b.drainTo(into);
        }
    }
}
