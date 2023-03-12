package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public sealed interface BatchGetter {
    <T> List<T> getList(BIt<T> it);
    <T> Batch<T> getBatch(BIt<T> it);
    boolean isTimingReliable();

    static List<BatchGetter> all() {
        return List.of(new NativeBatchGetter(), new ListBatchGetter(), new ItBatchGetter());
    }

    final class NativeBatchGetter implements BatchGetter {
        @Override public boolean isTimingReliable() { return true; }
        @Override public <T> List<T> getList(BIt<T> it) {
            var b = it.nextBatch();
            return Arrays.asList(b.array).subList(0, b.size);
        }
        @Override public <T> Batch<T> getBatch(BIt<T> it) { return it.nextBatch(); }
        @Override public String toString() { return "NativeBatchGetter"; }
    }

    final class ListBatchGetter implements BatchGetter {
        @Override public boolean isTimingReliable() { return true; }
        @Override public <T> List<T> getList(BIt<T> it) {
            var list = new ArrayList<T>();
            it.nextBatch(list);
            return list;
        }
        @Override public <T> Batch<T> getBatch(BIt<T> it) {
            var list = new ArrayList<T>();
            it.nextBatch(list);
            //noinspection unchecked
            T[] empty = (T[]) Array.newInstance(it.rowType().rowClass, 0);
            return new Batch<>(list.toArray(empty), list.size());
        }
        @Override public String toString() { return "ListBatchGetter"; }
    }

    final class ItBatchGetter implements BatchGetter {
        private boolean shouldFetch(int size, long start, BIt<?> it) {
            long elapsed = System.nanoTime()-start;
            boolean ready = size > 0
                    && (size >= it.maxBatch()
                        || (elapsed >= it.minWait(NANOSECONDS) && size >= it.minBatch())
                        || (elapsed >= it.maxWait(NANOSECONDS)));
            return !ready && it.hasNext();
        }

        @Override public boolean isTimingReliable() { return false; }
        @Override public <T> List<T> getList(BIt<T> it) {
            var list = new ArrayList<T>();
            long start = System.nanoTime();
            while (shouldFetch(list.size(), start, it))
                list.add(it.next());
            return list;
        }
        @Override public <T> Batch<T> getBatch(BIt<T> it) {
            Batch<T> batch = new Batch<>(it.rowType().rowClass, 10);
            long start = System.nanoTime();
            while (shouldFetch(batch.size, start, it))
                batch.add(it.next());
            return batch;
        }
        @Override public String toString() { return "ItBatchGetter"; }
    }
}
