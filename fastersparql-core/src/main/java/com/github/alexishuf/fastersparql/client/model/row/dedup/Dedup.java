package com.github.alexishuf.fastersparql.client.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.client.util.Merger;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class Dedup<R> {
    /**
     * Whether the given row, originated from the given source should NOT be considered a duplicate.
     *
     * @param source index of the source within the set of sources. This will be ignored unless
     *               the implementation is doing cross-source deduplication, where only a row
     *               previously output by <strong>another</strong> source will be considered
     *               duplicate.
     * @return {@code true} iff {@code row} is a duplicate
     */
    public abstract boolean isDuplicate(R row, int source);


    /**
     * Remove all items from {@code b} for which {@link Dedup#isDuplicate(Object, int)} returns false
     * and apply {@code projector} to the remaining items.
     *
     * @param b the batch to be filtered.
     * @param sourceIdx index of the source that originated this {@link Batch}.
     * @param projector {@code projector.merge(r, null)} will be applied for every
     *                  non-duplicate {@code r} in {@code b}
     */
    public final void filter(Batch<R> b, int sourceIdx, @Nullable Merger<R, ?> projector) {
        if (projector == null) { //hot branch
            R[] a = b.array;
            int o = 0;
            for (int i = 0, n = b.size; i < n; i++) {
                R row = a[i];
                if (!isDuplicate(row, sourceIdx)) {
                    if (o != i) a[o] = row;
                    ++o;
                }
            }
            b.size = o;
        } else { // cold branch
            projectAndFilter(b, sourceIdx, projector);
        }
    }

    protected final void projectAndFilter(Batch<R> b, int sourceIdx, Merger<R, ?> projector) {
        R[] a = b.array;
        int o = 0;
        for (int i = 0, n = b.size; i < n; i++) {
            R row = projector.merge(a[i], null);
            if (!isDuplicate(row, sourceIdx)) {
                if (o != i) a[o] = row;
                ++o;
            }
        }
        b.size = o;
    }
}
