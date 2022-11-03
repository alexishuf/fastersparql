package com.github.alexishuf.fastersparql.client.model.row.dedup;

import com.github.alexishuf.fastersparql.client.model.row.RowType;

import java.lang.reflect.Array;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WeakCrossSourceDedup<R> extends Dedup<R> {
    private final RowType<R, ?> rt;
    private final R[] rows;
    private final int[] hashesAndSources; // [hash for rows[0], sources for [0], hash for [1], ...]
    private final byte[] bucketInsertion; // values are 0 <= bucketInsertion[i] < 8
    private final int bucketMask;
    private final Lock lock = new ReentrantLock();

    public WeakCrossSourceDedup(RowType<R, ?> rowType, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException();
        // round capacity up to the nearest power-of-2
        capacity = 1 + (-1 >>> Integer.numberOfLeadingZeros(Math.max(8, capacity)-1));
        this.rt = rowType;
        int buckets = capacity >> 3;
        this.bucketMask = buckets-1;
        this.bucketInsertion = new byte[buckets]; // each bucket has 8 items
        this.hashesAndSources = new int[capacity<<1];
        //noinspection unchecked
        this.rows = (R[]) Array.newInstance(rowType.rowClass, capacity);
    }


    /**
     * Tests whether there was a previous {@code isDuplicate(r, another)} call with
     * {@code another != source} and {@code r} {@link RowType#equalsSameVars(Object, Object)}
     * {@code row}.
     *
     * <p>For performance reasons, false negatives are allowed. That is, this method may return
     * {@code false} when {@code row} is in fact a duplicate of another row produced by a source
     * other than {@code source}. The inverse (returning {@code true} for a non-duplicate row)
     * does not occur.</p>
     *
     * @param row the row to check
     * @param source index of the source within the set of sources. This will be ignored unless
     *               the implementation is doing cross-source deduplication, where only a row
     *               previously output by <strong>another</strong> source will be considered
     *               duplicate.
     * @return whether {@code row} is a duplicate of a row output by a source other
     *         than {@code source}
     */
    @Override public boolean isDuplicate(R row, int source) {
        int hash = rt.hash(row), bucket = hash & bucketMask;
        int begin = bucket << 3, end = begin+8, match = -1;
        R seenMatch = null;
        for (int i = begin; match == -1 && i < end; i++) {
            if (hashesAndSources[i<<1] == hash && rt.equalsSameVars(seenMatch = rows[i], row))
                match = i;
        }
        lock.lock();
        try {
            int sources, sourcesIdx;
            if (match == -1) { // add row to the table, consuming an insertion point
                sources = 0;
                match = begin + bucketInsertion[bucket]; //get insertion index
                bucketInsertion[bucket] = (byte) ((bucketInsertion[bucket] + 1) & 7);
                rows[match] = row;
                int hashIdx = match << 1;
                sourcesIdx = hashIdx+1;
                hashesAndSources[hashIdx] = hash;
            } else if (seenMatch != rows[match]) {
                // race: row was overwritten in the table before this thread got the lock.
                // Treat rows as non-duplicate. An eventual new call with row will not suffer
                // this race and will add it to the table.
                return false; // treat as not duplicate. subsequent call for same row will not suffer
            } else {
                sources = hashesAndSources[sourcesIdx = (match<<1) + 1];
            }
            int mask = 1 << source;
            boolean duplicate = (sources |= mask) != mask; //mark and test if only source
            hashesAndSources[sourcesIdx] = sources;
            return duplicate;
        } finally { lock.unlock(); }
    }
}
