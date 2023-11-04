
package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.dedup.HashBitset.HASH_MASK;

public final class WeakDedup<B extends Batch<B>> extends Dedup<B> {
    /** Rows in the set. This works as a list of buckets of size 1. */
    private final RowBucket<B> rows;
    /** Value such that {@code hash & mask} yields the bucket index for a given hash value */
    private final int capacity;
    /** If bit {@code hash(r) & bitsetMask} is set, r MAY be present, else it certainly is not. */
    private long[] bitset = HashBitset.get();

    public WeakDedup(BatchType<B> batchType, int cols) {
        super(batchType, cols);
        int rowsCapacity = batchType.preferredTermsPerBatch()/cols;
        this.rows = batchType.createBucket(rowsCapacity, cols);
        this.rows.maximizeCapacity();
        this.capacity = this.rows.capacity();
    }

    @Override public void clear(int cols) {
        Arrays.fill(bitset, 0L);
        rows.clear(rows.capacity(), cols);
    }

    @Override public void recycleInternals() {
        rows.recycleInternals();
        bitset = HashBitset.recycle(bitset);
    }

    @Override public int capacity() { return rows.capacity(); }

    @Override public boolean isWeak() { return true; }

    /**
     * Tests if there was a previous {@code isDuplicate(r, )} call with {@code r} that is
     * {@link RowBucket#equals(int, Batch, int)} with {@code row}.
     *
     * <p>For performance reasons, this implementation may return {@code false} when the row
     * is in fact a duplicate. The inverse, returning {@code true} on  non-duplicate row does
     * not occur.</p>
     *
     * @param row the row to test (and store for future calls)
     * @param source ignored
     * @return whether row is a duplicate.
     */
    @Override public boolean isDuplicate(B batch, int row, int source) {
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        int bucket = (hash&Integer.MAX_VALUE)%capacity, wordIdx = (hash&HASH_MASK)>>6;
        int nBucket = (bucket+1)%capacity;
        long bit = 1L << hash;
        lock();
        try {
            if ((bitset[wordIdx] & bit) != 0) { //row may be in set, we must compare
                // bucket+1 may have received an evicted row
                if (rows.equals(bucket, batch, row) || rows.equals(nBucket, batch, row))
                    return true;
            } else {
                bitset[wordIdx] |= bit;
            }
            if (rows.has(bucket) && !rows.has(nBucket))
                rows.set(nBucket, bucket); // if possible, delay eviction of old row at bucket
            rows.set(bucket, batch, row);
        } finally { unlock(); }
        return false;
    }

    /**
     * Check if there is a previous instance of {@code row} at source {@code 0}.
     * Unlike {@code isDuplicate(row, 0)}, this <strong>WILL NOT</strong> store {@code row}
     * in the set.
     */
    @Override public boolean contains(B batch, int row) {
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        if ((bitset[(hash&HASH_MASK) >> 6] & (1L << hash)) == 0)
            return false;
        int bucket = (hash&Integer.MAX_VALUE)%capacity;
        int nBucket = (bucket+1)%capacity;
        return rows.equals(bucket, batch, row) || rows.equals(nBucket, batch, row);
    }

    /** Execute {@code consumer.accept(r)} for every row in this set. */
    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        for (B b : rows)
            if (b != null) consumer.accept(b);
    }
}
