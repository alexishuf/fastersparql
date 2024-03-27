
package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.concurrent.PoolCleaner;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.FSProperties.reducedBatches;
import static com.github.alexishuf.fastersparql.batch.dedup.HashBitset.HASH_MASK;
import static java.lang.Runtime.getRuntime;

public final class WeakDedup<B extends Batch<B>> extends Dedup<B> {
    /** Rows in the set. This works as a list of buckets of size 1. */
    private final RowBucket<B> rows;
    /** Value such that {@code hash & mask} yields the bucket index for a given hash value */
    private final int capacity;
    /** If bit {@code hash(r) & bitsetMask} is set, r MAY be present, else it certainly is not. */
    private long[] bitset = HashBitset.get();
    private final boolean big;
    private boolean recycled;

    private static final int THREADS = getRuntime().availableProcessors();
    private static final int PRIMED_COUNT = 2*THREADS;
    private static final int REDUCED_POOL_CAPACITY = 16*THREADS;
    @SuppressWarnings("unchecked") private static LIFOPool<RowBucket<?>>[] POOLS = new LIFOPool[10];

    @SuppressWarnings("unchecked")
    private static <B extends Batch<B>> LIFOPool<RowBucket<B>> bucketPool(BatchType<B> bt) {
        int id = bt.id;
        if (POOLS[id] == null)
            registerBatchType(bt);
        return (LIFOPool<RowBucket<B>>)(Object)POOLS[id];
    }

    @SuppressWarnings("unchecked")
    public static <B extends Batch<B>> void registerBatchType(BatchType<B> bt) {
        int id = bt.id;
        var pools = POOLS;
        if (id > pools.length) // grow POOLS, if necessary
            POOLS = pools = Arrays.copyOf(pools, pools.length << 1);
        if (pools[id] == null) { // only create pool once
            LIFOPool<RowBucket<B>> pool;
            pool = new LIFOPool<>((Class<RowBucket<B>>)(Object)RowBucket.class,
                                  REDUCED_POOL_CAPACITY);
            pools[id] = (LIFOPool<RowBucket<?>>)(Object)pool;
            PoolCleaner.INSTANCE.monitor(pool); // clear refs of buckets removed from pool
            // prime pool
            int capacity = Math.min(Short.MAX_VALUE, bt.preferredTermsPerBatch()*reducedBatches());
            for (int i = 0; i < PRIMED_COUNT; i++)
                bt.createBucket(capacity, 1).poolInto(pool);
        }
    }

    public WeakDedup(BatchType<B> batchType, int cols, DistinctType type) {
        super(batchType, cols);
        int rowsCapacity = batchType.preferredRowsPerBatch(cols);
        RowBucket<B> rows = null;
        this.big = DistinctType.WEAK.compareTo(type) < 0;
        if (this.big) {
            rowsCapacity = Math.min(Short.MAX_VALUE/cols, rowsCapacity*reducedBatches());
            if ((rows = bucketPool(batchType).get()) != null) {
                rows.unmarkPooled();
                rows.clear(rowsCapacity, cols);
            }
        }
        if (rows == null)
            rows = batchType.createBucket(rowsCapacity, cols);
        rows.maximizeCapacity();
        this.rows     = rows;
        this.capacity = rows.capacity();
        if (this.capacity > HashBitset.BS_BITS)
            throw new AssertionError("bucket capacity > HashBitset.BS_BITS");
    }

    @Override public String toString() {
        return String.format("%s(%d)@%x", getClass().getSimpleName(), rows.capacity(),
                             System.identityHashCode(this));
    }

    @Override public void clear(int cols) {
        if (recycled)
            throw new InvalidSparqlException("Recycled");
        Arrays.fill(bitset, 0L);
        if (rows.cols() != cols)
            rows.clear(rows.capacity(), cols);
        // since constructor asserts bitset has more bits than rows.capacity() and bitset
        // is always checked before this.rows, clear() can be skipped if the number of
        // columns is not changed.
    }

    @Override public void recycleInternals() {
        if (recycled)
            throw new IllegalStateException("Already recycled");
        recycled = true;
        if (!big || !rows.poolInto(bucketPool(bt)))
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
        if (recycled)
            throw new IllegalStateException("Recycled");
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
        if (recycled)
            throw new InvalidSparqlException("Recycled");
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
        if (recycled)
            throw new InvalidSparqlException("Recycled");
        for (B b : rows)
            if (b != null) consumer.accept(b);
    }
}
