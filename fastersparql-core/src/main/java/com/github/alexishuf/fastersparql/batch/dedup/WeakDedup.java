
package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static com.github.alexishuf.fastersparql.batch.dedup.HashBitset.HASH_MASK;
import static com.github.alexishuf.fastersparql.sparql.DistinctType.compareTo;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;

public abstract sealed class WeakDedup<B extends Batch<B>> extends Dedup<B, WeakDedup<B>> {
    /** Rows in the set. This works as a list of buckets of size 1. */
    private RowBucket<B, ?> rows;
    /** Value such that {@code hash & mask} yields the bucket index for a given hash value */
    private final int capacity;
    /** If bit {@code hash(r) & bitsetMask} is set, r MAY be present, else it certainly is not. */
    private long[] bitset = HashBitset.get();

    private static final BucketPools SMALL_BUCKETS, BIG_BUCKETS;
    static {
        int cap = Alloc.THREADS*64, half = cap/2;
        SMALL_BUCKETS = new BucketPools(BatchType.PREFERRED_BATCH_TERMS,  cap);
        BIG_BUCKETS   = new BucketPools(Short.MAX_VALUE,                 half);
    }

    private static final class BucketPools {
        private final int bucketSize;
        private final int poolCapacity;
        private final LIFOPool<RowBucket<?, ?>>[] pools;
        private final ReentrantLock lock = new ReentrantLock();

        @SuppressWarnings("unchecked") public BucketPools(int bucketSize, int poolCapacity) {
            this.bucketSize   = bucketSize;
            this.poolCapacity = poolCapacity;
            this.pools        = new LIFOPool[10];
        }

        @SuppressWarnings("unchecked")
        private <B extends Batch<B>> LIFOPool<RowBucket<B, ?>> registerBatchType(BatchType<B> bt) {
            lock.lock();
            try {
                var p = (LIFOPool<RowBucket<B, ?>>)(Object)pools[bt.id];
                if (p != null)
                    return p; // concurrent call to registerBatchType()
                var cls = (Class<RowBucket<B,?>>)(Object)RowBucket.class;
                String poolName = poolCapacity >= Short.MAX_VALUE
                                ? "WeakDedup.BIG" : "WeakDedup.SMALL";
                p = new LIFOPool<>(cls, String.format("%s(%s)", poolName, bt),
                                   poolCapacity, bt.bucketBytesCost(bucketSize, 1));
                for (int i = 0, prime = poolCapacity/2; i < prime; i++) {
                    var bucket = bt.createBucket(bucketSize, 1).takeOwnership(RECYCLED);
                    if (p.offer(bucket) == null) bucket.setPool(p);
                    else                         bucket.recycle(RECYCLED); // should never happen
                }
                pools[bt.id] = (LIFOPool<RowBucket<?,?>>)(Object)p;
                return p;
            } finally {
                lock.unlock();
            }
        }

        public <B extends Batch<B>> Orphan<? extends RowBucket<B, ?>>
        createBucket(BatchType<B> bt, int cols) {
            @SuppressWarnings("unchecked") var p = (LIFOPool<RowBucket<B,?>>)(Object)pools[bt.id];
            if (p == null)
                p = registerBatchType(bt);
            RowBucket<B, ?> b = p.get();
            int rowsCapacity = bucketSize/Math.max(1, cols);
            if (b == null) {
                b = bt.createBucket(rowsCapacity, cols).takeOwnership(RECYCLED);
                b.setPool(p);
            } else {
                b.clear(rowsCapacity, cols);
            }
            return b.releaseOwnership(RECYCLED);
        }
    }

    private WeakDedup(BatchType<B> batchType, int cols, DistinctType type) {
        super(batchType, cols);
        rows = (compareTo(type, DistinctType.WEAK) > 0 ? BIG_BUCKETS : SMALL_BUCKETS)
                .createBucket(batchType, cols)
                .takeOwnership(this);
        this.capacity = rows.capacity();
        if (this.capacity > HashBitset.BS_BITS)
            throw new AssertionError("bucket capacity > HashBitset.BS_BITS");
    }

    protected static final class Concrete<B extends Batch<B>> extends WeakDedup<B>
            implements Orphan<WeakDedup<B>> {
        public Concrete(BatchType<B> batchType, int cols, DistinctType type) {
            super(batchType, cols, type);
        }
        @Override public WeakDedup<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable WeakDedup<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        rows = rows.recycle(this);
        bitset = HashBitset.recycle(bitset);
        return null;
    }

    @Override public String journalName() {
        if (journalName == null)
            journalName = "WeakDedup("+capacity+")@"+toHexString(identityHashCode(this));
        return journalName;
    }

    @Override public void clear(int cols) {
        requireAlive();
        Arrays.fill(bitset, 0L);
        if (rows.cols() != cols)
            rows.clear(rows.capacity(), cols);
        // since constructor asserts bitset has more bits than rows.capacity() and bitset
        // is always checked before this.rows, clear() can be skipped if the number of
        // columns is not changed.
    }

    @Override public int capacity() { return capacity; }

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
        requireAlive();
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
        requireAlive();
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
        requireAlive();
        for (B b : rows)
            if (b != null) consumer.accept(b);
    }
}
