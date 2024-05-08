package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.cleanIntsAtLeast;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.recycleIntsAndGetEmpty;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.max;

public abstract sealed class WeakCrossSourceDedup<B extends Batch<B>> extends Dedup<B, WeakCrossSourceDedup<B>> {
    private RowBucket<B, ?> table;
    private int[] hashesAndSources; // [hash for rows[0], sources for [0], hash for [1], ...]
    private byte[] bucketInsertion; // values are 0 <= bucketInsertion[i] < 8
    private Bytes bucketInsertionHandle;
    private short buckets, bucketWidth;


    private static final ReentrantLock POOLS_LOCK = new ReentrantLock();
    @SuppressWarnings("rawtypes") private static LIFOPool[] POOLS = new LIFOPool[10];

    static {
        pool(TermBatchType.TERM);
        pool(CompressedBatchType.COMPRESSED);
        pool(StoreBatchType.STORE);
    }

    @SuppressWarnings("unchecked")
    private static <B extends Batch<B>> LIFOPool<RowBucket<B, ?>> pool(BatchType<B> bt) {
        if (bt.id >= POOLS.length || POOLS[bt.id] == null)
            makePool(bt);
        return (LIFOPool<RowBucket<B, ?>>)POOLS[bt.id];
    }

    private static <B extends Batch<B>> void makePool(BatchType<B> bt) {
        POOLS_LOCK.lock();
        try {
            if (bt.id >= POOLS.length)
                POOLS = Arrays.copyOf(POOLS, POOLS.length<<1);
            short rowsCapacity = bt.preferredRowsPerBatch(1);
            int bytesPerBucket = bt.bucketBytesCost(rowsCapacity, 1);
            @SuppressWarnings("unchecked") var cls = (Class<RowBucket<B,?>>)(Object)RowBucket.class;
            int capacity = Alloc.THREADS * 64;
            LIFOPool<RowBucket<B, ?>> p = new LIFOPool<>(cls,
                    "WeakCrossSourceDedup<" + bt + ">.POOL",
                    capacity, bytesPerBucket);
            POOLS[bt.id] = p;
            for (int i = 0, n = capacity/2; i < n; i++) {
                var bucket = bt.createBucket(rowsCapacity, 1).takeOwnership(RECYCLED);
                if (p.offer(bucket) == null) bucket.setPool(p);
                else                         bucket.recycle(RECYCLED); // should never happen
            }
        } finally {
            POOLS_LOCK.unlock();
        }
    }

    private WeakCrossSourceDedup(BatchType<B> batchType, int cols) {
        super(batchType, cols);
        if (cols < 0)
            throw new IllegalArgumentException();
        int rowsCapacity = max(batchType.preferredRowsPerBatch(cols), 1);
        LIFOPool<RowBucket<B, ?>> pool = pool(batchType);
        RowBucket<B, ?> table = pool.get();
        if (table == null)
            table = bt.createBucket(rowsCapacity, cols).takeOwnership(this).setPool(pool);
        else
            table.transferOwnership(RECYCLED, this).clear(rowsCapacity, cols);
        this.table = table;
        this.table.maximizeCapacity();
        clear0(table.capacity());
    }

    @Override public @Nullable WeakCrossSourceDedup<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        table                 = table.recycle(this);
        hashesAndSources      = recycleIntsAndGetEmpty(hashesAndSources);
        bucketInsertionHandle = bucketInsertionHandle.recycleAndGetEmpty(this);
        bucketInsertion       = bucketInsertionHandle.arr;
        return null;
    }

    protected static final class Concrete<B extends Batch<B>>
            extends WeakCrossSourceDedup<B>
            implements Orphan<WeakCrossSourceDedup<B>> {
        public Concrete(BatchType<B> batchType, int cols) {
            super(batchType, cols);
        }
        @Override public WeakCrossSourceDedup<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    private void clear0(int rowsCapacity) {
        bucketWidth = (short)(rowsCapacity > 16 ? 4 : 1);
        buckets     = (short)(rowsCapacity/bucketWidth);
        rowsCapacity = buckets*bucketWidth;

        hashesAndSources = cleanIntsAtLeast(rowsCapacity<<1, hashesAndSources);
        bucketInsertionHandle = Bytes.cleanAtLeast(buckets, bucketInsertionHandle, this);
        bucketInsertion = bucketInsertionHandle.arr;
    }

    @Override public void clear(int cols) {
        int rowsCapacity = max(table.batchType().preferredRowsPerBatch(cols), 1);
        table.clear(rowsCapacity, cols);
        table.maximizeCapacity();
        rowsCapacity = table.capacity();
        clear0(rowsCapacity);
    }

    @Override public int capacity() { return table == null ? 0 : table.capacity(); }

    @Override public boolean isWeak() { return true; }

    /**
     * Tests whether there was a previous {@code isDuplicate(r, another)} call with
     * {@code another != source} and {@code r} {@link RowBucket#equals(int, Batch, int)}
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
    @Override public boolean isDuplicate(B batch, int row, int source) {
        requireAlive();
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        short bucket = (short)((hash&Integer.MAX_VALUE)%buckets), bucketWidth = this.bucketWidth;
        int begin = bucket*bucketWidth, end = begin+bucketWidth, match = -1;
        for (int i = begin; match == -1 && i < end; i++) {
            if (hashesAndSources[i<<1] == hash && table.equals(i, batch, row))
                match = i;
        }
        lock();
        try {
            int sources, sourcesIdx;
            if (match == -1) { // add row to the table, consuming an insertion point
                sources = 0;
                match = begin + bucketInsertion[bucket]; //get insertion index
                bucketInsertion[bucket] = (byte) ((bucketInsertion[bucket] + 1) % bucketWidth);
                table.set(match, batch, row);
                int hashIdx = match << 1;
                sourcesIdx = hashIdx+1;
                hashesAndSources[hashIdx] = hash;
            } else if (!table.equals(match, batch, row)) {
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
        } finally { unlock(); }
    }

    @Override public boolean contains(B batch, int row) {
        requireAlive();
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        int i = ((hash&Integer.MAX_VALUE)%buckets)*bucketWidth, e = i+bucketWidth;
        while (i < e && (hashesAndSources[i<<1] != hash || !table.equals(i, batch, row)))
            ++i;
        return i < e;
    }

    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        requireAlive();
        for (B b : table)
            if (b != null) consumer.accept(b);
    }
}
