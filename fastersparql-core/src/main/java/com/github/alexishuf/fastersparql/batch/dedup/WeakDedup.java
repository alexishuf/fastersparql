
package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;

import java.util.Arrays;

import static java.lang.Integer.numberOfLeadingZeros;

public final class WeakDedup<B extends Batch<B>> extends Dedup<B> {
    /** Rows in the set. This works as a list of buckets of size 1. */
    private final RowBucket<B> rows;
    /** Value such that {@code hash & mask} yields the bucket index for a given hash value */
    private final int mask, bitsetWords;
    /** If bit {@code hash(r) & bitsetMask} is set, r MAY be present, else it certainly is not. */
    private int[] bitset;
    /** {@code hash & bitsetMask} yields a <strong>bit</strong> index in {@code bitset} */
    private final int bitsetMask;

    public WeakDedup(BatchType<B> batchType, int capacity, int cols) {
        super(batchType, cols);
        if (capacity <= 0) throw new IllegalArgumentException();
        capacity = 1+(mask = capacity < 8 ? 7 : -1 >>> numberOfLeadingZeros(capacity-1));
        // allocated bucket above capacity to avoid range checking of bucket+1 accesses
        rows = batchType.createBucket(capacity+1, cols);
        // since we do not store hashes, we can use the (capacity/2)*32 bits to create a bitset
        // such bitset allows faster non-membership detection than calling rt.equalsSameVars()
        bitset = new int[bitsetWords = capacity>>1];
        bitsetMask = (capacity<<4)-1;
    }

    @Override public void clear(int cols) {
        bitset = ArrayPool.intsAtLeast(bitsetWords, bitset);
        Arrays.fill(bitset, 0);
        rows.clear(rows.capacity(), cols);
    }

    @Override public void recycleInternals() {
        rows.recycleInternals();
        bitset = ArrayPool.INT.offer(bitset, bitset.length);
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
        if (debug) checkBatchType(batch);
        // beware of race conditions / lost updates
        // the writes on bitset[] and row[] are not synchronized and one thread may undo
        // updates done by another. For example, if thread 1 is doing add(r1) and thread 2 is
        // doing add(r2) where r2 and r1 collide in bitset or rows, it may occur that after both
        // exit add() returning true, only the side effects of thread 2 remain. A future call to
        // add(r1) would then wrongly return true. However, this is allowed by the contract
        // (wrong true return). What would not be allowed is a wrong false return which does
        // not occur.
        int hash = batch.hash(row), bucket = hash&mask;
        int bitIdx = hash&bitsetMask, wordIdx = bitIdx >> 5, bit = 1 << bitIdx;
        if ((bitset[wordIdx] & bit) != 0) { //row may be in set, we must compare
            // in addition to bucket, also check bucket+1, since it may have received an evicted row
            if (rows.equals(bucket, batch, row) || rows.equals(bucket+1, batch, row))
                return true;
        } else {
            bitset[wordIdx] |= bit;
        }
        if (rows.has(bucket) && !rows.has(bucket+1))
            rows.set(bucket+1, bucket); // if possible, delay eviction of old row at bucket
        rows.set(bucket, batch, row);
        return false;
    }

    /**
     * Check if there is a previous instance of {@code row} at source {@code 0}.
     * Unlike {@code isDuplicate(row, 0)}, this <strong>WILL NOT</strong> store {@code row}
     * in the set.
     */
    @Override public boolean contains(B batch, int row) {
        if (debug) checkBatchType(batch);
        int hash = batch.hash(row), bucket = hash&mask;
        int bitIdx = hash&bitsetMask, wordIdx = bitIdx >> 5, bit = 1 << bitIdx;
        return (bitset[wordIdx] & bit) != 0
                && rows.equals(bucket, batch, row) || rows.equals(bucket+1, batch, row);
    }

    /** Execute {@code consumer.accept(r)} for every row in this set. */
    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        for (B b : rows)
            if (b != null) consumer.accept(b);
    }
}
