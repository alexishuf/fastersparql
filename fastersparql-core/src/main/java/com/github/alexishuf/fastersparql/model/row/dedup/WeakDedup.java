package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;

import java.lang.reflect.Array;

import static java.lang.Integer.numberOfLeadingZeros;

public final class WeakDedup<R> extends Dedup<R> {
    private final RowType<R> rt;
    /** Rows in the set. This works as a list of buckets of size 1. */
    private final R[] rows;
    /** Value such that {@code hash & mask} yields the bucket index for a given hash value */
    private final int mask;
    /** If bit {@code hash(r) & bitsetMask} is set, r MAY be present, else it certainly is not. */
    private final int[] bitset;
    /** {@code hash & bitsetMask} yields a <strong>bit</strong> index in {@code bitset} */
    private final int bitsetMask;

    public WeakDedup(RowType<R> rowType, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException();
        this.rt = rowType;
        capacity = 1+(mask = capacity < 8 ? 7 : -1 >>> numberOfLeadingZeros(capacity-1));
        //noinspection unchecked
        rows = (R[]) Array.newInstance(rowType.rowClass, capacity+1);
        // since we do not store hashes, we can use the (capacity/2)*32 bits to create a bitset
        // such bitset allows faster non-membership detection than calling rt.equalsSameVars()
        bitset = new int[capacity>>1];
        bitsetMask = (capacity<<4)-1;
    }

    @Override public int capacity() { return rows.length; }

    @Override public boolean isWeak() { return true; }

    /**
     * Tests if there was a previous {@code isDuplicate(r, )} call with {@code r} that is
     * {@link RowType#equalsSameVars(Object, Object)} with {@code row}.
     *
     * <p>For performance reasons, this implementation may return {@code false} when the row
     * is in fact a duplicate. The inverse, returning {@code true} on  non-duplicate row does
     * not occur.</p>
     *
     * @param row the row to test (and store for future calls)
     * @param source ignored
     * @return whether row is a duplicate.
     */
    @Override public boolean isDuplicate(R row, int source) {
        // beware of race conditions / lost updates
        // the writes on bitset[] and row[] are not synchronized and one thread may undo
        // updates done by another. For example, if thread 1 is doing add(r1) and thread 2 is
        // doing add(r2) where r2 and r1 collide in bitset or rows, it may occur that after both
        // exit add() returning true, only the side effects of thread 2 remain. A future call to
        // add(r1) would then wrongly return true. However, this is allowed by the contract
        // (wrong true return). What would not be allowed is a wrong false return which does
        // not occur.
        int hash = rt.hash(row), bucket = hash&mask;
        int bitIdx = hash&bitsetMask, wordIdx = bitIdx >> 5, bit = 1 << bitIdx;
        if ((bitset[wordIdx] & bit) != 0) { //row may be in set, we must compare
            // in addition to bucket, also check bucket+1, since it may have received an evicted row
            if (rt.equalsSameVars(row, rows[bucket]) || rt.equalsSameVars(row, rows[bucket+1]))
                return true;
        } else {
            bitset[wordIdx] |= bit;
        }
        if (rows[bucket] != null && rows[bucket+1] == null)
            rows[bucket+1] = rows[bucket]; // if possible, delay eviction of old row at bucket
        rows[bucket] = row;
        return false;
    }

    /**
     * Check if there is a previous instance of {@code row} at source {@code 0}.
     * Unlike {@code isDuplicate(row, 0)}, this <strong>WILL NOT</strong> store {@code row}
     * in the set.
     */
    @Override public boolean contains(R row) {
        int hash = rt.hash(row), bucket = hash&mask;
        int bitIdx = hash&bitsetMask, wordIdx = bitIdx >> 5, bit = 1 << bitIdx;
        return (bitset[wordIdx] & bit) != 0
                && rt.equalsSameVars(row, rows[bucket]) || rt.equalsSameVars(row, rows[bucket+1]);
    }

    /** Execute {@code consumer.accept(r)} for every row in this set. */
    @Override public <E extends Throwable> void forEach(ThrowingConsumer<R, E> consumer) throws E {
        for (R r : rows)
            if (r != null) consumer.accept(r);
    }
}
