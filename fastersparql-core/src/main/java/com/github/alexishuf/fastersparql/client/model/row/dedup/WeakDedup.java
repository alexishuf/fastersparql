package com.github.alexishuf.fastersparql.client.model.row.dedup;

import com.github.alexishuf.fastersparql.client.model.row.RowType;

import java.lang.reflect.Array;

public final class WeakDedup<R> extends Dedup<R> {
    private final RowType<R, ?> rt;
    /** Rows in the set. This works as a list of buckets of size 1. */
    private final R[] rows;
    /** If bit {@code hash(r) & bitsetMask} is set, r MAY be present, else it certainly is not. */
    private final int[] bitset;
    /** {@code hash & bitsetMask} yields a <strong>bit</strong> index in {@code bitset} */
    private final int bitsetMask;

    public WeakDedup(RowType<R, ?> rowType, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException();
        this.rt = rowType;
        //noinspection unchecked
        rows = (R[]) Array.newInstance(rowType.rowClass, capacity);
        // since we do not store hashes, we can use the capacity*32 bits to create a bitset
        // such bitset allows faster non-membership detection than calling rt.equalsSameVars()
        bitset = new int[capacity];
        bitsetMask = (capacity<<5)-1;
    }

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
        int hash = rt.hash(row), bucket = (hash & 0x7fffffff) % rows.length;
        int bitIdx = hash & bitsetMask, wordIdx = bitIdx >> 5, bit = 1 << bitIdx;
        if ((bitset[wordIdx] & bit) != 0) { //row may be in set, we must compare
            if (rt.equalsSameVars(row, rows[bucket]))
                return true;
        } else {
            bitset[wordIdx] |= bit;
        }
        rows[bucket] = row;
        return false;
    }
}
