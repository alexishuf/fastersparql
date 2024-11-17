package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public abstract sealed class UnitBucket extends AbstractOwned<UnitBucket>
        implements RowBucket<UnitBatch, UnitBucket> {

    private UnitBatch[] rows;
    private int cols;

    static int estimateBytes(int rows) {
        return 16+8+16+UnitBatch.BYTES*rows;
    }

    private UnitBucket(int rows, int cols) {
        this.rows = new UnitBatch[rows];
        this.cols = cols;
    }

    public static Orphan<UnitBucket> create(int rows, int cols) {return new Concrete(rows, cols);}

    private static final class Concrete extends UnitBucket implements Orphan<UnitBucket> {
        public Concrete(int rows, int cols) {super(rows, cols);}
        @Override public UnitBucket takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable UnitBucket recycle(Object currentOwner) {
        for (int i = 0; i < rows.length; i++)
            rows[i] = Owned.safeRecycle(rows[i], this);
        return internalMarkGarbage(currentOwner);
    }

    @Override public BatchType<UnitBatch> batchType() {return UnitBatchType.UNIT;}
    @Override public int                   capacity() {return rows.length;}
    @Override public int                       cols() {return cols;}
    @Override public void          maximizeCapacity() {}

    @Override public @This UnitBucket setPool(LIFOPool<RowBucket<UnitBatch, ?>> pool) {
        return this;
    }

    @Override public void grow(int additionalRows) {
        if (additionalRows <= 0)
            return;
        rows = Arrays.copyOf(rows, rows.length+additionalRows);
    }

    @Override public void clear(int rowsCapacity, int cols) {
        this.cols = cols;
        for (int i = 0; i < rows.length; i++)
            rows[i] = Owned.safeRecycle(rows[i], this);
        if (rowsCapacity != rows.length)
            rows = new UnitBatch[rowsCapacity];
    }

    @Override public void dump(MutableRope dest, int row) {
        if (!has(row)) {
            dest.append(DUMP_NULL);
        } else {
            dest.append('[');
            int cols = cols();
            for (int c = 0; c < cols; c++) {
                var term = rows[row].get(0, c);
                if (term == null) dest.append(DUMP_NULL);
                else              dest.append(term);
                dest.append(',').append(' ');
            }
            if (cols > 0) dest.len -= 2;
            dest.append('[');
        }
    }
    private static final byte[] DUMP_NULL = "null".getBytes(StandardCharsets.UTF_8);

    @Override public boolean has(int row) {
        if (row < 0 || row >= rows.length)
            return false;
        var b = rows[row];
        return b != null && b.rows > 0;
    }

    @Override public int hashCode(int row) {
        if (row < 0 || row >= rows.length)
            throw new IndexOutOfBoundsException();
        var b = rows[row];
        return b == null || b.rows == 0 ? 0 : b.hash(0);
    }

    @Override public boolean equals(int row, UnitBatch other, int otherRow) {
        if (row < 0 || row >= rows.length)
            throw new IndexOutOfBoundsException();
        var b = rows[row];
        if (b == null || b.rows == 0)
            return false;
        try {
            return b.equals(0, other, otherRow);
        } catch (IndexOutOfBoundsException ignored) {
            // may happen if b is modified concurrently (WeakCrossSourceDedup+ITERATOR)
            return false;
        }
    }

    @Override public @NonNull Iterator<UnitBatch> iterator() {
        return new It();
    }
    private final class It implements Iterator<UnitBatch> {
        int row = findNext(0);

        private int findNext(int start) {
            int i = start;
            while (i < rows.length && (rows[i] == null || rows[i].rows == 0))
                i++;
            return i;
        }

        @Override public boolean hasNext() {return row < rows.length;}

        @Override public UnitBatch next() {
            UnitBatch b = rows[row];
            row = findNext(row+1);
            return b;
        }
    }

    @Override public void set(int dst, UnitBatch batch, int row) {
        if (batch.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (dst < 0 || dst >= rows.length)
            throw new IndexOutOfBoundsException();
        UnitBatch dstBatch = rows[dst];
        if (dstBatch != null)
            dstBatch.clear();
        else
            rows[dst] = dstBatch = UnitBatch.create(cols).takeOwnership(this);
        dstBatch.putRow(batch, row);
    }

    @Override public void set(int dst, int src) {
        if (src == dst)
            return; // no-op
        if (src < 0 || src >= rows.length || dst < 0 || dst >= rows.length)
            throw new IndexOutOfBoundsException();
        UnitBatch srcBatch = rows[src], dstBatch = rows[dst];
        if (dstBatch != null)
            dstBatch.clear();
        if (srcBatch != null && srcBatch.rows != 0) {
            if (dstBatch == null)
                rows[dst] = dstBatch = UnitBatch.create(cols).takeOwnership(this);
            dstBatch.putRow(srcBatch, 0);
        }
    }

    @Override public void set(int dst, RowBucket<UnitBatch, ?> other, int src) {
        var o = (UnitBucket)other;
        if (o.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (dst < 0 || dst >= rows.length || src < 0 || src >= o.rows.length)
            throw new IndexOutOfBoundsException();
        UnitBatch dstBatch = rows[dst], srcBatch = o.rows[src];
        if (dstBatch != null)
            dstBatch.clear();
        if (srcBatch != null && srcBatch.rows > 0) {
            if (dstBatch == null)
                rows[dst] = dstBatch = UnitBatch.create(cols).takeOwnership(this);
            dstBatch.putRow(srcBatch, 0);
        }
    }

    @Override public void putRow(UnitBatch dst, int srcRow) {
        if (srcRow < 0 || srcRow >= rows.length)
            throw new IndexOutOfBoundsException();
        UnitBatch b = rows[srcRow];
        if (b == null || b.rows == 0)
            return; // no-op
        dst.putRow(b, 0);
    }
}
