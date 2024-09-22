package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.cleanLongsAtLeast;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.recycleLongs;

public abstract sealed class CABucket
        extends AbstractOwned<CABucket>
        implements RowBucket<CABatch, CABucket> {
    private CABatch b;
    private long[] has;
    private LIFOPool<RowBucket<CABatch, ?>> pool;

    static int estimateBytes(int rows, int cols) {
        return 16+8*4
                + 20+rows*cols*8          /* long[] ids */
                + 20+BS.longsFor(rows)*8; /* long[] has */
    }

    public static Orphan<CABucket> create(int rows, int cols) {
        return new Concrete(rows, cols);
    }

    private CABucket(int rows, int cols) {
        int safeRows = Math.max(1, rows);
        b = CABatch.createNotPooled(safeRows, cols).takeOwnership(this);
        b.clear(cols).garbageFillUntil(rows);
        this.has = cleanLongsAtLeast(BS.longsFor(safeRows));
    }

    @Override public @Nullable CABucket recycle(Object currentOwner) {
        if (pool != null) {
            internalMarkRecycled(currentOwner);
            if (pool.offer(this) == null)
                return null;
            currentOwner = SpecialOwner.RECYCLED;
        }
        internalMarkGarbage(currentOwner);
        b   = b.recycle(this);
        has = recycleLongs(has);
        return null;
    }

    static final class Concrete extends CABucket
            implements Orphan<CABucket> {
        public Concrete(int rows, int cols) {super(rows, cols);}
        @Override public CABucket takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @This CABucket setPool(LIFOPool<RowBucket<CABatch, ?>> pool) {
        this.pool = pool;
        return this;
    }

    @Override public CABatchType batchType() { return CABatchType.CA; }

    @Override public void maximizeCapacity() { b.garbageFillUntil(b.rowsCapacity()); }

    @Override public void grow(int additionalRows) {
        int nRows = b.rows+additionalRows;
        if (nRows > b.rowsCapacity()) {
            var bigger = CABatch.createNotPooled(nRows, b.cols).takeOwnership(this);
            var old = b;
            bigger.copy(old);
            bigger.garbageFillUntil(nRows);
            b = bigger;
            old.recycle(this);
            int oldWords = has.length, requiredWords = BS.longsFor(nRows);
            if (requiredWords > oldWords) {
                has = ArrayAlloc.grow(has, requiredWords);
                Arrays.fill(has, oldWords, has.length, 0L);
            }
        } else if (nRows > b.rows) {
            b.garbageFillUntil(nRows);
        }
    }

    @Override public void clear(int rows, int cols) {
        if (rows*cols > b.termsCapacity()) {
            var bigger = CABatch.createNotPooled(rows, cols).takeOwnership(this);
            bigger.garbageFillUntil(rows);
            b.recycle(this);
            b = bigger;
        } else {
            b.clear(cols).garbageFillUntil(rows);
        }
        int reqWords = BS.longsFor(rows);
        if (reqWords > has.length) {
            recycleLongs(has);
            has = cleanLongsAtLeast(reqWords);
        } else {
            Arrays.fill(has, 0L);
        }
    }

    @Override public int            cols() { return b.cols; }
    @Override public int        capacity() { return b.rows; }
    @Override public int hashCode(int row) { return b.hash(row); }

    @Override public boolean has(int row) {
        return row < b.rows && BS.get(has, row);
    }

    @Override public void set(int dst, CABatch other, int row) {
        b.setRow(dst, other, row);
        BS.set(has, dst);
    }

    @Override public void set(int dst, RowBucket<CABatch, ?> other, int src) {
        var bucket = (CABucket)other;
        if (BS.get(bucket.has, src)) {
            b.setRow(dst, bucket.b, src);
            BS.set(has, dst);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void set(int dst, int src) {
        if (src == dst)
            return;
        if (BS.get(has, src)) {
            BS.set(has, dst);
            b.setRow(dst, b, src);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void putRow(CABatch dst, int srcRow) {
        if (BS.get(has, srcRow))
            dst.putRow(b, srcRow);
    }

    @Override public boolean equals(int row, CABatch other, int otherRow) {
        return BS.get(has, row) && b.equals(row, other, otherRow);
    }

    private static final byte[] DUMP_NULL = "null".getBytes(StandardCharsets.UTF_8);
    @Override public void dump(MutableRope dest, int row) {
        if (!has(row)) {
            dest.append(DUMP_NULL);
        } else {
            dest.append('[');
            int cols = b.cols;
            for (int c = 0; c < cols; c++)
                b.writeNT(c==0 ? dest : dest.append(',').append(' '), row, c);
            dest.append(']');
        }
    }

    @Override public String toString() {
        return getClass().getSimpleName()+"{capacity="+capacity()+'}';
    }

    @Override public @NonNull It iterator() { return new It(); }

    public class It implements Iterator<CABatch> {
        private boolean atEnd = false;

        @Override public boolean hasNext() { return  !atEnd; }

        @Override public CABatch next() {
            if (!hasNext()) throw new NoSuchElementException();
            atEnd = true;
            return b;
        }
    }
}
