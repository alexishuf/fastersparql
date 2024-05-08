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

import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.*;
import static java.lang.System.arraycopy;

public abstract sealed class IdBatchBucket<B extends IdBatch<B>>
        extends AbstractOwned<IdBatchBucket<B>>
        implements RowBucket<B, IdBatchBucket<B>> {

    private long[] ids;
    private long[] has;
    private int rows, cols, rowsCapacity;
    private final IdBatchType<B> type;
    private LIFOPool<RowBucket<B, ?>> pool;

    static int estimateBytes(int rows, int cols) {
        return 16+8*4
                + 20+rows*cols*8          /* long[] ids */
                + 20+BS.longsFor(rows)*8; /* long[] has */
    }

    public IdBatchBucket(IdBatchType<B> type, int rows, int cols) {
        this.type = type;
        this.rows = rows;
        this.cols = cols;
        this.ids          = longsAtLeast(rows*cols);
        this.rowsCapacity = ids.length/Math.max(1, cols);
        this.has          = cleanLongsAtLeast(BS.longsFor(rowsCapacity));
    }

    @Override public @Nullable IdBatchBucket<B> recycle(Object currentOwner) {
        if (pool != null) {
            internalMarkRecycled(currentOwner);
            if (pool.offer(this) == null)
                return null;
            currentOwner = SpecialOwner.RECYCLED;
        }
        internalMarkGarbage(currentOwner);
        has = recycleLongs(has);
        return null;
    }

    static final class Concrete<B extends IdBatch<B>> extends IdBatchBucket<B>
            implements Orphan<IdBatchBucket<B>> {
        public Concrete(IdBatchType<B> type, int rows, int cols) {super(type, rows, cols);}
        @Override public IdBatchBucket<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @This IdBatchBucket<B> setPool(LIFOPool<RowBucket<B, ?>> pool) {
        this.pool = pool;
        return this;
    }

    @Override public IdBatchType<B> batchType() { return type; }

    @Override public void maximizeCapacity() {
        rows = rowsCapacity;
    }

    @Override public void grow(int additionalRows) {
        int nRows = rows+additionalRows;
        if (nRows > rowsCapacity) {
            int safeCols = Math.max(1, cols);
            ids = ArrayAlloc.grow(ids, nRows*safeCols);
            rowsCapacity = ids.length/safeCols;
            int oldWords = has.length, requiredWords = BS.longsFor(rowsCapacity);
            if (requiredWords > oldWords) {
                has = ArrayAlloc.grow(has, requiredWords);
                Arrays.fill(has, oldWords, has.length, 0L);
            }
        }
        rows = nRows;
    }

    @Override public void clear(int rows, int cols) {
        boolean clean = false;
        int safeCols = Math.max(1, cols);
        if (cols != this.cols) {
            rowsCapacity = ids.length/safeCols;
            this.cols = cols;
        }
        if (rows > rowsCapacity) {
            recycleLongs(ids);
            ids          = longsAtLeast(rows*safeCols);
            rowsCapacity = ids.length/safeCols;
            int reqWords = BS.longsFor(rows);
            if (reqWords > has.length) {
                recycleLongs(has);
                has = cleanLongsAtLeast(reqWords);
                clean = true;
            }
        }
        this.rows = rows;
        if (!clean)
            Arrays.fill(has, 0L);
    }

    @Override public int            cols() { return cols; }
    @Override public int        capacity() { return rows; }
    @Override public int hashCode(int row) {
        int acc = 0, cols = this.cols, end = row*(cols+1);
        if (end > ids.length)
            throw new IndexOutOfBoundsException(row);
        if (BS.get(has, row)) {
            for (int i = row*cols; i < end; ++i)
                acc ^= type.hashId(ids[i]);
        }
        return acc;
    }

    @Override public boolean has(int row) {
        return row < rowsCapacity && BS.get(has, row);
    }

    @Override public void set(int dst, B batch, int row) {
        int cols = batch.cols;
        if (cols != this.cols)
            throw new IllegalArgumentException("cols mismatch");
        else if (row >= batch.rows)
            throw new IndexOutOfBoundsException(row);
        BS.set(has, dst);
        arraycopy(batch.arr, row*cols, ids, dst*cols, cols);
    }

    @Override public void set(int dst, RowBucket<B, ?> other, int src) {
        @SuppressWarnings("unchecked") var bucket = (IdBatchBucket<B>)other;
        int cols = bucket.cols;
        if (cols != this.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (dst < 0 || dst >= rows || src < 0 || src >= bucket.rows)
            throw new IndexOutOfBoundsException("src or dst are out of bounds");
        if (BS.get(bucket.has, src)) {
            BS.set(has, dst);
            arraycopy(bucket.ids, src*cols, ids, dst*cols, cols);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void set(int dst, int src) {
        if (src == dst)
            return;
        int cols = this.cols;
        if (BS.get(has, src)) {
            BS.set(has, dst);
            arraycopy(ids, src*cols, ids, dst*cols, cols);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void putRow(B dst, int srcRow) {
        if (BS.get(has, srcRow)) {
            if (dst.cols != cols)
                throw new IllegalArgumentException("cols mismatch");
            dst.putRow(ids, srcRow*cols);
        }
    }

    @Override public boolean equals(int row, B other, int otherRow) {
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (row < 0 || row > rows)
            throw new IndexOutOfBoundsException(row);
        return BS.get(has, row) && other.equals(otherRow, ids, row*cols);
    }

    private static final byte[] DUMP_NULL = "null".getBytes(StandardCharsets.UTF_8);
    @Override public void dump(MutableRope dest, int row) {
        if (!has(row)) {
            dest.append(DUMP_NULL);
        } else {
            dest.append('[');
            int cols = this.cols;
            for (int i = row*cols, end = i+cols; i < end; i++)
                type.appendNT(dest, ids[i], DUMP_NULL).append(',').append(' ');
            if (cols > 0)
                dest.len -= 2;
            dest.append(']');
        }
    }

    @Override public String toString() {
        return getClass().getSimpleName()+"{capacity="+capacity()+'}';
    }

    @Override public @NonNull It iterator() {
        return new It();
    }

    public class It implements Iterator<B>, AutoCloseable {
        private B tmp = type.create(cols).takeOwnership(this);
        private int row = 0;
        private boolean filled = false;

        @Override public void close() {
            if (tmp != null) tmp = tmp.recycle(this);
        }

        @Override public boolean hasNext() {
            boolean hasNext = tmp != null;
            if (hasNext && !filled) {
                filled = true;
                tmp.clear();
                var bucket = IdBatchBucket.this;
                long[] has = bucket.has;
                long[] ids = bucket.ids;
                int rows = bucket.rows, cols = bucket.cols;
                for (int terms = 0; terms < tmp.termsCapacity && row < rows; ++row) {
                    if (BS.get(has, row)) {
                        terms += cols;
                        tmp.putRow(ids, row*cols);
                    }
                }
                hasNext = tmp.rows > 0;
                if (!hasNext)
                    close();
            }
            return hasNext;
        }

        @Override public B next() {
            if (!hasNext()) throw new NoSuchElementException();
            filled = false;
            return tmp;
        }
    }
}
