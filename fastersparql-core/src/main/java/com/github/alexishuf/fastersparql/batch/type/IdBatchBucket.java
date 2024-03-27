package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.System.arraycopy;

public final class IdBatchBucket<B extends IdBatch<B>> implements RowBucket<B> {

    private B b;
    private long[] has;
    private final IdBatchType<B> type;

    public IdBatchBucket(IdBatchType<B> type, int rows, int cols) {
        if (rows > Short.MAX_VALUE || cols > Short.MAX_VALUE)
            throw new IllegalArgumentException("rows or cols overflow 2-byte short");
        this.type = type;
        int terms = rows * cols;
        B b = null;
        if (terms <= type.preferredTermsPerBatch()) {
            b = type.create(cols);
            if (b.termsCapacity < terms) b = type.recycle(b);
        }
        if (b == null)
            b = type.createSpecial(terms);
        this.b = b;
        b.rows = (short)rows;
        b.cols = (short)cols;
        int words = BS.longsFor(b.termsCapacity/Math.max(1, cols));
        has = ArrayPool.longsAtLeastUpcycle(words);
        Arrays.fill(has, 0, words, 0L);
    }

    @Override public IdBatchType<B> batchType() { return type; }

    @Override public void maximizeCapacity() {
        short old = b.rows;
        if ((b.rows = (short)(b.termsCapacity/b.cols)) > old)
            BS.clear(has, old, b.rows);
    }

    @Override public void grow(int additionalRows) {
        if (additionalRows <= 0)
            return;
        var b = this.b;
        int oRows = b.rows, nRows = oRows+additionalRows, nTerms = nRows*b.cols;
        if (nRows > Short.MAX_VALUE)
            throw new IllegalArgumentException("new rows will overflow 2-byte short");
        if (nTerms > b.termsCapacity)
            b = reAlloc(b, nTerms);
        b.rows = (short)nRows;
        int words = BS.longsFor(nRows);
        if (has.length < words)
            has = ArrayPool.grow(has, words);
        BS.clear(has, oRows, nRows);
    }

    private B reAlloc(B b, int terms) {
        var bigger = type.createSpecial(terms).clear(b.cols);
        bigger.copy(b);
        type.recycleSpecial(b);
        this.b = b = bigger;
        has = ArrayPool.grow(has, BS.longsFor(b.termsCapacity/Math.max(1, b.cols)));
        return b;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        b = b.clear(cols);
        grow(rowsCapacity);
    }

    @Override public @Nullable IdBatchBucket<B> recycleInternals() {
        b   = type.recycleSpecial(b);
        has = ArrayPool.LONG.offerToNearest(has, has.length);
        return null;
    }
    @Override public int            cols() { return b == null ? 0 : b.cols; }
    @Override public int        capacity() { return b == null ? 0 : b.rowsCapacity(); }
    @Override public int hashCode(int row) { return BS.get(has, row) ? b.hash(row) : 0; }

    @Override public boolean has(int row) {
        return row < b.rows && BS.get(has, row);
    }

    @Override public void set(int dst, B batch, int row) {
        var b = this.b;
        int cols = batch.cols;
        if (cols != b.cols)
            throw new IllegalArgumentException();
        else if (row >= batch.rows)
            throw new IndexOutOfBoundsException(row);
        BS.set(has, dst);
        arraycopy(batch.arr, row*cols, b.arr, dst*cols, cols);
    }

    @Override public void set(int dst, RowBucket<B> other, int src) {
        var b = this.b;
        IdBatchBucket<B> bucket = (IdBatchBucket<B>) other;
        int cols = b.cols;
        if (bucket.b.cols != b.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (BS.get(bucket.has, src)) {
            BS.set(has, dst);
            arraycopy(bucket.b.arr, src*cols, b.arr, dst*cols, cols);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void set(int dst, int src) {
        var b = this.b;
        if (src == dst)
            return;
        long[] a = b.arr;
        int cols = b.cols;
        if (BS.get(has, src)) {
            BS.set(has, dst);
            arraycopy(a, src*cols, a, dst*cols, cols);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void putRow(B dst, int srcRow) {
        if (BS.get(has, srcRow))
            dst.putRow(b, srcRow);
    }

    @Override public boolean equals(int row, B other, int otherRow) {
        return BS.get(has, row) && b.equals(row, other, otherRow);
    }

    private static final byte[] DUMP_NULL = "null".getBytes(StandardCharsets.UTF_8);
    @Override public void dump(ByteRope dest, int row) {
        if (!has(row)) {
            dest.append(DUMP_NULL);
        } else {
            dest.append('[');
            int cols = cols();
            SegmentRope local = SegmentRope.pooled();
            for (int c = 0; c < cols; c++) {
                if (b.localView(row, c, local)) {
                    SegmentRope sh = b.shared(row, c);
                    SegmentRope fst, snd;
                    if (b.sharedSuffixed(row, c)) { fst = local; snd =    sh; }
                    else                          { fst =    sh; snd = local; }
                    dest.append(fst);
                    dest.append(snd);
                } else {
                    dest.append(DUMP_NULL);
                }
                dest.append(',').append(' ');
            }
            if (cols > 0) dest.len -= 2;
            dest.append(']');
        }
    }

    @Override public String toString() {
        return getClass().getSimpleName()+"{capacity="+capacity()+'}';
    }

    @Override public @NonNull Iterator<B> iterator() {
        return new Iterator<>() {
            private B tmp = b.type().create(b.cols);
            private int row = 0;
            private boolean filled = false;

            @Override public boolean hasNext() {
                boolean has = tmp != null;
                if (has && !filled) {
                    filled = true;
                    var b = IdBatchBucket.this.b;
                    tmp.clear(b.cols);
                    for (; row < b.rows; ++row) {
                        if (BS.get(IdBatchBucket.this.has, row))
                            tmp.putRow(b, row);
                    }
                    has = tmp.rows > 0;
                    if (!has)
                        tmp = tmp.recycle();
                }
                return has;
            }

            @Override public B next() {
                if (!hasNext()) throw new NoSuchElementException();
                filled = false;
                return tmp;
            }
        };
    }
}
