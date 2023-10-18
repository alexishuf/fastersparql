package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.System.arraycopy;

public abstract class IdBatchBucket<B extends IdBatch<B>> implements RowBucket<B> {
    protected static final long NULL = 0;
    private B b;

    public IdBatchBucket(B b, int rows) {
        b.rows = 0;
        this.b = b.withCapacity(rows);
        b.rows = rows;
        int terms  = rows*b.cols;
        var arr    = b.arr;
        var hashes = b.hashes;
        for (int i = 0; i < terms; i++)    arr[i] = 0;
        for (int i = 0; i < terms; i++) hashes[i] = 0;
    }

    @Override public void grow(int additionalRows) {
        if (additionalRows <= 0)
            return;
        int begin  = b.rows*b.cols, end = additionalRows*b.cols;
        b          = b.withCapacity(additionalRows);
        b.rows    += additionalRows;
        var arr    = b.arr;
        var hashes = b.hashes;
        for (int i = begin; i < end; i++)    arr[i] = 0;
        for (int i = begin; i < end; i++) hashes[i] = 0;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        b = b.clear(cols).withCapacity(rowsCapacity);
        b.rows = rowsCapacity;
        int terms = rowsCapacity*cols;
        var arr = b.arr;
        var hashes = b.hashes;
        for (int i = 0; i < terms; i++)    arr[i] = 0;
        for (int i = 0; i < terms; i++) hashes[i] = 0;
    }

    @Override public void recycleInternals()        { b = b.recycle(); }
    @Override public int              cols()        { return b.cols; }
    @Override public int          capacity()        { return b.rowsCapacity(); }
    @Override public int hashCode(int row) { return b.hash(row); }

    @Override public boolean has(int row) {
        var b = this.b;
        long[] a = b.arr;
        for (int cols = b.cols, i = row*cols, e = i+cols; i < e; i++)
            if (a[i] != NULL) return true;
        return false;
    }

    @Override public void set(int dst, B batch, int row) {
        var b = this.b;
        int cols = batch.cols;
        if (cols != b.cols)
            throw new IllegalArgumentException();
        arraycopy(batch.arr, row*cols, b.arr, dst*cols, cols);
    }

    @Override public void set(int dst, RowBucket<B> other, int src) {
        var b = this.b;
        IdBatchBucket<B> bucket = (IdBatchBucket<B>) other;
        int cols = b.cols;
        if (bucket.b.cols != b.cols)
            throw new IllegalArgumentException("cols mismatch");
        arraycopy(bucket.b.arr, src*cols, b.arr, dst*cols, cols);
    }

    @Override public void set(int dst, int src) {
        var b = this.b;
        if (src == dst) return;
        long[] a = b.arr;
        int cols = b.cols;
        arraycopy(a, src*cols, a, dst*cols, cols);
    }

    @Override public boolean equals(int row, B other, int otherRow) {
        return b.equals(row, other, otherRow);
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
            boolean has = true;
            @Override public boolean hasNext() { return has; }

            @Override public B next() {
                if (!has) throw new NoSuchElementException();
                has = false;
                return b;
            }
        };
    }
}
