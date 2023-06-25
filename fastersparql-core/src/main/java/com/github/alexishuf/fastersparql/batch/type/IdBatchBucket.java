package com.github.alexishuf.fastersparql.batch.type;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.System.arraycopy;

public abstract class IdBatchBucket<B extends IdBatch<B>> implements RowBucket<B> {
    protected static final long NULL = 0;
    protected final B b;

    public IdBatchBucket(B b, int rows) {
        this.b = b;
        b.reserve(rows, 0);
        Arrays.fill(b.arr, NULL);
        b.rows = rows;
    }

    @Override public void grow(int additionalRows) {
        int oldRows = b.rows, newRows = b.rows + additionalRows;
        b.rows = 0;
        b.reserve(newRows, 0);
        Arrays.fill(b.arr, oldRows*b.cols, b.arr.length, NULL);
        b.rows = newRows;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        b.rows = 0;
        b.cols = cols;
        grow(rowsCapacity);
    }

    @Override public void recycleInternals() {
        b.recycleInternals();
    }

    @Override public int            cols()               { return b.cols; }
    @Override public int            capacity()            { return b.rowsCapacity(); }
    @Override public @Nullable B    batchOf(int rowSlot)  { return b; }
    @Override public int            batchRow(int rowSlot) { return rowSlot; }

    @Override public boolean has(int row) {
        long[] a = b.arr;
        for (int cols = b.cols, i = row*cols, e = i+cols; i < e; i++)
            if (a[i] != NULL) return true;
        return false;
    }

    @Override public void set(int dst, B batch, int row) {
        int cols = batch.cols;
        if (cols != b.cols)
            throw new IllegalArgumentException();
        arraycopy(batch.arr, row*cols, b.arr, dst*cols, cols);
    }

    @Override public void set(int dst, int src) {
        if (src == dst) return;
        long[] a = b.arr;
        int cols = b.cols;
        arraycopy(a, src*cols, a, dst*cols, cols);
    }

    @Override public boolean equals(int row, B other, int otherRow) {
        return b.equals(row, other, otherRow);
    }

    @Override public String toString() {
        return getClass().getSimpleName()+"{capacity="+capacity()+'}';
    }

    @Override public Iterator<B> iterator() {
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
