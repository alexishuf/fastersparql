package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.System.arraycopy;

public class StoreBatchBucket implements RowBucket<StoreBatch> {
    private final long NULL = 0;
    private final StoreBatch b;

    public StoreBatchBucket(int rowsCapacity, int cols) {
        b = StoreBatchType.INSTANCE.create(rowsCapacity, cols, 0);
        b.reserve(rowsCapacity, 0);
        Arrays.fill(b.arr, NULL);
        b.rows = rowsCapacity;
    }

    @Override public void grow(int additionalRows) {
        int oldLen = b.arr.length;
        b.arr = Arrays.copyOf(b.arr, oldLen +(additionalRows*b.cols));
        Arrays.fill(b.arr, oldLen, b.arr.length, NULL);
        b.rows += additionalRows;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        int required = rowsCapacity * cols, len = b.arr.length;
        if (len < required)
            b.arr = new long[len = required];
        Arrays.fill(b.arr, NULL);
        b.cols = cols;
        b.rows = len/cols;
    }

    @Override public boolean has(int row) {
        long[] a = b.arr;
        for (int cols = b.cols, i = row*cols, e = i+cols; i < e; i++)
            if (a[i] != NULL) return true;
        return false;
    }

    @Override public BatchType<StoreBatch> batchType()             { return StoreBatch.TYPE; }
    @Override public int                       cols()            { return b.cols; }
    @Override public int                   capacity()            { return b.rowsCapacity(); }
    @Override public @Nullable StoreBatch    batchOf(int rowSlot)  { return b; }
    @Override public int                   batchRow(int rowSlot) { return rowSlot; }

    @Override public void set(int dst, StoreBatch batch, int row) {
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

    @Override public boolean equals(int row, StoreBatch other, int otherRow) {
        return b.equals(row, other, otherRow);
    }

    @Override public Iterator<StoreBatch> iterator() {
        return new Iterator<>() {
            boolean has = true;
            @Override public boolean hasNext() {
                return has;
            }

            @Override public StoreBatch next() {
                if (!has) throw new NoSuchElementException();
                has = false;
                return b;
            }
        };
    }

    @Override public String toString() { return "StoreBatchBucket{capacity="+capacity()+"}"; }
}
