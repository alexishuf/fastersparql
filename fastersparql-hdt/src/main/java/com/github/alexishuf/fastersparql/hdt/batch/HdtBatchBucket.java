package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import static java.lang.System.arraycopy;

public class HdtBatchBucket implements RowBucket<HdtBatch> {
    private final long NULL = IdAccess.NOT_FOUND;
    private final HdtBatch b;

    public HdtBatchBucket(int rowsCapacity, int cols) {
        b = HdtBatchType.INSTANCE.create(rowsCapacity, cols, 0);
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

    @Override public BatchType<HdtBatch> batchType()             { return HdtBatch.TYPE; }
    @Override public int                       cols()            { return b.cols; }
    @Override public int                   capacity()            { return b.rowsCapacity(); }
    @Override public @Nullable HdtBatch    batchOf(int rowSlot)  { return b; }
    @Override public int                   batchRow(int rowSlot) { return rowSlot; }

    @Override public void set(int dst, HdtBatch batch, int row) {
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

    @Override public boolean equals(int row, HdtBatch other, int otherRow) {
        int cols = other.cols;
        if (cols != b.cols) throw new IllegalArgumentException();
        long[] la = b.arr, ra = other.arr;
        for (int l = row*cols, r = otherRow*cols, e = l+cols; l < e; l++, r++)
            if (!Objects.equals(la[l], ra[r])) return false;
        return true;
    }

    @Override public Iterator<HdtBatch> iterator() {
        return new Iterator<>() {
            boolean has = true;
            @Override public boolean hasNext() {
                return has;
            }

            @Override public HdtBatch next() {
                if (!has) throw new NoSuchElementException();
                has = false;
                return b;
            }
        };
    }

    @Override public String toString() { return "HdtBatchBucket{capacity="+capacity()+"}"; }
}
