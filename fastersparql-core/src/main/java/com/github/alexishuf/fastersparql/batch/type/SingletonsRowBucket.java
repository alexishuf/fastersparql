package com.github.alexishuf.fastersparql.batch.type;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SingletonsRowBucket<B extends Batch<B>> implements RowBucket<B> {
    private final BatchType<B> bt;
    private B[] rows;
    private int cols;

    public SingletonsRowBucket(BatchType<B> bt, int rowsCapacity, int cols) {
        //noinspection unchecked
        this.rows = (B[]) Array.newInstance(bt.batchClass(), rowsCapacity);
        this.bt = bt;
        this.cols = cols;
    }

    @Override public void grow(int additionalRows) {
        this.rows = Arrays.copyOf(rows, rows.length+additionalRows);
    }

    @Override public void clear(int rowsCapacity, int cols) {
        recycleInternals();
        Arrays.fill(rows, null);
        this.cols = cols;
    }

    @Override public void recycleInternals() {
        BatchType<B> bt = batchType();
        for (B row : rows) {
            if (row != null  && bt.recycle(row) != null)
                break; // pool is full, stop
        }
    }

    @Override public BatchType<B> batchType()            { return bt; }
    @Override public int           capacity()            { return rows.length; }
    @Override public int               cols()            { return cols; }
    @Override public boolean            has(int row)     { return rows[row] != null; }
    @Override public @Nullable B    batchOf(int rowSlot) { return rows[rowSlot]; }
    @Override public int           batchRow(int rowSlot) { return 0; }

    @Override public void set(int dst, B batch, int row) {
        B current = rows[dst];
        if (current == null) rows[dst] = current = bt.createSingleton(cols);
        else                 current.clear();
        current.putRow(batch, row);
    }

    @Override public void set(int dst, int src) {
        B srcBatch = rows[src];
        if (srcBatch == null)
            bt.recycle(rows[dst]);
        set(dst, srcBatch, 0);
    }

    @Override public boolean equals(int row, B other, int otherRow) {
        B left = rows[row];
        if (left == null) return other == null && otherRow == 0;
        return left.equals(0, other, otherRow);
    }

    @Override public Iterator<B> iterator() {
        return new Iterator<>() {
            private int i = findNext(0);

            private int findNext(int i) {
                while (i < rows.length && rows[i] == null) ++i;
                return i;
            }

            @Override public boolean hasNext() { return i < rows.length; }

            @Override public B next() {
                if (!hasNext()) throw new NoSuchElementException();
                int i = this.i;
                this.i = findNext(i+1);
                return rows[i];
            }
        };
    }

    @Override public String toString() {
        return "SingletonsRowBucket{bt="+bt+", capacity="+rows.length+"}";
    }
}
