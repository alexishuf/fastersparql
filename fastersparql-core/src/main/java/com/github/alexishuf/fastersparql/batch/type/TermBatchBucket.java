package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import static java.lang.System.arraycopy;

public class TermBatchBucket implements RowBucket<TermBatch> {
    private static final Term NULL = Term.splitAndWrap(new ByteRope("<urn:fastersparql:null>"));
    private final TermBatch b;

    public TermBatchBucket(int rowsCapacity, int cols) {
        b = TermBatchType.INSTANCE.create(rowsCapacity, cols, 0);
        b.reserve(rowsCapacity, 0);
        b.rows = rowsCapacity;
        Arrays.fill(b.arr, NULL);
    }

    @Override public void grow(int additionalRows) {
        int oldLength = b.arr.length;
        b.arr = Arrays.copyOf(b.arr, oldLength+(additionalRows*b.cols));
        Arrays.fill(b.arr, oldLength, b.arr.length, NULL);
        b.rows += additionalRows;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        int required = rowsCapacity * cols, len = b.arr.length;
        if (len < required)
            b.arr = new Term[len = required];
        Arrays.fill(b.arr, NULL);
        b.cols = cols;
        b.rows = len/cols;
    }

    @Override public boolean has(int row) {
        Term[] a = b.arr;
        for (int cols = b.cols, i = row*cols, e = i+cols; i < e; i++)
            if (a[i] != NULL) return true;
        return false;
    }

    @Override public BatchType<TermBatch> batchType()            { return Batch.TERM; }
    @Override public int                       cols()            { return b.cols; }
    @Override public int                   capacity()            { return b.rowsCapacity(); }
    @Override public @Nullable TermBatch    batchOf(int rowSlot) { return b; }
    @Override public int                   batchRow(int rowSlot) { return rowSlot; }

    @Override public void set(int dst, TermBatch batch, int row) {
        int cols = batch.cols;
        if (cols != b.cols)
            throw new IllegalArgumentException();
        arraycopy(batch.arr, row*cols, b.arr, dst*cols, cols);
    }

    @Override public void set(int dst, int src) {
        if (src == dst) return;
        Term[] a = b.arr;
        int cols = b.cols;
        arraycopy(a, src*cols, a, dst*cols, cols);
    }

    @Override public boolean equals(int row, TermBatch other, int otherRow) {
        int cols = other.cols;
        if (cols != b.cols) throw new IllegalArgumentException();
        Term[] la = b.arr, ra = other.arr;
        for (int l = row*cols, r = otherRow*cols, e = l+cols; l < e; l++, r++)
            if (!Objects.equals(la[l], ra[r])) return false;
        return true;
    }

    @Override public Iterator<TermBatch> iterator() {
        return new Iterator<>() {
            private TermBatch tmp = Batch.TERM.createSingleton(cols());
            private int row = skipEmpty(0);

            private int skipEmpty(int row) {
                while (row < b.rows && !has(row)) ++row;
                return row;
            }

            @Override public boolean hasNext() {
                boolean has = row < b.rows;
                if (!has && tmp != null)
                    tmp = Batch.TERM.recycle(tmp);
                return has;
            }

            @Override public TermBatch next() {
                if (!hasNext()) throw new NoSuchElementException();
                tmp.clear();
                tmp.putRow(b, row);
                if ((row = skipEmpty(++row)) >= b.rows)
                    row = Integer.MAX_VALUE;
                return tmp;
            }
        };
    }

    @Override public String toString() { return "TermBatchBucket{capacity="+capacity()+"}"; }
}
