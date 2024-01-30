package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static java.lang.System.arraycopy;

public class TermBatchBucket implements RowBucket<TermBatch> {
    private static final Term NULL = Term.splitAndWrap(new ByteRope("<urn:fastersparql:null>"));
    private TermBatch b;

    public TermBatchBucket(int rows, int cols) {
        if (rows > Short.MAX_VALUE || cols > Short.MAX_VALUE)
            throw new IllegalArgumentException("rows or cols overflow 2-byte short");
        int terms = rows*cols;
        TermBatch b = null;
        if (terms < TERM.preferredTermsPerBatch()) {
            b = TERM.create(cols);
            if (b.termsCapacity() < terms) b = TERM.recycle(b);
        }
        if (b == null)
            b = TERM.createSpecial(terms);
        this.b = b;
        b.rows = (short)rows;
        b.cols = (short)cols;
        Arrays.fill(b.arr, 0, terms, NULL);
    }

    @Override public @Nullable TermBatchBucket recycleInternals() {
        b = TERM.recycleSpecial(b);
        return null;
    }

    @Override public void maximizeCapacity() {
        short old = b.rows;
        if ((b.rows = (short)(b.termsCapacity()/b.cols)) > old)
            Arrays.fill(b.arr, old*b.cols, b.rows*b.cols, NULL);
    }

    @Override public void grow(int addRows) {
        if (addRows <= 0)
            return;
        var b = this.b;
        int nRows = b.rows+addRows, begin = b.rows*b.cols, end = nRows*b.cols;
        if (nRows > Short.MAX_VALUE)
            throw new IllegalArgumentException("new rows will overflow 2-byte short");
        if (end > b.termsCapacity())
            b = reAlloc(b, end);
        b.rows = (short)nRows;
        Arrays.fill(b.arr, begin, end, NULL);
    }

    private TermBatch reAlloc(TermBatch b, int terms) {
        var bigger = TERM.createSpecial(terms).clear(b.cols);
        bigger.copy(b);
        TERM.recycleSpecial(b);
        this.b = b = bigger;
        return b;
    }

    @Override public void clear(int rows, int cols) {
        b = b.clear(cols);
        grow(rows);
    }

    @Override public boolean has(int row) {
        var b = this.b;
        Term[] a = b.arr;
        for (int cols = b.cols, i = row*cols, e = i+cols; i < e; i++)
            if (a[i] != NULL) return true;
        return false;
    }

    @Override public BatchType<TermBatch> batchType()        { return TERM; }
    @Override public int                       cols()        { return b == null ? 0 : b.cols; }
    @Override public int                   capacity()        { return b == null ? 0 : b.rows; }
    @Override public int                   hashCode(int row) { return b.hash(row); }

    @Override public void set(int dst, TermBatch batch, int row) {
        var b = this.b;
        int cols = batch.cols;
        if (cols != b.cols)
            throw new IllegalArgumentException();
        if (dst >= b.rows)
            throw new IndexOutOfBoundsException("dst >= capacity()");
        arraycopy(batch.arr, row*cols, b.arr, dst*cols, cols);
    }



    @Override public void set(int dst, RowBucket<TermBatch> other, int src) {
        var b = this.b;
        TermBatchBucket bucket = (TermBatchBucket) other;
        int cols = b.cols;
        if (bucket.b.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        arraycopy(bucket.b.arr, src*cols, b.arr, dst*cols, cols);
    }

    @Override public void set(int dst, int src) {
        var b = this.b;
        if (src == dst) return;
        Term[] a = b.arr;
        int cols = b.cols;
        arraycopy(a, src*cols, a, dst*cols, cols);
    }

    @Override public void putRow(TermBatch dst, int srcRow) {
        dst.putRow(b, srcRow);
    }

    @Override public boolean equals(int row, TermBatch other, int otherRow) {
        var b = this.b;
        int cols = other.cols;
        if (cols != b.cols) throw new IllegalArgumentException();
        Term[] la = b.arr, ra = other.arr;
        for (int l = row*cols, r = otherRow*cols, e = l+cols; l < e; l++, r++)
            if (!Objects.equals(la[l], ra[r])) return false;
        return true;
    }

    private static final byte[] DUMP_NULL = "null".getBytes(StandardCharsets.UTF_8);

    @Override public void dump(ByteRope dest, int row) {
        if (!has(row)) {
            dest.append(DUMP_NULL);
        } else {
            dest.append('[');
            int cols = cols();
            for (int c = 0; c < cols; c++) {
                Term term = b.get(row, c);
                if (term == null) dest.append(DUMP_NULL);
                else              dest.append(term);
                dest.append(',').append(' ');
            }
            if (cols > 0) dest.len -= 2;
            dest.append('[');
        }
    }

    @Override public @NonNull Iterator<TermBatch> iterator() {
        return new Iterator<>() {
            private TermBatch tmp = TERM.create(b.cols);
            private int row = skipEmpty(0);

            private int skipEmpty(int row) {
                while (row < b.rows && !has(row)) ++row;
                return row;
            }

            @Override public boolean hasNext() {
                boolean has = row < b.rows;
                if (!has && tmp != null)
                    tmp = TERM.recycle(tmp);
                return has;
            }

            @Override public TermBatch next() {
                if (!hasNext()) throw new NoSuchElementException();
                tmp.clear();
                var b = TermBatchBucket.this.b;
                tmp.putRow(b, row);
                if ((row = skipEmpty(++row)) >= b.rows)
                    row = Integer.MAX_VALUE;
                return tmp;
            }
        };
    }

    @Override public String toString() { return "TermBatchBucket{capacity="+capacity()+"}"; }
}
