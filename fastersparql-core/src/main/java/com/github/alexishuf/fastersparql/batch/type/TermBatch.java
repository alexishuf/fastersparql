package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.arraycopy;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class TermBatch extends Batch<TermBatch> {
    Term[] arr;
    private int offerIdx = Integer.MAX_VALUE, offerEnd = 0;

    public Term[] arr() { return arr; }

    /* --- --- --- lifecycle --- --- --- */

    /**
     * Creates a batch that holds {@code arr} <strong>BY REFERENCE</strong>. {@code arr} must
     * enumerate all terms of all rows in row-major order (column {@code c} of row {@code r} is
     * at index {@code r*cols + c}).
     *
     * @param arr row-major array of terms. held by <strong>REFERENCE</strong>
     * @param rows number of rows in {@code lst}
     * @param cols number of columns in {@code lst}
     * @throws IllegalArgumentException if {@code arr.length < rows*cols}
     */
    public TermBatch(Term[] arr, int rows, int cols) {
        super(rows, cols);
        this.arr = arr;
        if (arr.length < rows*cols)
            throw new IllegalArgumentException("arr.length < rows*cols");
    }

    /**
     * Creates a batch that holds {@code lst} <strong>BY REFERENCE</strong>. {@code lst} must
     * enumerate all terms of all rows in row-major order (column {@code c} of row {@code r} is
     * at index {@code r*cols + c}).
     *
     * @param lst row-major list of terms. held by <strong>REFERENCE</strong>
     * @param rows number of rows in {@code lst}
     * @param cols number of columns in {@code lst}
     * @throws IllegalArgumentException if {@code lst.size() < rows*cols}
     */
    public static TermBatch rowMajor(List<Term> lst, int rows, int cols) {
        if (lst.size() != rows*cols)
            throw new IllegalArgumentException("lst.size() < rows*cols");
        TermBatch b = TERM.create(rows, cols, 0);
        b.reserve(rows, 0);
        b.arr = lst.toArray(b.arr);
        b.rows = rows;
        return b;
    }

    /** Create a {@link TermBatch} with a single row and {@code row.size()} columns. */
    @SafeVarargs
    public static TermBatch of(List<Term>... rows) {
        int cols = -1;
        List<Term> flat = new ArrayList<>();
        for (List<Term> row : rows) {
            if (cols == -1)
                cols = row.size();
            else if (cols != row.size())
                throw new IllegalArgumentException("Non-uniform columns counts");
            flat.addAll(row);
        }
        TermBatch b = TERM.create(rows.length, cols, 0);
        b.reserve(rows.length, 0);
        b.arr = flat.toArray(b.arr);
        b.rows  = rows.length;
        return b;
    }


    public TermBatch(int rowsCapacity, int cols) {
        super(0, cols);
        this.arr = new Term[Math.max(1, rowsCapacity)*cols];
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public Batch<TermBatch> copy() {
        return new TermBatch(arr, rows, cols);
    }

    @Override public int bytesUsed() { return (rows*cols)<<3; }

    @Override public int rowsCapacity() {
        return arr.length/Math.max(1, cols);
    }

    @Override public boolean hasCapacity(int rowsCapacity, int bytesCapacity) {
        return arr.length >= rowsCapacity;
    }

    @Override public boolean hasMoreCapacity(TermBatch o) {
        return arr.length > o.arr.length;
    }

    /* --- --- --- row accessors --- --- --- */

    @Override public int bytesUsed(int row) { return cols<<3; }

    /* --- --- --- term accessors --- --- --- */

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        return arr[row * cols + col];
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void reserve(int additionalRows, int addBytes) {
        int required = (rows+additionalRows) * cols;
        if (arr.length < required)
            arr = Arrays.copyOf(arr, Math.max(required, arr.length+(arr.length>>2)));
    }

    @Override public void clear(int newColumns) {
        rows = 0;
        cols = newColumns;
    }

    @Override public boolean beginOffer() {
        int required = (rows + 1) * cols;
        if (arr.length < required) return false;
        for (int i = required-cols; i < required; i++) arr[i] = null;
        offerEnd = required;
        offerIdx = required-cols;
        return true;
    }

    @Override public boolean offerTerm(Term t) {
        if (offerIdx >= offerEnd) throw new IllegalStateException();
        arr[offerIdx] = t;
        ++offerIdx;
        return true;
    }

    @Override public boolean commitOffer() {
        if (offerIdx != offerEnd)
            throw new IllegalStateException();
        ++rows;
        offerEnd = 0;
        offerIdx = Integer.MAX_VALUE;
        return true;
    }

    @Override public boolean offer(TermBatch other) {
        if (other.cols != cols) throw new IllegalArgumentException();
        int out = rows * cols, nTerms = other.rows*other.cols;
        if (nTerms > arr.length-out) return false;
        arraycopy(other.arr, 0, arr, out, nTerms);
        rows += other.rows;
        return true;
    }

    @Override public void put(TermBatch other) {
        if (other.cols != cols)
            throw new IllegalArgumentException();
        int oRows = other.rows;
        reserve(oRows, 0);
        arraycopy(other.arr, 0, arr, rows*cols, oRows *other.cols);
        rows += oRows;
    }

    @Override public void beginPut() {
        reserve(1, 0);
        beginOffer();
    }

    @Override public void putTerm(Term t) { offerTerm(t); }

    @Override public void commitPut() { commitOffer(); }

    @Override public void putRow(TermBatch other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException();
        reserve(1, 0);
        arraycopy(other.arr, row*other.cols, arr, cols*rows++, cols);
    }

    @Override public void putRow(Term[] row) {
        if (row.length != cols) throw new IllegalArgumentException();
        reserve(1, 0);
        arraycopy(row, 0, arr, cols*rows++, row.length);
    }

    /* --- --- --- operation objects --- --- --- */
    private static final VarHandle REC_TMP;
    @SuppressWarnings("unused") // access through REC_TMP
    private static Term[] recTmp;

    static {
        try {
            REC_TMP = lookup().findStaticVarHandle(TermBatch.class, "recTmp", Term[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Get a {@code Term[]} with {@code length >= required} possibly filled with garbage */
    private static Term[] swapTmp(Term @Nullable [] offer, int required) {
        Term[] old = (Term[])REC_TMP.getOpaque();
        int oLen = old == null ? 0 : old.length;
        boolean hasCapacity = oLen >= required;
        if (hasCapacity || (offer != null && offer.length > oLen)) {
            if (offer != null) Arrays.fill(offer, null);
            if (REC_TMP.compareAndSet(old, offer) && hasCapacity) return old;
        }
        return new Term[required];
    }


    static final class Merger extends BatchMerger<TermBatch> {
        private Term @MonotonicNonNull [] tmp;

        public Merger(BatchType<TermBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public TermBatch projectInPlace(TermBatch b) {
            int w = b.cols;
            if (columns == null || columns.length > w) {
                var projected = project(null, b);
                if (projected != b) recycle(b);
                return projected;
            }
            Term[] tmp = this.tmp;
            if (tmp == null || tmp.length < w)
                this.tmp = tmp = swapTmp(tmp, w);
            b.cols = columns.length;
            Term[] arr = b.arr;
            for (int in = 0, out = 0, inEnd = w*b.rows; in < inEnd; in += w) {
                arraycopy(arr, in, tmp, 0, w);
                for (int src : columns)
                    arr[out++] = src >= 0 ? tmp[src] : null;
            }
            return b;
        }
    }

    static final class Filter extends BatchFilter<TermBatch> {
        private Term @MonotonicNonNull [] tmp;

        public Filter(BatchType<TermBatch> batchType, @Nullable BatchMerger<TermBatch> projector,
                      RowFilter<TermBatch> rowFilter) {
            super(batchType, projector, rowFilter);
        }

        private TermBatch filterInPlaceEmpty(TermBatch b) {
            int survivors = 0, rows = b.rows;
            for (int r = 0; r < rows; r++) {
                if (!rowFilter.drop(b, r)) ++survivors;
            }
            if (projector != null)
                b.cols = requireNonNull(projector.columns).length;
            b.rows = survivors;
            b.offerIdx = Integer.MAX_VALUE;
            Arrays.fill(b.arr, null);
            return b;
        }

        @Override public TermBatch filterInPlace(TermBatch b, BatchMerger<TermBatch> projector) {
            Term[] arr = b.arr;
            int r = 0, rows = b.rows, w = b.cols, out = 0;
            int @Nullable [] columns = projector == null ? null : projector.columns;
            if (rows == 0 || w == 0 || (columns != null && columns.length == 0))
                return filterInPlaceEmpty(b);
            if (columns != null && rowFilter.targetsProjection()) {
                b = projector.projectInPlace(b);
                columns = null;
            }
            if (columns == null) {
                //move r until we find a gap start (1+ rows that must be dropped)
                while (r < rows && !rowFilter.drop(b, r)) ++r;
                out = r*w; // rows in [0, r) must be kept
                ++r; // r==rows or must be dropped, do not call drop(b, r) again
                for (int keep, kTerms; r < rows; out += kTerms) {
                    // skip over rows to be dropped
                    while (r < rows &&  rowFilter.drop(b, r)) ++r;
                    // find keep region [keep, keep+kTerms). ++r because either r==rows or is a keep
                    kTerms = (keep = r++) < rows ? w : 0;
                    for (; r < rows && !rowFilter.drop(b, r); ++r) kTerms += w;
                    // copy keep rows
                    arraycopy(arr, keep*w, arr, out, kTerms);
                }
                b.rows = out/w;
            } else {
                Term[] tmp = this.tmp;
                if (tmp == null || tmp.length < w)
                    this.tmp = tmp = swapTmp(tmp, w);
                boolean mayGrow = columns.length*rows > arr.length;
                // when projecting and filtering, there is no gain in copying regions
                for (int inRowStart = 0; r < rows; ++r, inRowStart += w) {
                    if (rowFilter.drop(b, r)) continue;
                    arraycopy(arr, inRowStart, tmp, 0, w);
                    if (mayGrow && out+columns.length > arr.length) {
                        int newLen = Math.max(columns.length*rows, arr.length+(arr.length>>1));
                        b.arr = arr = Arrays.copyOf(arr, newLen);
                    }
                    for (int src : columns)
                        arr[out++] = src >= 0 ? tmp[src] : null;
                }
                b.cols = columns.length;
                b.rows = out/columns.length;
            }
            return b;
        }
    }
}