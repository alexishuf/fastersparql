package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.VarHandle;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static java.lang.System.arraycopy;

public final class TermBatch extends Batch<TermBatch> {
    private static final Term[] EMPTY_TERMS = new Term[0];

    Term[] arr;
    private int offerRowBase = -1;

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
        BatchEvent.Created.record(this);
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
        int terms = rows*cols;
        if (lst.size() != terms)
            throw new IllegalArgumentException("lst.size() < terms");
        TermBatch b = TERM.createForTerms(terms, cols);
        for (int i = 0; i < terms; i++)
            b.arr[i] = lst.get(i);
        b.rows = rows;
        return b;
    }

    /** Create a {@link TermBatch} with a single row and {@code row.size()} columns. */
    @SafeVarargs
    public static TermBatch of(List<Term>... rows) {
        int cols = rows.length == 0 ? 0 : rows[0].size();
        TermBatch b = TERM.createForTerms(rows.length*cols, cols);
        Term[] arr = b.arr;
        int out = 0;
        for (List<Term> row : rows) {
            for (int c = 0; c < cols; c++)
                arr[out++] = row.get(c);
        }
        b.rows  = rows.length;
        return b;
    }

    public void markGarbage() {
        if (MARK_POOLED && (int)P.getAndSetRelease(this, P_GARBAGE) != P_POOLED)
            throw new IllegalStateException("marking not pooled as garbage");
        arr = EMPTY_TERMS;
        BatchEvent.Garbage.record(this);
    }

    @Override public boolean validate() {
        if (!SELF_VALIDATE)
            return true;
        if (rows < 0 || cols < 0)
            return false; // negative dimensions
        if (rows*cols > termsCapacity())
            return false;
        if (!hasCapacity(rows*cols, localBytesUsed()))
            return false;
        if ((byte)P.getOpaque(this) != P_UNPOOLED)
            return false; //pooled or garbage is not valid
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                if (!equals(r, c, this, r, c))
                    return false;
                Term term = get(r, c);
                if (!equals(r, c, term))
                    return false;
                if (term != null && hash(r, c) != term.hashCode())
                    return false;
            }
            if (!equals(r, this, r))
                return false;
        }
        return true;
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public TermBatchType type()         { return TERM; }
    @Override public int           rowsCapacity() { return arr.length/Math.max(1, cols); }
    @Override public int          termsCapacity() { return arr.length; }
    @Override public int     totalBytesCapacity() { return arr.length*4; }

    @Override public boolean hasCapacity(int terms, int localBytes) { return terms <= arr.length; }

    @Override public TermBatch withCapacity(int addRows) {
        requireUnpooled();
        int cols = this.cols, terms = (rows+addRows)*cols;
        return terms > arr.length ? grown(terms, cols, null, null) : this;
    }

    private TermBatch grown(int terms, int cols, @Nullable VarHandle rec, @Nullable Object holder) {
        var dst = TERM.createForTerms(terms, cols).doAppend(arr, 0, rows, cols);
        if (rec != null) {
            if (rec.compareAndExchangeRelease(holder, null, markPooled()) == null)
                return dst;
            markUnpooledNoTrace();
        }
        TERM.recycle(this);
        return dst;
    }

    private TermBatch doAppend(Term[] arr, int row, int rowCount, int cols) {
        arraycopy(arr, row*cols, this.arr, this.rows*cols, rowCount*cols);
        this.rows += rowCount;
        assert validate();
        return this;
    }

    @Override public TermBatch copy(@Nullable TermBatch dest) {
        requireUnpooled();
        int rows = this.rows, cols = this.cols;
        return TERM.empty(dest, rows, cols).doAppend(arr, 0, rows, cols);
    }

    @Override public TermBatch copyRow(int row, @Nullable TermBatch dst) {
        requireUnpooled();
        int cols = this.cols;
        return TERM.emptyForTerms(dst, cols, cols).doAppend(arr, row, 1, cols);
    }

    /* --- --- --- term accessors --- --- --- */

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        requireUnpooled();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException();
        return arr[row * cols + col];
    }

    @Override public @Nullable PlainRope getRope(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? null : new TwoSegmentRope(t.first(), t.second());
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, Term dest) {
        Term t = get(row, col);
        if (t == null) return false;
        dest.set(t.shared(), t.local(), t.sharedSuffixed());
        return true;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        Term t = get(row, col);
        if (t == null) return false;
        dest.wrapFirst(t.first());
        dest.wrapSecond(t.second());
        return true;
    }

    /* --- --- --- mutators --- --- --- */

    @Override public @Nullable TermBatch recycle() {return TERM.recycle(this);}

    @Override public void clear() { rows = 0; }

    @Override public @This TermBatch clear(int newColumns) {
        rows = 0;
        cols = newColumns;
        return this;
    }

    @Override public void abortPut() throws IllegalStateException {
        requireUnpooled();
        if (offerRowBase < 0) return;
        offerRowBase = -1;
    }

//    @Override public void put(TermBatch other) {
//        if (other.cols != cols)
//            throw new IllegalArgumentException();
//        int oRows = other.rows;
//        reserve(oRows, 0);
//        arraycopy(other.arr, 0, arr, rows*cols, oRows *other.cols);
//        rows += oRows;
//    }

    @Override
    public TermBatch put(TermBatch other, @Nullable VarHandle rec, Object holder) {
        int oRows = other.rows, cols = this.cols;
        if (oRows <= 0) return this; // no-op
        if (cols != other.cols) throw new IllegalArgumentException("cols mismatch");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int rows = this.rows, terms = (rows+oRows)*cols;
        var dst = terms > arr.length ? grown(terms, cols, rec, holder) : this;
        return dst.doAppend(other.arr, 0, oRows, cols);
    }

    @Override public TermBatch put(TermBatch other) { return put(other, null, null); }

    @Override public @This TermBatch putConverting(Batch<?> other, @Nullable VarHandle rec,
                                                   @Nullable Object holder) {
        int cols = this.cols, oRows = other.rows;
        if (cols != other.cols) throw new IllegalArgumentException();
        if (oRows <= 0) return this;
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int terms = (rows+oRows)*cols;
        TermBatch dst = terms > arr.length ? grown(terms, cols, rec, holder) : this;
        Term[] arr = dst.arr;
        for (int r = 0, i = rows*cols; r < oRows; r++) {
            for (int c = 0; c < cols; c++)
                arr[i++] = other.get(r, c);
        }
        dst.rows += oRows;
        assert dst.validate();
        return dst;
    }

    @Override public TermBatch putRowConverting(Batch<?> other, int row) {
        if (other.type() == TERM)
            return putRow((TermBatch) other, row);
        int cols = this.cols;
        if (cols != other.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int terms = ++rows*cols;
        var dst = terms > arr.length ? grown(terms, cols, null, null) : this;
        var arr = dst.arr;
        for (int c = 0, out = terms-cols; c < cols; c++)
            arr[out+c] = other.get(row, c);
        assert validate();
        return dst;
    }

    @Override public TermBatch beginPut() {
        requireUnpooled();
        int cols = this.cols, terms = (rows+1)*cols;
        var dst = terms > arr.length ? grown(terms, cols, null, null) : this;
        var arr = dst.arr;
        for (int i = terms-cols; i < terms; i++) arr[i] = null;
        dst.offerRowBase = terms-cols;
        return dst;
    }

    @Override public void putTerm(int col, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (col < 0 || col >= cols) throw new IndexOutOfBoundsException();
        arr[offerRowBase+ col] = t == null ? null : t.asImmutable();
    }

    @Override public void commitPut() {
        requireUnpooled();
        if (offerRowBase < 0) throw new IllegalStateException();
        ++rows;
        offerRowBase = -1;
        assert validate();
    }

    @Override public TermBatch putRow(TermBatch other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (row < 0 || row >= other.rows) throw new IndexOutOfBoundsException("row out of bounds");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int cols = this.cols, terms = (rows+1)*cols;
        var dst = terms > arr.length ? grown(terms, cols, null, null) : this;
        return dst.doAppend(other.arr, row, 1, cols);
    }

    /* --- --- --- operation objects --- --- --- */

    public static final class Merger extends BatchMerger<TermBatch> {
        public Merger(BatchType<TermBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override
        public TermBatch merge(@Nullable TermBatch dst, TermBatch left, int leftRow,
                               @Nullable TermBatch right) {
            int rows = right == null ? 0 : right.rows;
            dst = TERM.withCapacity(dst, rows, sources.length);
            if (rows == 0)
                return mergeWithMissing(dst, left, leftRow);
            int l = leftRow*left.cols, d = dst.rows*dst.cols, dEnd = rows*dst.cols;
            dst.rows += rows;
            Term[] arr = dst.arr, rArr = right.arr, lArr = left.arr;
            for (int rt = 0, rc = right.cols; d < dEnd; rt+=rc) {
                for (int c = 0, src; c < sources.length; c++, d++) {
                    arr[d] = (src=sources[c]) == 0 ? null
                           : src > 0 ? lArr[l+src-1] : rArr[rt-src-1];
                }
            }
            assert dst.validate();
            return dst;
        }

        private TermBatch mergeWithMissing(TermBatch dst, TermBatch left, int leftRow) {
            Term[] dIds = dst.arr, lIds = left.arr;
            int d = dst.rows*dst.cols, l = leftRow*left.cols;
            ++dst.rows;
            for (int c = 0, src; c < sources.length; c++)
                dIds[d+c] = (src=sources[c]) < 0 ? null : lIds[l+src];
            return dst;
        }

        @Override public TermBatch project(TermBatch dst, TermBatch in) {
            int[] cols = this.columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            return projectInto(TERM.withCapacity(dst, in.rows, cols.length), cols, in);
        }

        @Override public TermBatch projectRow(@Nullable TermBatch dst, TermBatch in, int row) {
            if (columns == null) throw new UnsupportedOperationException("not a projecting merger");
            dst = TERM.withCapacity(dst, 1, columns.length);
            Term[] dArr = dst.arr, iArr = in.arr;
            int i = row*in.cols, d = dst.rows*dst.cols;
            ++dst.rows;
            for (int c = 0, src; c < columns.length; c++)
                dArr[d+c] = (src=columns[c]) < 0 ? null : iArr[i+src];
            assert dst.validate();
            return dst;
        }

        private TermBatch projectInto(TermBatch dst, int[] cols, TermBatch in) {
            Term[] dArr = dst.arr, iArr = in.arr;
            int iCols = in.cols, iEnd = in.rows*iCols, d;
            if (dst == in) {
                d = 0;
                dst.cols = cols.length;
            } else {
                d = dst.rows*dst.cols;
                dst.rows += in.rows;
            }
            for (int it=0; it < iEnd; it+=iCols, d+=cols.length) {
                for (int c = 0, src; c < cols.length; c++)
                    dArr[d+c] = (src=cols[c]) < 0 ? null : iArr[it+src];
            }
            assert dst.validate();
            return dst;
        }

        @Override public TermBatch projectInPlace(TermBatch b) {
            int[] cols = this.columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            int terms = b == null ? 0 : b.rows*cols.length;
            if (terms == 0)
                return projectInPlaceEmpty(b);
            var dst = safeInPlaceProject ? b : TERM.createForTerms(terms, cols.length);
            projectInto(dst, cols, b);
            if (dst != b)
                TERM.recycle(b);
            return dst;
        }
    }

    public static final class Filter extends BatchFilter<TermBatch> {
        private final @Nullable Merger projector;

        public Filter(BatchType<TermBatch> batchType, Vars vars,
                      @Nullable Merger projector, RowFilter<TermBatch> rowFilter,
                      @Nullable BatchFilter<TermBatch> before) {
            super(batchType, vars, rowFilter, before);
            assert projector == null || projector.vars.equals(vars);
            this.projector = projector;
        }

        private TermBatch filterInto(TermBatch dst, TermBatch in) {
            Term[] dArr = dst.arr, iArr = in.arr;
            RowFilter.Decision decision = DROP;
            int r = 0, rows = in.rows, cols = in.cols;
            int dTerm = dst == in ? 0 : dst.rows*cols;
            for (int start = 0, nTerms; r < rows && decision != TERMINATE; start = ++r) {
                while (r < rows && (decision=rowFilter.drop(in, r)) == KEEP)
                    ++r;
                if (r > start) {
                    arraycopy(iArr, start*cols, dArr, dTerm, nTerms=(r-start)*cols);
                    dTerm += nTerms;
                }
            }
            return endFilter(dst, dTerm, cols, decision == TERMINATE);
        }

        private TermBatch projectingFilterInto(TermBatch dst, TermBatch in, Merger projector) {
            int[] cols = projector.columns;
            assert cols != null;
            Term[] dArr = dst.arr, iArr = in.arr;
            int rows = in.rows, inCols = in.cols, dTerm = dst == in ? 0 : dst.rows*dst.cols;
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(in, r)) {
                    case KEEP -> {
                        for (int c = 0, src; c < cols.length; c++)
                            dArr[dTerm+c] = (src=cols[c]) < 0 ? null : iArr[r*inCols+src];
                        dTerm += cols.length;
                    }
                    case TERMINATE -> rows = -1;
                }
            }
            return endFilter(dst, dTerm, cols.length, rows == -1);
        }

        @Override public TermBatch filter(@Nullable TermBatch dst, TermBatch in) {
            if (before != null)
                in = before.filter(null, in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(dst, in);
            var projector = this.projector;
            TermBatch garbage = null;
            if (projector != null && rowFilter.targetsProjection()) {
                in = projector.project(null, in);
                projector = null;
                if (dst == null || dst.rows == 0) {
                    garbage = dst;
                    dst = in;
                } else {
                    garbage = in;
                }
            }
            dst = TERM.withCapacity(dst, in.rows, outColumns);
            dst = projector == null ? filterInto(dst, in)
                                    : projectingFilterInto(dst, in, projector);
            if (garbage != null)
                TERM.recycle(garbage);
            return dst;
        }

        @Override public TermBatch filterInPlace(TermBatch in) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(in, in);
            var projector = this.projector;
            if (projector != null && rowFilter.targetsProjection()) {
                in = projector.projectInPlace(in);
                projector = null;
            }
            return projector == null ? filterInto(in, in) : filterInPlaceProjecting(in, projector);
        }

        private TermBatch filterInPlaceProjecting(TermBatch in, Merger projector) {
            TermBatch dst, garbage;
            if (projector.safeInPlaceProject) {
                garbage = null;
                dst = in;
            } else {
                dst = TERM.createForTerms(in.rows*outColumns, outColumns);
                garbage = in;
            }
            dst = projectingFilterInto(dst, in, projector);
            if (garbage != null)
                TERM.recycle(garbage);
            return dst;
        }
    }
}
