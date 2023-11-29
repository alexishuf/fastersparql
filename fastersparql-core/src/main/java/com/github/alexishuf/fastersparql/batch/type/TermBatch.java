package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;

public final class TermBatch extends Batch<TermBatch> {
    final Term[] arr;
    private short offerRowBase = -1;
    final boolean special;

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
    public TermBatch(Term[] arr, int rows, int cols, boolean special) {
        super((short)rows, (short)cols);
        if (rows > Short.MAX_VALUE)
            throw new IllegalArgumentException("rows > Short.MAX_VALUE");
        this.arr = arr;
        this.special = special;
        if (arr.length < rows*cols)
            throw new IllegalArgumentException("arr.length < rows*cols");
        BatchEvent.Created.record(this);
    }

    private TermBatch createTail() {
        TermBatch b = TERM.create(cols), tail = this.tail;
        if (tail == null)
            throw new UnsupportedOperationException("intermediary batch");
        tail.tail = null;
        tail.next = b;
        this.tail = b;
        return b;
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
        TermBatch b = TERM.create(cols);
        if (b.arr.length < terms) {
            b.recycle();
            b = new TermBatch(new Term[terms], rows, cols, true);
        }
        for (int i = 0; i < terms; i++)
            b.arr[i] = lst.get(i);
        return b;
    }

    /** Create a {@link TermBatch} with a single row and {@code row.size()} columns. */
    @SafeVarargs
    public static TermBatch of(List<Term>... rows) {
        int cols = rows.length == 0 ? 0 : rows[0].size();
        TermBatch b = TERM.create(cols);
        if (b.termsCapacity() < rows.length*cols) {
            b.recycle();
            b = new TermBatch(new Term[rows.length*cols], rows.length, cols, true);
        }
        for (List<Term> row : rows)
            b.putRow(row);
        return b;
    }

    @Override void markGarbage() {
        if (MARK_POOLED && (int)P.getAndSetRelease(this, P_GARBAGE) != P_POOLED)
            throw new IllegalStateException("marking not pooled as garbage");
//        arr = EMPTY_TERMS;
        BatchEvent.Garbage.record(this);
    }

    @Override protected boolean validateNode(Validation validation) {
        if (!SELF_VALIDATE || validation == Validation.NONE)
            return true;
        //noinspection ConstantValue
        if (rows < 0 || cols < 0)
            return false; // negative dimensions
        if (rows*cols > termsCapacity())
            return false;
        if (!hasCapacity(rows*cols, localBytesUsed()))
            return false;
        return (byte) P.getOpaque(this) == P_UNPOOLED; //pooled or garbage is not valid
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public TermBatchType type()         { return TERM; }
    @Override public int           rowsCapacity() { return arr.length/Math.max(1, cols); }
    @Override public int          termsCapacity() { return arr.length; }
    @Override public int     totalBytesCapacity() { return arr.length*4; }

    @Override public boolean hasCapacity(int terms, int localBytes) { return terms <= arr.length; }

    private void doAppend(Term[] arr, int row, short rowCount, short cols) {
        arraycopy(arr, row*cols, this.arr, this.rows*cols, rowCount*cols);
        this.rows += rowCount;
        assert validate();
    }

    @Override public TermBatch dup() {return dup((int)currentThread().threadId());}
    @Override public TermBatch dup(int threadId) {
        TermBatch b = TERM.createForThread(threadId, cols);
        b.copy(this);
        return b;
    }

    @Override public TermBatch dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public TermBatch dupRow(int row, int threadId) {
        requireUnpooled();
        short cols = this.cols;
        TermBatch b = TERM.createForThread(threadId, cols);
        b.doAppend(arr, row, (short)1, cols);
        return b;
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

    @Override public void clear() {
        Arrays.fill(arr, 0, rows*cols, null);
        rows = 0;
        tail = this;
        if (next != null) {
            next = next.recycle();
        }
    }

    @Override public TermBatch clear(int newColumns) {
        if (newColumns > arr.length) {
            TermBatch bigger = TERM.create(newColumns);
            recycle();
            return bigger;
        }
        Arrays.fill(arr, 0, rows*cols, null);
        rows = 0;
        cols = (short)newColumns;
        tail = this;
        if (next != null)
            next = next.recycle();
        return this;
    }

    @Override public void abortPut() throws IllegalStateException {
        TermBatch tail = tail();
        if (tail.offerRowBase < 0) return;
        tail.offerRowBase = -1;
        if (tail != this && tail.rows == 0)
            dropTail(tail);
    }

//    @Override public void put(TermBatch other) {
//        if (other.cols != cols)
//            throw new IllegalArgumentException();
//        int oRows = other.rows;
//        reserve(oRows, 0);
//        arraycopy(other.arr, 0, arr, rows*cols, oRows *other.cols);
//        rows += oRows;
//    }

    @Override public void copy(TermBatch o) {
        short cols = this.cols;
        if (o.cols != cols)
            throw new IllegalArgumentException("other.cols != cols");
        if (cols == 0) {
            addRowsToZeroColumns(o.totalRows());
        } else {
            var dst = tail();
            for (; o != null; o = o.next) {
                o.requireUnpooled();
                for (short nr, or = 0, oRows = o.rows; or < oRows; or += nr) {
                    nr = (short) (dst.arr.length / cols - dst.rows);
                    if (nr <= 0)
                        nr = (short) ((dst = createTail()).arr.length / cols);
                    if (or + nr > oRows) nr = (short) (oRows - or);
                    dst.doAppend(o.arr, or, nr, cols);
                }
            }
        }
    }

    @Override public void append(TermBatch other) {
        short cols = this.cols;
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (rows == 0)
            other = copyFirstNodeToEmpty(other);
        TermBatch dst = tail(), src = other, prev = null;
        for (; src != null; src = (prev=src).next) {
            src.requireUnpooled();
            short dstPos = (short) (dst.rows*cols), srcRows = src.rows;
            if (dstPos + srcRows*cols > dst.arr.length)
                break;
            dst.doAppend(src.arr, 0, srcRows, cols);
        }
        if (src != null)
            other = appendRemainder(other, prev, src);
        TERM.recycle(other); // append() caller always gives ownership away
        assert validate() : "corrupted";
    }

    @Override public void putConverting(Batch<?> other) {
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        for (short oRows; other != null; other = other.next) {
            other.requireUnpooled();
            if ((oRows = other.rows) <= 0)
                continue; // skip empty batches
            for (int r = 0; r < oRows; r++) {
                beginPut();
                for (int c = 0; c < cols; c++)
                    putTerm(c, other.get(r, c));
                commitPut();
            }
        }
    }

    @Override public void putRowConverting(Batch<?> other, int row) {
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        beginPut();
        for (int c = 0; c < cols; c++)
            putTerm(c, other.get(row, c));
        commitPut();
    }

    @Override public void beginPut() {
        TermBatch tail = tail();
        short cols = tail.cols, begin = (short)(tail.rows*cols);
        int end = begin+cols;
        if (end > tail.arr.length) {
            tail  = createTail();
            begin = 0;
            end   = cols;
        }

        var arr = tail.arr;
        for (int i = begin; i < end; i++) arr[i] = null;
        tail.offerRowBase = begin;
    }

    @Override public void putTerm(int col, Term t) {
        var tail = this.tail;
        if (tail == null || tail.offerRowBase < 0) throw new IllegalStateException();
        if (col < 0 || col >= tail.cols) throw new IndexOutOfBoundsException();
        tail.arr[tail.offerRowBase+col] = t == null ? null : t.asImmutable();
    }

    @Override public void commitPut() {
        var tail = tail();
        if (tail.offerRowBase < 0) throw new IllegalStateException();
        ++tail.rows;
        tail.offerRowBase = -1;
        assert tail.validate();
    }

    @Override public void putRow(TermBatch other, int row) {
        short cols = this.cols;
        other.requireUnpooled();
        if (other.cols != cols) throw new IllegalArgumentException("other.cols != cols");
        if (row >= other.rows) throw new IndexOutOfBoundsException("row >= other.rows");

        var dst = tail();
        if ((dst.rows+1)*cols > dst.arr.length)
            dst = createTail();
        dst.doAppend(other.arr, row, (short)1, cols);
    }

    /* --- --- --- operation objects --- --- --- */

    @SuppressWarnings("UnnecessaryLocalVariable")
    public static final class Merger extends BatchMerger<TermBatch> {
        TermBatchType termBatchType;
        private final short outColumns;

        public Merger(BatchType<TermBatch> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            this.termBatchType = (TermBatchType) batchType;
            this.outColumns = (short)sources.length;
        }

        private TermBatch setupDst(TermBatch offer, boolean inplace) {
            int cols = outColumns;
            if (offer != null) {
                if (offer.rows == 0 || inplace)
                    offer.cols = (short)cols;
                else if (offer.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return offer;
            }
            return termBatchType.create(cols);
        }

        private TermBatch createTail(TermBatch root) {
            return root.setTail(termBatchType.create(outColumns));
        }

        private TermBatch mergeWithMissing(TermBatch dst, TermBatch left, int leftRow,
                                           TermBatch right) {
            TermBatch tail = dst.tail();
            Term[] lArr = left.arr;
            int l = leftRow*left.cols;
            for (int rows = right == null || right.rows == 0 ? 1 : right.totalRows(), nr
                 ; rows > 0; rows -= nr) {
                int d = tail.rows*tail.cols;
                if ((nr=(tail.termsCapacity()-d)/sources.length) <= 0) {
                    d = 0;
                    nr = (tail=createTail(dst)).termsCapacity()/sources.length;
                }
                Term[] dArr = tail.arr;
                tail.rows += (short)(nr = min(nr, rows));
                for (int e = d+nr*sources.length; d < e; d += sources.length) {
                    for (int c = 0, s; c < sources.length; c++)
                        dArr[d+c] = (s=sources[c]) > 0 ? lArr[l+s-1] : null;
                }
            }
            assert dst.validate();
            return dst;
        }

        @Override public TermBatch merge(@Nullable TermBatch dst, TermBatch left, int leftRow,
                                         @Nullable TermBatch right) {
            dst = setupDst(dst, false);
            if (sources.length == 0)
                return mergeThin(dst, right);
            if (right == null || right.rows*right.cols == 0)
                return mergeWithMissing(dst, left, leftRow, right);

            Term[] la = left.arr;
            short l = (short)(leftRow*left.cols), rc = right.cols;
            TermBatch tail = dst.tail();
            for (; right != null; right = right.next) {
                Term[] ra = right.arr, da = tail.arr;
                for (short rr=0, rRows=right.rows, nr; rr < rRows; rr += nr) {
                    if ((nr = (short)( da.length/sources.length - tail.rows )) <= 0) {
                        tail = createTail(dst);
                        nr = (short)( (da = tail.arr).length/sources.length );
                    }
                    if (rr+nr > rRows) nr = (short)(rRows-rr);
                    short d = (short)(tail.rows*tail.cols);
                    tail.rows += nr;
                    for (short r = (short)(rr*rc), re = (short)((rr+nr)*rc); r < re; r+=rc) {
                        for (int c = 0, s; c < sources.length; c++, ++d)
                            da[d] = (s=sources[c]) == 0 ? null : s > 0 ? la[l+s-1] : ra[r-s-1];
                    }
                }
            }
            assert dst.validate();
            return dst;
        }

        @Override public TermBatch project(TermBatch dst, TermBatch in) {
            short[] cols = this.columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            boolean inplace = dst == in;
            short ic = in.cols;
            dst = setupDst(dst, inplace);
            if (cols.length == 0)
                return mergeThin(dst, in);
            TermBatch tail = inplace ? dst : dst.tail();
            for (; in != null; in = in.next) {
                Term[] ia = in.arr, da;
                for (short ir = 0, iRows = in.rows, d, nr; ir < iRows; ir += nr) {
                    if (inplace) {
                        d         = 0;
                        da        = ia;
                        tail      = in;
                        tail.rows = nr = iRows;
                        tail.cols = (short)cols.length;
                    } else {
                        d = (short)(tail.rows*tail.cols);
                        if (d+tail.cols > (da=tail.arr).length) {
                            da = (tail = createTail(dst)).arr;
                            d = 0;
                        }
                        tail.rows += nr = (short)min((da.length-d)/cols.length, iRows-ir);
                    }
                    for (short i=(short)(ir*ic), ie=(short)((ir+nr)*ic); i < ie; i+=ic) {
                        for (int c = 0, src; c < cols.length; c++, ++d)
                            da[d] = (src=cols[c]) < 0 ? null : ia[i+src];
                    }
                }
            }
            assert dst.validate();
            return dst;
        }

        @Override public TermBatch projectInPlace(TermBatch b) {
            if (b == null || b.rows == 0 || outColumns == 0)
                return projectInPlaceEmpty(b);
            var dst = project(safeInPlaceProject ? b : null, b);
            if (dst != b)
                b.recycle();
            return dst;
        }

        @Override public TermBatch processInPlace(TermBatch b) { return projectInPlace(b); }

        @Override public @Nullable TermBatch onBatch(TermBatch batch) {
            if (batch == null) return null;
            int receivedRows = batch.totalRows();
            onBatchPrologue(batch);
            return onBatchEpilogue(projectInPlace(batch), receivedRows);
        }

        @Override public TermBatch projectRow(@Nullable TermBatch dst, TermBatch in, int row) {
            var cols = columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            TermBatch tail = (dst = setupDst(dst, false)).tail();
            int d = tail.rows*tail.cols;
            Term[] da = tail.arr, ia = in.arr;
            if (d+tail.cols > da.length) {
                da = (tail = createTail(dst)).arr;
                d = 0;
            }
            ++tail.rows;

            for (int c = 0, src, i = row*in.cols; c < cols.length; c++)
                da[d+c] = (src=cols[c]) < 0 ? null : ia[i+src];
            assert tail.validate();
            return dst;
        }

        @Override
        public TermBatch mergeRow(@Nullable TermBatch dst, TermBatch left, int leftRow, TermBatch right, int rightRow) {
            dst = setupDst(dst, false);
            TermBatch tail = dst.tailUnchecked();
            if (sources.length > 0) {
                Term[] dArr = tail.arr, lArr = left.arr, rArr = right.arr;
                int d = (short) (tail.rows * tail.cols);
                short l = (short) (leftRow * left.cols), r = (short) (rightRow * right.cols);
                if (d + tail.cols > dArr.length) {
                    dArr = (tail = createTail(dst)).arr;
                    d = 0;
                }
                for (int c = 0, s; c < sources.length; c++)
                    dArr[d++] = (s = sources[c]) > 0 ? lArr[l+s-1] : (s == 0 ? null : rArr[r-s-1]);
            }
            tail.rows++;
            assert tail.validate();
            return dst;
        }
    }

    public static final class Filter extends BatchFilter<TermBatch> {
        private final @Nullable Filter before;
        private final @Nullable Merger projector;

        public Filter(BatchType<TermBatch> batchType, Vars vars,
                      @Nullable Merger projector, RowFilter<TermBatch> rowFilter,
                      @Nullable BatchFilter<TermBatch> before) {
            super(batchType, vars, rowFilter, before);
            assert projector == null || projector.vars.equals(vars);
            this.projector = projector;
            this.before = (Filter)before;
        }

        @Override public TermBatch processInPlace(TermBatch b) { return filterInPlace(b); }

        @Override public @Nullable TermBatch onBatch(TermBatch batch) {
            if (batch == null) return null;
            int receivedRows = batch.totalRows();
            onBatchPrologue(batch);
            return onBatchEpilogue(filterInPlace(batch), receivedRows);
        }


        @Override public TermBatch filterInPlace(TermBatch in) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(in, in, in);
            var p = this.projector;
            if (p != null && rowFilter.targetsProjection()) {
                in = p.projectInPlace(in);
                p = null;
            }
            if (!rowFilter.isNoOp()) {
                TermBatch b = in, prev = in;
                short cols = in.cols;
                var decision = DROP;
                while (b != null) {
                    int rows = b.rows, d = 0;
                    Term[] arr = b.arr;
                    decision = DROP;
                    for (int r = 0; r < rows && decision != TERMINATE; r++) {
                        int start = r;
                        while (r < rows && (decision = rowFilter.drop(b, r)) == KEEP) ++r;
                        if (r > start) {
                            int n = (r-start)*cols, srcPos = start*cols;
                            arraycopy(arr, srcPos, arr, d, n);
                            d += n;
                        }
                    }
                    b.rows = (short) (d / cols);
                    if (d == 0 && b != in)  // remove b from linked list
                        b = filterInPlaceSkipEmpty(b, prev);
                    if (decision == TERMINATE) {
                        cancelUpstream();
                        if (b.next  != null) b.next = TERM.recycle(b.next);
                        if (in.rows ==    0) in     = TERM.recycle(in);
                    }
                    b = (prev = b).next;
                }
                in = filterInPlaceEpilogue(in, prev);
            }
            if (p != null && in != null && in.rows > 0)
                in = p.projectInPlace(in);
            return in;
        }
    }
}
