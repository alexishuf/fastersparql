package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.HANGMAN;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;

public abstract sealed class TermBatch extends Batch<TermBatch> {
    public static final int BYTES = 16
            + 2*2 /* rows, cols */
            + 2*4 /* tail+padding */
            + 4   /* arr */
            + 2   /* offerRowBase */
            + 2   /* padding */
            + PREFERRED_BATCH_TERMS*4;
    final FinalTerm[] arr;
    private short offerRowBase = -1;

    public FinalTerm[] arr() { return arr; }

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
    public TermBatch(FinalTerm[] arr, int rows, int cols) {
        super((short)rows, (short)cols);
        if (rows > Short.MAX_VALUE)
            throw new IllegalArgumentException("rows > Short.MAX_VALUE");
        this.arr = arr;
        if (arr.length < rows*cols)
            throw new IllegalArgumentException("arr.length < rows*cols");
        updateLeakDetectorRefCapacity();
        BatchEvent.Created.record(this);
    }

    protected final static class Concrete extends TermBatch implements Orphan<TermBatch> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
        public Concrete(FinalTerm[] arr, int rows, int cols) {
            super(arr, rows, cols);
        }
        @Override public TermBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable TermBatch recycle(Object currentOwner) {
        Object nodeOwner = currentOwner;
        for (TermBatch node = this, next; node != null; nodeOwner=node, node=next) {
            next = node.next;
            node.next = null;
            node.tail = node;
            BatchEvent.Pooled.record(node);
            if (node.rows <= 8 || node.arr.length != PREFERRED_BATCH_TERMS)
                node.doRecycle(nodeOwner);
            else
                TermBatchCleaner.INSTANCE.sched(node, nodeOwner); // async clean & pool.offer()

        }
        return  null;
    }

    private void doRecycle(Object currentOwner) {
        var arr = this.arr;
        if (arr.length == PREFERRED_BATCH_TERMS) {
            internalMarkRecycled(currentOwner);
            currentOwner = RECYCLED;
            clear();
            if (TERM.pool.offer(this) == null)
                return;
        }
        try {
            internalMarkGarbage(currentOwner);
        } catch (Throwable ignored) {assert false : "markGarbage() failed";}
    }

    void clearAndMarkRecycled(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        clear();
    }

    void doRecycleToShared(Object currentOwner) {
        var arr = this.arr;
        if (arr.length == PREFERRED_BATCH_TERMS) {
            internalMarkRecycled(currentOwner);
            currentOwner = RECYCLED;
            clear();
            if (TERM.pool.offerToShared(this) == null)
                return;
        }
        try {
            internalMarkGarbage(currentOwner);
        } catch (Throwable ignored) {assert false : "markGarbage() failed";}
    }

    private TermBatch createTail() {
        TermBatch tail = this.tail, b = TERM.create(cols).takeOwnership(tail);
        tail.tail = b;
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
    public static Orphan<TermBatch> rowMajor(List<? extends Term> lst, int rows, int cols) {
        int terms = rows*cols;
        if (lst.size() != terms)
            throw new IllegalArgumentException("lst.size() < terms");
        TermBatch b = TERM.create(cols).takeOwnership(lst);
        if (b.arr.length < terms) {
            b.recycle(lst);
            b = new TermBatch.Concrete(new FinalTerm[terms], rows, cols).takeOwnership(lst);
        }
        for (int i = 0; i < terms; i++)
            b.arr[i] = FinalTerm.asFinal(lst.get(i));
        return b.releaseOwnership(lst);
    }

    /** Create a {@link TermBatch} with a single row and {@code row.size()} columns. */
    @SafeVarargs
    public static Orphan<TermBatch> of(List<? extends Term>... rows) {
        int cols = rows.length == 0 ? 0 : rows[0].size();
        TermBatch b = TERM.create(cols).takeOwnership(rows);
        if (b.termsCapacity() < rows.length*cols) {
            b.recycle(rows);
            b = new TermBatch.Concrete(new FinalTerm[rows.length*cols], rows.length, cols)
                    .takeOwnership(rows);
        }
        for (List<? extends Term> row : rows)
            b.putRow(row);
        return b.releaseOwnership(rows);
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
        return isAliveOrNotMarking();
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public TermBatchType type()         { return TERM; }
    @Override public int           rowsCapacity() { return Math.min(Short.MAX_VALUE, arr.length/Math.max(1, cols)); }
    @Override public int          termsCapacity() { return Math.min(Short.MAX_VALUE, arr.length); }
    @Override public int     totalBytesCapacity() { return arr.length*4; }

    @Override public boolean hasCapacity(int terms, int localBytes) { return terms <= arr.length; }

    private void doAppend(FinalTerm[] arr, int row, short rowCount, short cols) {
        arraycopy(arr, row*cols, this.arr, this.rows*cols, rowCount*cols);
        this.rows += rowCount;
        assert validate();
    }

    @Override public Orphan<TermBatch> dup() {return dup((int)currentThread().threadId());}
    @Override public Orphan<TermBatch> dup(int threadId) {
        TermBatch b = TERM.createForThread(threadId, cols).takeOwnership(this);
        b.copy(this);
        return b.releaseOwnership(this);
    }

    @Override public Orphan<TermBatch> dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public Orphan<TermBatch> dupRow(int row, int threadId) {
        requireAlive();
        short cols = this.cols;
        TermBatch b = TERM.createForThread(threadId, cols).takeOwnership(this);
        b.doAppend(arr, row, (short)1, cols);
        return b.releaseOwnership(this);
    }

    /* --- --- --- term accessors --- --- --- */

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        requireAlive();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException();
        return arr[row * cols + col];
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        Term t = get(row, col);
        if (t == null) return false;
        dest.wrap(t.shared(), t.local(), t.sharedSuffixed());
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

    @Override public void clear() {
        if (rows > 0) {
            Arrays.fill(arr, 0, rows*cols, null);
            rows = 0;
        }
        tail = this;
        if (next != null)
            next = next.recycle(this);
    }

    @Override public TermBatch clear(int newColumns) {
        if (newColumns > arr.length)
            throw new IllegalArgumentException("newColumns too large");
        if (rows > 0) {
            Arrays.fill(arr, 0, rows*cols, null);
            rows = 0;
        }
        cols = (short)newColumns;
        tail = this;
        if (next != null)
            next = next.recycle(this);
        return this;
    }

    @Override public void abortPut() throws IllegalStateException {
        TermBatch tail = tail();
        if (tail.offerRowBase < 0) return;
        tail.offerRowBase = -1;
        dropEmptyTail();
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
            requireAlive();
            var dst = tail();
            for (; o != null; o = o.next) {
                o.requireAlive();
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

    @Override protected boolean copySingleNodeIfFast(TermBatch nonEmpty) {
        TermBatch tail = this.tail;
        short cols = nonEmpty.cols, rows = nonEmpty.rows;
        int dstPos = tail.rows*cols, nTerms = rows*cols;
        if (dstPos+nTerms > tail.termsCapacity())
            return false;
        arraycopy(nonEmpty.arr, 0, tail.arr, dstPos, nTerms);
        tail.rows += rows;
        return true;
    }

    @Override public void append(Orphan<TermBatch> orphan) {
        short cols = this.cols;
        if (peekColumns(orphan) != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (rows == 0)
            orphan = copyFirstNodeToEmpty(orphan);
        TermBatch dst = tail(), src = null;
        try {
            while (orphan != null) {
                src = orphan.takeOwnership(dst);
                orphan = null;
                short dstPos = (short) (dst.rows*cols), srcRows = src.rows;
                if (dstPos+srcRows*cols <= dst.arr.length) {
                    dst.doAppend(src.arr, 0, srcRows, cols);
                    orphan = src.detachHead();
                    src = src.recycle(dst);
                }
            }
            if (src != null)
                src = quickAppend0(src);
        } finally {
            if (src   != null)     src.recycle(dst);
            if (orphan != null) orphan.takeOwnership(HANGMAN).recycle(HANGMAN);
        }
        assert validate() : "corrupted";
    }

    @Override public void putConverting(Batch<?> other) {
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        for (short oRows; other != null; other = other.next) {
            other.requireAlive();
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
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        this.requireAlive();
        other.requireAlive();

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
        if (col < 0 || col >= tail.cols) throw new IndexOutOfBoundsException();
        tail.arr[tail.offerRowBase+col] = FinalTerm.asFinal(t);
    }

    @Override public void commitPut() {
        var tail = tail();
        if (tail.offerRowBase < 0) throw new IllegalStateException();
        ++tail.rows;
        tail.offerRowBase = -1;
        if (!tail.validate())
            System.out.println("##");
        assert tail.validate();
    }

    @Override public void putRow(TermBatch other, int row) {
        short cols = this.cols;
        other.requireAlive();
        if (other.cols != cols) throw new IllegalArgumentException("other.cols != cols");
        if (row >= other.rows) throw new IndexOutOfBoundsException("row >= other.rows");

        var dst = tail();
        if ((dst.rows+1)*cols > dst.arr.length)
            dst = createTail();
        dst.doAppend(other.arr, row, (short)1, cols);
    }

    /* --- --- --- operation objects --- --- --- */

    @SuppressWarnings("UnnecessaryLocalVariable")
    public static abstract sealed class Merger extends BatchMerger<TermBatch, Merger> {
        private final short outColumns;

        public Merger(BatchType<TermBatch> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            this.outColumns = (short)sources.length;
        }

        protected static final class Concrete extends Merger implements Orphan<Merger> {
            public Concrete(BatchType<TermBatch> batchType, Vars outVars, short[] sources) {
                super(batchType, outVars, sources);
            }
            @Override public Merger takeOwnership(Object o) {return takeOwnership0(o);}
        }

        private TermBatch setupDst(Orphan<TermBatch> offer, boolean inPlace) {
            int cols = outColumns;
            if (offer != null) {
                TermBatch b = offer.takeOwnership(this);
                if (b.rows == 0 || inPlace)
                    b.cols = (short)cols;
                else if (b.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return b;
            }
            return TERM.create(cols).takeOwnership(this);
        }

        private TermBatch createTail(TermBatch root) {
            return root.setTail(TERM.create(outColumns));
        }

        private Orphan<TermBatch> mergeWithMissing(TermBatch dst, TermBatch left, int leftRow,
                                                   TermBatch right) {
            TermBatch tail = dst.tail();
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
                        dArr[d+c] = (s=sources[c]) > 0 ? left.arr[l+s-1] : null;
                }
            }
            assert dst.validate();
            return dst.releaseOwnership(this);
        }

        @Override public Orphan<TermBatch> merge(@Nullable Orphan<TermBatch> dstOffer,
                                                 TermBatch left, int leftRow,
                                                 @Nullable TermBatch right) {
            var dst = setupDst(dstOffer, false);
            if (sources.length == 0)
                return mergeThin(dst, right).releaseOwnership(this);
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
            return dst.releaseOwnership(this);
        }

        @Override public Orphan<TermBatch> project(Orphan<TermBatch> dstOffer, TermBatch in) {
            if (dstOffer == in)
                return projectInPlace(dstOffer);
            return project0(setupDst(dstOffer, false), in, in.cols).releaseOwnership(this);
        }

        private TermBatch project0(TermBatch dst, TermBatch in, short ic) {
            short[] cols = this.columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            boolean inPlace = dst == in;
            if (cols.length == 0)
                return mergeThin(dst, in);
            TermBatch tail = inPlace ? dst : dst.tail();
            for (; in != null; in = in.next) {
                Term[] ia = in.arr, da;
                for (short ir = 0, iRows = in.rows, d, nr; ir < iRows; ir += nr) {
                    if (inPlace) {
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
            //noinspection DataFlowIssue
            assert dst.validate();
            return dst;
        }

        @Override public Orphan<TermBatch> projectInPlace(Orphan<TermBatch> orphan) {
            if (peekRows(orphan) == 0 || outColumns == 0)
                return projectInPlaceEmpty(orphan);
            short ic = peekColumns(orphan);
            TermBatch dst = setupDst(safeInPlaceProject ? orphan : null, safeInPlaceProject);
            TermBatch in = safeInPlaceProject ? dst : orphan.takeOwnership(this);
            try {
                return project0(dst, in, ic).releaseOwnership(this);
            } finally {
                if (in != dst) in.recycle(this);
            }
        }

        @Override public Orphan<TermBatch> processInPlace(Orphan<TermBatch> b) {
            return projectInPlace(b);
        }

        @Override public void onBatch(Orphan<TermBatch> orphan) {
            if (orphan != null) {
                int rcvRows = peekTotalRows(orphan);
                if (beforeOnBatch(orphan))
                    afterOnBatch(projectInPlace(orphan), rcvRows);
            }
        }

        @Override public void onBatchByCopy(TermBatch batch) {
            if (batch != null) {
                int rcvRows = batch.totalRows();
                if (beforeOnBatch(batch))
                    afterOnBatch(project(fillingBatch(), batch), rcvRows);
            }
        }

        @Override public Orphan<TermBatch> projectRow(@Nullable Orphan<TermBatch> dstOffer,
                                                      TermBatch in, int row) {
            var cols = columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            in.requireAlive();
            if (row >= in.rows)
                throw new IndexOutOfBoundsException(row);
            TermBatch dst = setupDst(dstOffer, false), tail = dst.tail();
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
            return dst.releaseOwnership(this);
        }

        @Override
        public Orphan<TermBatch>
        mergeRow(@Nullable Orphan<TermBatch> dstOffer, TermBatch left, int leftRow,
                 TermBatch right, int rightRow) {
            left.requireAlive();
            right.requireAlive();
            if (leftRow >= left.rows)
                throw new IndexOutOfBoundsException(leftRow);
            if (rightRow >= right.rows)
                throw new IndexOutOfBoundsException(rightRow);
            TermBatch dst = setupDst(dstOffer, false), tail = dst.tail;
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
            return dst.releaseOwnership(this);
        }
    }

    public static abstract sealed class Filter extends BatchFilter<TermBatch, Filter> {
        private final @Nullable Filter beforeFilter;
        private final @Nullable Merger projector;

        public Filter(BatchType<TermBatch> batchType, Vars vars,
                      @Nullable Orphan<Merger> projector,
                      Orphan<? extends RowFilter<TermBatch, ?>> rowFilter,
                      @Nullable Orphan<? extends BatchFilter<TermBatch, ?>> before) {
            super(batchType, vars, rowFilter, before);
            this.projector = Orphan.takeOwnership(projector, this);
            assert this.projector == null || this.projector.vars.equals(vars);
            this.beforeFilter = (Filter)this.before;
        }

        @Override protected void doRelease() {
            Owned.safeRecycle(projector, this);
            super.doRelease();
        }

        protected static final class Concrete extends Filter implements Orphan<Filter> {
            public Concrete(BatchType<TermBatch> batchType, Vars vars,
                            @Nullable Orphan<Merger> projector,
                            Orphan<? extends RowFilter<TermBatch, ?>> rowFilter,
                            @Nullable Orphan<? extends BatchFilter<TermBatch, ?>> before) {
                super(batchType, vars, projector, rowFilter, before);
            }
            @Override public Filter takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public Orphan<TermBatch> processInPlace(Orphan<TermBatch> b) {
            return filterInPlace(b);
        }

        @Override public void onBatch(Orphan<TermBatch> batch) {
            if (batch != null) {
                int rcvRows = peekTotalRows(batch);
                if (beforeOnBatch(batch))
                    afterOnBatch(filterInPlace(batch), rcvRows);
            }
        }

        @Override public Orphan<TermBatch> filterInPlace(Orphan<TermBatch> inOrphan) {
            if (beforeFilter != null)
                inOrphan = beforeFilter.filterInPlace(inOrphan);
            if (inOrphan == null)
                return null;
            var p = this.projector;
            if (p != null && rowFilter.targetsProjection()) {
                inOrphan = p.projectInPlace(inOrphan);
                p = null;
            }
            var in = inOrphan.takeOwnership(this);
            if (in.rows*outColumns == 0)
                return filterEmpty(in).releaseOwnership(this);
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
                        if (b.next  != null) b.next = b.next.recycle(b);
                        if (in.rows ==    0) in     = in.recycle(this);
                    }
                    b = (prev = b).next;
                }
                in = filterInPlaceEpilogue(in, prev);
            }
            var resultOrphan = Owned.releaseOwnership(in, this);
            if (p != null && in != null && in.rows > 0)
                resultOrphan = p.projectInPlace(resultOrphan);
            return resultOrphan;
        }
    }
}
