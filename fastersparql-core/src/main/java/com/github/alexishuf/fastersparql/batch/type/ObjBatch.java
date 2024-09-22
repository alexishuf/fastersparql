package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.HANGMAN;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;

public abstract class ObjBatch<B extends ObjBatch<B, T>, T> extends Batch<B> {
    public static final int BYTES = 16
            + 2*2 /* rows, cols */
            + 2*4 /* tail+padding */
            + 4   /* arr */
            + 2   /* offerRowBase */
            + 2   /* padding */
            + PREFERRED_BATCH_TERMS*4;
    final T[] arr;
    protected short offerRowBase = -1;

    public T[] arr() { return arr; }
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
    public ObjBatch(T[] arr, int rows, int cols) {
        super((short)rows, (short)cols);
        if (rows > Short.MAX_VALUE)
            throw new IllegalArgumentException("rows > Short.MAX_VALUE");
        this.arr = arr;
        if (arr.length < rows*cols)
            throw new IllegalArgumentException("arr.length < rows*cols");
        updateLeakDetectorRefCapacity();
        BatchEvent.Created.record(this);
    }

    @SuppressWarnings("unchecked")
    @Override public @Nullable B recycle(Object currentOwner) {
        Object nodeOwner = currentOwner;
        for (ObjBatch<B, T> node = this, next; node != null; nodeOwner=node, node=next) {
            next = node.next;
            node.next = null;
            node.tail = (B)node;
            BatchEvent.Pooled.record(node);
            if (node.rows <= 8 || node.arr.length != PREFERRED_BATCH_TERMS)
                node.doRecycle(nodeOwner);
            else
                schedNodeCleanup((B)node, nodeOwner);
        }
        return  null;
    }

    protected abstract void schedNodeCleanup(B node, Object nodeOwner);

    private void doRecycle(Object currentOwner) {
        var arr = this.arr;
        if (arr.length == PREFERRED_BATCH_TERMS) {
            internalMarkRecycled(currentOwner);
            currentOwner = RECYCLED;
            clear();
            //noinspection unchecked
            if (type().pool.offer((B)this) == null)
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
            //noinspection unchecked
            if (type().pool.offerToShared((B)this) == null)
                return;
        }
        try {
            internalMarkGarbage(currentOwner);
        } catch (Throwable ignored) {assert false : "markGarbage() failed";}
    }

    private B createTail() { return setTail(type().create(cols)); }

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

    @Override public int       rowsCapacity() { return (short)Math.min(Short.MAX_VALUE, arr.length/Math.max(1, cols)); }
    @Override public int      termsCapacity() { return Math.min(Short.MAX_VALUE, arr.length); }
    @Override public int totalBytesCapacity() { return arr.length*4; }

    @Override public boolean hasCapacity(int terms, int localBytes) { return terms <= arr.length; }

    private void doAppend(T[] arr, int row, short rowCount, short cols) {
        arraycopy(arr, row*cols, this.arr, this.rows*cols, rowCount*cols);
        this.rows += rowCount;
        assert validate();
    }

    @Override public Orphan<B> dup() {return dup((int)currentThread().threadId());}
    @Override public Orphan<B> dup(int threadId) {
        B b = type().createForThread(threadId, cols).takeOwnership(this);
        //noinspection unchecked
        b.copy((B)this);
        return b.releaseOwnership(this);
    }

    @Override public Orphan<B> dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public Orphan<B> dupRow(int row, int threadId) {
        requireAlive();
        short cols = this.cols;
        ObjBatch<B, T> b = type().createForThread(threadId, cols).takeOwnership(this);
        b.doAppend(arr, row, (short)1, cols);
        return b.releaseOwnership(this);
    }

    /* --- --- --- term accessors --- --- --- */

    public @Nullable T obj(@NonNegative int row, @NonNegative int col) {
        requireAlive();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException();
        return arr[row * cols + col];
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void clear() {
        if (rows > 0) {
            Arrays.fill(arr, 0, rows*cols, null);
            rows = 0;
        }
        //noinspection unchecked
        tail = (B)this;
        if (next != null)
            next = next.recycle(this);
    }

    @Override public B clear(int newColumns) {
        if (newColumns > arr.length)
            throw new IllegalArgumentException("newColumns too large");
        if (rows > 0) {
            Arrays.fill(arr, 0, rows*cols, null);
            rows = 0;
        }
        cols = (short)newColumns;
        @SuppressWarnings("unchecked") B self = (B)this;
        tail = self;
        if (next != null)
            next = next.recycle(this);
        return self;
    }

    @Override public void abortPut() throws IllegalStateException {
        ObjBatch<B, T> tail = tail();
        if (tail.offerRowBase < 0) return;
        tail.offerRowBase = -1;
        dropEmptyTail();
    }

    @Override public void copy(B o) {
        short cols = this.cols;
        if (o.cols != cols)
            throw new IllegalArgumentException("other.cols != cols");
        if (cols == 0) {
            addRowsToZeroColumns(o.totalRows());
        } else {
            requireAlive();
            ObjBatch<B, T> dst = tail();
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

    @Override public void append(Orphan<B> orphan) {
        short cols = this.cols;
        if (peekColumns(orphan) != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (rows == 0)
            orphan = copyFirstNodeToEmpty(orphan);
        ObjBatch<B, T> dst = tail();
        B src = null;
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
                src = setTailAndReturnNull(src);
        } finally {
            if (src   != null)     src.recycle(dst);
            if (orphan != null) orphan.takeOwnership(HANGMAN).recycle(HANGMAN);
        }
        assert validate() : "corrupted";
    }

    @Override public void deFragmentMiddleNodes() {
        B prev = next, tail = this.tail, n;
        if (prev == null || (n=prev.next) == tail || n == null)
            return;
        short cols = this.cols, dstPos = (short)(prev.rows*cols);
        short nRows, prevCapacity = (short)prev.termsCapacity();
        while ((n=prev.next) != tail && n != null) {
            short nTerms = (short)((nRows=n.rows)*cols);
            if (dstPos+nTerms <= prevCapacity) {
                arraycopy(n.arr, 0, prev.arr, dstPos, nTerms);
                prev.rows += nRows;
                prev.next  = n.dropHead(prev);
                dstPos    += nTerms;
            } else {
                prev         = n;
                prevCapacity = (short)prev.termsCapacity();
                dstPos       = (short)(prev.rows*cols);
            }
        }
    }

    protected abstract void putTermConverting(int dstCol, Batch<?> other, int row, int col);

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
                    putTermConverting(c, other, r, c);
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
            putTermConverting(c, other, row, c);
        commitPut();
    }

    @Override public void beginPut() {
        ObjBatch<B, T> tail = tail();
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

    public void putTerm(int col, T t) {
        ObjBatch<B, T> tail = this.tail;
        if (col < 0 || col >= tail.cols) throw new IndexOutOfBoundsException();
        tail.arr[tail.offerRowBase+col] = t;
    }

    @Override public void putNullTerm(int col) {
        ObjBatch<B, T> tail = this.tail;
        if (col < 0 || col >= tail.cols) throw new IndexOutOfBoundsException();
        tail.arr[tail.offerRowBase+col] = null;
    }

    @Override public void commitPut() {
        var tail = tail();
        if (tail.offerRowBase < 0) throw new IllegalStateException();
        ++tail.rows;
        tail.offerRowBase = -1;
        assert tail.validate();
    }

    public final @This B putRow(T[] terms, int offset) {
        beginPut();
        for (short c = 0, cols = this.cols; c < cols; c++)
            putTerm(c, terms[offset+c]);
        commitPut();
        //noinspection unchecked
        return (B)this;
    }

    @Override public void putRow(B other, int row) {
        short cols = this.cols;
        other.requireAlive();
        if (other.cols != cols) throw new IllegalArgumentException("other.cols != cols");
        if (row >= other.rows) throw new IndexOutOfBoundsException("row >= other.rows");

        ObjBatch<B, T> dst = tail();
        if ((dst.rows+1)*cols > dst.arr.length)
            dst = createTail();
        dst.doAppend(other.arr, row, (short)1, cols);
    }

    /* --- --- --- operation objects --- --- --- */

    @SuppressWarnings("UnnecessaryLocalVariable")
    public static abstract sealed class Merger<T, B extends ObjBatch<B, T>>
            extends BatchMerger<B, Merger<T, B>> {
        private final short outColumns;

        public Merger(BatchType<B> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            this.outColumns = (short)sources.length;
        }

        protected static final class Concrete<T, B extends ObjBatch<B, T>>
                extends Merger<T, B>
                implements Orphan<Merger<T, B>> {
            public Concrete(BatchType<B> batchType, Vars outVars, short[] sources) {
                super(batchType, outVars, sources);
            }
            @Override public Merger<T, B> takeOwnership(Object o) {
                return takeOwnership0(o);
            }
        }

        private B setupDst(Orphan<B> offer, boolean inPlace) {
            int cols = outColumns;
            if (offer != null) {
                B b = offer.takeOwnership(this);
                if (b.rows == 0 || inPlace)
                    b.cols = (short)cols;
                else if (b.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return b;
            }
            return batchType.create(cols).takeOwnership(this);
        }

        private B createTail(B root) {
            return root.setTail(batchType.create(outColumns));
        }

        private Orphan<B> mergeWithMissing(B dst, B left, int leftRow,
                                                   B right) {
            B tail = dst.tail();
            int l = leftRow*left.cols;
            for (int rows = right == null || right.rows == 0 ? 1 : right.totalRows(), nr
                 ; rows > 0; rows -= nr) {
                int d = tail.rows*tail.cols;
                if ((nr=(tail.termsCapacity()-d)/sources.length) <= 0) {
                    d = 0;
                    nr = (tail=createTail(dst)).termsCapacity()/sources.length;
                }
                T[] dArr = tail.arr;
                tail.rows += (short)(nr = min(nr, rows));
                for (int e = d+nr*sources.length; d < e; d += sources.length) {
                    for (int c = 0, s; c < sources.length; c++)
                        dArr[d+c] = (s=sources[c]) > 0 ? left.arr[l+s-1] : null;
                }
            }
            assert dst.validate();
            return dst.releaseOwnership(this);
        }

        @Override public Orphan<B> merge(@Nullable Orphan<B> dstOffer,
                                                 B left, int leftRow,
                                                 @Nullable B right) {
            var dst = setupDst(dstOffer, false);
            if (sources.length == 0)
                return mergeThin(dst, right).releaseOwnership(this);
            if (right == null || right.rows*right.cols == 0)
                return mergeWithMissing(dst, left, leftRow, right);

            T[] la = left.arr;
            short l = (short)(leftRow*left.cols), rc = right.cols;
            B tail = dst.tail();
            for (; right != null; right = right.next) {
                T[] ra = right.arr, da = tail.arr;
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

        @Override public Orphan<B> project(Orphan<B> dstOffer, B in) {
            if (dstOffer == in)
                return projectInPlace(dstOffer);
            return project0(setupDst(dstOffer, false), in, in.cols).releaseOwnership(this);
        }

        private B project0(B dst, B in, short ic) {
            short[] cols = this.columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            boolean inPlace = dst == in;
            if (cols.length == 0)
                return mergeThin(dst, in);
            B tail = inPlace ? dst : dst.tail();
            for (; in != null; in = in.next) {
                T[] ia = in.arr, da;
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

        @Override public Orphan<B> projectInPlace(Orphan<B> orphan) {
            if (peekRows(orphan) == 0 || outColumns == 0)
                return projectInPlaceEmpty(orphan);
            short ic = peekColumns(orphan);
            B dst = setupDst(safeInPlaceProject ? orphan : null, safeInPlaceProject);
            B in = safeInPlaceProject ? dst : orphan.takeOwnership(this);
            try {
                return project0(dst, in, ic).releaseOwnership(this);
            } finally {
                if (in != dst) in.recycle(this);
            }
        }

        @Override public Orphan<B> processInPlace(Orphan<B> b) {
            return projectInPlace(b);
        }

        @Override public void onBatch(Orphan<B> orphan) {
            if (orphan != null) {
                int rcvRows = peekTotalRows(orphan);
                if (beforeOnBatch(orphan))
                    afterOnBatch(projectInPlace(orphan), rcvRows);
            }
        }

        @Override public void onBatchByCopy(B batch) {
            if (batch != null) {
                int rcvRows = batch.totalRows();
                if (beforeOnBatch(batch))
                    afterOnBatch(project(fillingBatch(), batch), rcvRows);
            }
        }
    }

    public static abstract sealed class Filter<T, B extends ObjBatch<B, T>>
            extends BatchFilter<B, ObjBatch.Filter<T, B>> {
        private final @Nullable Filter<T, B> beforeFilter;
        private final @Nullable Merger<T, B> projector;

        public Filter(ObjBatchType<T, B> batchType, Vars vars,
                      @Nullable Orphan<Merger<T, B>> projector,
                      Orphan<? extends RowFilter<B, ?>> rowFilter,
                      @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
            super(batchType, vars, rowFilter, before);
            this.projector = Orphan.takeOwnership(projector, this);
            assert this.projector == null || this.projector.vars.equals(vars);
            //noinspection unchecked
            this.beforeFilter = (Filter<T, B>)this.before;
        }

        @Override protected void doRelease() {
            Owned.safeRecycle(projector, this);
            super.doRelease();
        }

        protected static final class Concrete<T, B extends ObjBatch<B, T>>
                extends Filter<T, B> implements Orphan<Filter<T, B>> {
            public Concrete(ObjBatchType<T, B> batchType, Vars vars,
                            @Nullable Orphan<Merger<T, B>> projector,
                            Orphan<? extends RowFilter<B, ?>> rowFilter,
                            @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
                super(batchType, vars, projector, rowFilter, before);
            }
            @Override public Filter<T, B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public Orphan<B> processInPlace(Orphan<B> b) {
            return filterInPlace(b);
        }

        @Override public void onBatch(Orphan<B> batch) {
            if (batch != null) {
                int rcvRows = peekTotalRows(batch);
                if (beforeOnBatch(batch))
                    afterOnBatch(filterInPlace(batch), rcvRows);
            }
        }

        @Override public Orphan<B> filterInPlace(Orphan<B> inOrphan) {
            if (beforeFilter != null)
                inOrphan = beforeFilter.filterInPlace(inOrphan);
            if (inOrphan == null)
                return null;
            var p = this.projector;
            if (p != null && rowFilter.targetsProjection()) {
                inOrphan = p.projectInPlace(inOrphan);
                p = null;
            }
            var filtered = inOrphan.takeOwnership(this);
            if (filtered.rows*outColumns == 0)
                return filterEmpty(filtered).releaseOwnership(this);
            if (!rowFilter.isNoOp()) {
                var next     = filtered;
                short cols   = filtered.cols, rows;
                var decision = DROP;
                filtered     = null;
                while (next != null) {
                    var b    = next;
                    next     = Orphan.takeOwnership(next.detachHead(), this);
                    rows     = b.rows;
                    decision = DROP;
                    int d    = 0;
                    for (int r = 0; r < rows && decision != TERMINATE; r++) {
                        int start = r;
                        while (r < rows && (decision = rowFilter.drop(b, r)) == KEEP) ++r;
                        if (r > start) {
                            int n = (r-start)*cols, srcPos = start*cols;
                            arraycopy(b.arr, srcPos, b.arr, d, n);
                            d += n;
                        }
                    }
                    b.rows = (short) (d / cols);
                    if      (d == 0)           b.recycle(this);
                    else if (filtered == null) filtered = b;
                    else                       filtered.setTail(b.releaseOwnership(this));
                    if (decision == TERMINATE) {
                        cancelUpstream();
                        next = Batch.safeRecycle(next, this);
                    }
                }
                assert filtered == null || filtered.validate() : "corrupted";
                if (filtered == null && decision != TERMINATE)
                    filtered = batchType.create(outColumns).takeOwnership(this);
            }
            var resultOrphan = Owned.releaseOwnership(filtered, this);
            if (p != null && filtered != null && filtered.rows > 0)
                resultOrphan = p.projectInPlace(resultOrphan);
            return resultOrphan;
        }
    }
}
