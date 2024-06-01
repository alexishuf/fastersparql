package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.LONG;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.HANGMAN;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.onSpinWait;

public abstract class IdBatch<B extends IdBatch<B>> extends Batch<B> {
    public static final int BYTES = 16 /* obj header */
            + 2*2 /* rows, cols */
            + 2*4 /* tail+padding */
            + 2*4 /* arr, cachedTerm */
            + 3*2 /* short fields */
            + 2   /* padding */
            + IdBatchType.PREFERRED_BATCH_TERMS*8;
    private static final VarHandle CACHED_LOCK;
    static {
        try {
            CACHED_LOCK = MethodHandles.lookup().findVarHandle(IdBatch.class, "plainCachedLock", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public    final  long[] arr;
    public    final short   termsCapacity;
    protected       short   offerRowBase = -1;
    @SuppressWarnings("unused") // access through CACHED_LOCK
    private boolean plainCachedLock;
    private short cachedAddr = -1;
    private FinalTerm cachedTerm;

    protected IdBatch(long[] ids, short cols) {
        super((short)0, cols);
        if (ids.length < cols)
            throw new IllegalArgumentException("ids.length < cols");
        this.arr           = ids;
        this.termsCapacity = (short)arr.length;
        updateLeakDetectorRefCapacity();
    }

    @SuppressWarnings("unchecked") @Override public @Nullable B recycle(Object currentOwner) {
        IdBatchType<B> type = idType();
        Object nodeOwner = currentOwner;
        for (B node = (B)this, next; node != null; nodeOwner=node, node=next) {
            node.internalMarkRecycled(nodeOwner);
            next = node.next;
            node.next = null;
            node.tail = node;
            BatchEvent.Pooled.record(node);
            if (type.pool.offer(node) != null) {
                try {
                    node.internalMarkGarbage(RECYCLED);
                } catch (Throwable ignored) { assert false : "markGarbage() failed"; }
            }
        }
        return null;
    }

    @Override protected @Nullable B internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        LONG.offer(arr, termsCapacity);
        return null;
    }

    @Override protected boolean validateNode(Validation validation) {
        if (!SELF_VALIDATE || validation == Validation.NONE)
            return true;
        if (termsCapacity != arr.length)
            return false;
        return super.validateNode(validation);
    }

    /* --- --- --- batch accessors --- --- --- */

    public long[] arr() { return arr; }

    @Override public int       rowsCapacity() { return cols == 0 ? Integer.MAX_VALUE : termsCapacity/cols; }
    @Override public int      termsCapacity() { return termsCapacity; }
    @Override public int totalBytesCapacity() { return termsCapacity*8; }

    @Override public boolean hasCapacity(int terms, int local) { return terms <= termsCapacity; }

    @Override public final BatchType<B> type() { return idType(); }

    public abstract IdBatchType<B> idType();

    /* --- --- --- helpers --- --- --- */

    protected FinalTerm cachedTerm(int address) {
        // try returning a cached value
        int cachedAddr;
        FinalTerm cachedTerm;
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, false, true)) onSpinWait();
        try {
            cachedAddr = this.cachedAddr;
            cachedTerm = this.cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, false); }
        return cachedAddr == address ? cachedTerm : null;
    }

    protected void cacheTerm(int address, FinalTerm term) {
        while (CACHED_LOCK.weakCompareAndSetAcquire(this, false, true)) onSpinWait();
        try {
            this.cachedAddr = (short)address;
            this.cachedTerm = term;
        } finally { CACHED_LOCK.setRelease(this, false); }
    }


    protected B createTail() {return setTail(idType().create(cols));}

    /* --- --- --- term-level access --- --- --- */

    public long id(int row, int col) {
        requireAlive();
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException("(row, col) out of bounds");
        return arr[row * cols + col];
    }

    public long linkedId(int row, int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.id(rel, col);
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col, B other, int oRow, int oCol) {
        return equals(row, col, other.id(oRow, oCol));
    }

    @Override public boolean equals(int row, B other, int oRow) {
        short cols = this.cols;
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (oRow < 0 || oRow >= other.rows)
            throw new IndexOutOfBoundsException(oRow);
        return equals(row, other.arr, cols*oRow);
    }

    public abstract boolean equals(@NonNegative int row, long[] ids, int idsOffset);
    public abstract boolean equals(@NonNegative int row, @NonNegative int col, long id);

    /* --- --- --- mutators --- --- --- */

    @SuppressWarnings("unchecked") @Override public final void clear() {
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
        this.tail       = (B)this;
        if (next != null)
            next.recycle(this);
    }

    @SuppressWarnings("unchecked") @Override public final @This B clear(int cols) {
        if (cols > termsCapacity)
            throw new IllegalArgumentException("cols too large");
        if (next != null)
            next = next.recycle(this);
        this.cols       = (short)cols;
        this.rows       =  0;
        this.cachedAddr = -1;
        this.cachedTerm = null;
        this.tail       = (B)this;
        return (B)this;
    }

    @Override public void abortPut() throws IllegalStateException {
        B tail = tail();
        tail.requireAlive();
        if (tail.offerRowBase < 0) return;
        tail.offerRowBase = -1;
        dropEmptyTail();
        assert validate();
    }


    protected final void doPut(B src, int srcPos, int dstPos, short nRows, short cols) {
        int nTerms = nRows*cols;
        arraycopy(src.arr,    srcPos, arr,    dstPos, nTerms);
        rows += nRows;
        assert validate();
    }

    @Override public final void copy(B o) {
        short cols = this.cols;
        if (o.cols != cols) {
            throw new IllegalArgumentException("cols mismatch");
        } else if (cols == 0) {
            addRowsToZeroColumns(o.totalRows());
        } else {
            B dst = tail();
            for (; o != null; o = o.next) {
                o.requireAlive();
                for (short nr, or = 0, oRows = o.rows; or < oRows; or += nr) {
                    nr = (short) (dst.termsCapacity / cols - dst.rows);
                    if (nr <= 0)
                        nr = (short) ((dst = createTail()).termsCapacity / cols);
                    if (or + nr > oRows) nr = (short) (oRows - or);
                    dst.doPut(o, or * cols, dst.rows * cols, nr, cols);
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
        B dst = tail(), src = null;
        try {
            while (orphan != null) {
                src = orphan.takeOwnership(dst);
                orphan = null;
                short dstPos = (short)(dst.rows*cols), srcRows = src.rows;
                if (dstPos + (src.rows)*cols <= dst.termsCapacity) {
                    dst.doPut(src, 0, dstPos, srcRows, cols); // copy contents
                    orphan = src.detachHead();
                    src = src.recycle(dst);
                }
            }
            if (src != null)
                src = setTailAndReturnNull(src);
        } finally {
            if (   src != null)    src.recycle(dst);
            if (orphan != null) orphan.takeOwnership(HANGMAN).recycle(HANGMAN);
        }
        assert validate();
    }

    @Override public boolean deFragmentMiddleNodes() {
        B prev = next, tail = this.tail, n;
        if (prev == null || (n=prev.next) == tail || n == null)
            return false;
        short cols = this.cols, dstPos = (short)(prev.rows*cols);
        short nRows;
        while ((n=prev.next) != tail && n != null) {
            int nTerms = (short)((nRows=n.rows)*cols);
            if (dstPos+nTerms <= prev.termsCapacity) {
                arraycopy(n.arr, 0, prev.arr, dstPos, nTerms);
                prev.rows += nRows;
                prev.next  = n.dropHead(prev);
                return true;
            } else {
                prev = n;
                dstPos = (short)(prev.rows*cols);
            }
        }
        return false;
    }

    @Override public final void beginPut() {
        B dst = tail();
        short cols = dst.cols, begin = (short)(dst.rows*cols);
        int end = begin+cols;
        if (end > dst.termsCapacity) {
            dst = createTail();
            begin = 0;
            end = cols;
        }
        long[] arr    = dst.arr;
        for (int i = begin; i < end; i++)     arr[i] = 0;
        dst.offerRowBase = begin;
    }

    @Override public void putTerm(int destCol, Term t) {
        B tail = tail();
        if (tail.offerRowBase < 0)
            throw new IllegalStateException();
        if (t != null)
            throw new UnsupportedOperationException("Cannot put non-null terms. Use putConverting(Batch, int) instead");
        tail.doPutTerm(destCol, 0);
    }

    @Override public void putNullTerm(int col) {
        B tail = this.tail;
        if (tail.offerRowBase < 0)
            throw new IllegalStateException();
        tail.doPutTerm(col, 0);
    }

    @Override public final void putTerm(int d, B b, int r, int c) {
        B dst = tail();
        if (dst.offerRowBase < 0) throw new IllegalStateException();
        int oCols = b.cols;
        if (r < 0 || c < 0 || r >= b.rows || c >= oCols || d < 0 || d >= dst.cols)
            throw new IndexOutOfBoundsException();
        dst.arr[dst.offerRowBase+ d] = b.arr[r *oCols + c];
    }

    public final void putTerm(int col, long sourcedId) {
        B dst = tail();
        if (dst.offerRowBase < 0)      throw new IllegalStateException();
        if (col < 0 || col > dst.cols) throw new IndexOutOfBoundsException();
        dst.arr[dst.offerRowBase+col] = sourcedId;
    }

    public final void doPutTerm(int col, long sourcedId) {
        arr[offerRowBase+col] = sourcedId;
    }

    @Override public final void commitPut() {
        B dst = tail();
        if (dst.offerRowBase < 0) throw new IllegalStateException();
        ++dst.rows;
        dst.offerRowBase = -1;
        assert validate();
    }

    public final void putRow(long[] ids, int offset) {
        var tail = this.tail;
        short cols = this.cols, rows = tail.rows;
        int dst = rows*cols;
        if (dst+cols > tail.termsCapacity) {
            tail = createTail();
            dst = rows =0;
        }
        tail.rows = (short)(rows+1);
        arraycopy(ids, offset, tail.arr, dst, cols);
    }

    @Override public final void putRow(B other, int row) {
        short cols = this.cols;
        other.requireAlive();
        if (other.cols != cols) throw new IllegalArgumentException("other.cols != cols");
        if (row >= other.rows)
            throw new IndexOutOfBoundsException("row >= other.rows");

        B dst = tail();
        int dstPos = dst.rows*cols;
        if (dstPos + cols > dst.termsCapacity) {
            dst = createTail();
            dstPos = 0;
        }
        dst.doPut(other, row*cols, dstPos, (short)1, cols);
    }

    public final void putConverting(Batch<?> other) {
        if (other.type() == type()) {//noinspection unchecked
            copy((B)other);
            return;
        }
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        try (var t = PooledTermView.ofEmptyString()) {
            for (short oRows; other != null; other = other.next) {
                other.requireAlive();
                if ((oRows = other.rows) <= 0)
                    continue; // skip empty batches
                for (int r = 0; r < oRows; r++) {
                    beginPut();
                    for (int c = 0; c < cols; c++) {
                        if (other.getView(r, c, t))
                            putTerm(c, t);
                    }
                    commitPut();
                }
            }
        }
    }

    @Override public final void putRowConverting(Batch<?> other, int row) {
        short cols = this.cols;
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        this .requireAlive();
        other.requireAlive();

        try (var t = PooledTermView.ofEmptyString()) {
            beginPut();
            for (int c = 0; c < cols; c++) {
                if (other.getView(row, c, t))
                    putTerm(c, t);
            }
            commitPut();
        } catch (Throwable t) {
            abortPut();
            throw t;
        }
    }

    /* --- --- --- operation objects --- --- --- */

    public static abstract sealed class Merger<B extends IdBatch<B>>
            extends BatchMerger<B, Merger<B>> {
        private final IdBatchType<B> idBatchType;
        private final short outColumns;

        public Merger(BatchType<B> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            this.idBatchType = (IdBatchType<B>)batchType;
            this.outColumns  = (short)sources.length;
        }

        protected static final class Concrete<B extends IdBatch<B>>
                extends Merger<B> implements Orphan<Merger<B>> {
            public Concrete(BatchType<B> batchType, Vars outVars, short[] sources) {
                super(batchType, outVars, sources);
            }
            @Override public Merger<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        private B setupDst(Orphan<B> offer, boolean inplace) {
            int cols = outColumns;
            if (offer != null) {
                B b = offer.takeOwnership(this);
                if (b.rows == 0 || inplace)
                    b.cols = (short)cols;
                else if (b.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return b;
            }
            return idBatchType.create(cols).takeOwnership(this);
        }

        private B createTail(B root) {
            return root.setTail(idBatchType.create(outColumns));
        }

        protected Orphan<B> mergeWithMissing(B dst, B left, int leftRow, B right) {
            int l = leftRow*left.cols;
            B tail = dst.tail();
            for (int rows = right == null || right.rows == 0 ? 1 : right.totalRows(), nr
                 ; rows > 0; rows -= nr) {
                int d = tail.rows*tail.cols;
                if ((nr=(tail.termsCapacity-d)/sources.length) <= 0) {
                    nr = (tail=createTail(dst)).termsCapacity/sources.length;
                    d = 0;
                }
                long[] dIds    = tail.arr;
                tail.rows += (short)(nr=min(nr, rows));
                for (int e = d+nr*sources.length; d < e; d += sources.length) {
                    for (int c = 0, s; c < sources.length; c++) {
                        if ((s=sources[c]) > 0)
                            dIds[d+c] = left.arr[l+(--s)];
                        else
                            dIds[d+c] = 0L;
                    }
                }
            }
            assert tail.validate();
            return dst.releaseOwnership(this);
        }

        @SuppressWarnings("UnnecessaryLocalVariable")
        @Override public final Orphan<B> merge(@Nullable Orphan<B> dstOffer, B left, int leftRow,
                                               @Nullable B right) {
            var dst = setupDst(dstOffer, false);
            if (sources.length == 0)
                return mergeThin(dst, right).releaseOwnership(this);
            if (right == null || right.rows*right.cols == 0)
                return mergeWithMissing(dst, left, leftRow, right);

            long[] lIds    = left.arr;
            short l = (short)(leftRow*left.cols), rc = right.cols;
            B tail = dst.tail();
            for (; right != null; right = right.next) {
                long[] rIds    = right.arr;
                for (short rr = 0, rRows = right.rows, nr; rr < rRows; rr += nr) {
                    if ((nr=(short)(tail.termsCapacity/sources.length-tail.rows)) <= 0) {
                        tail = createTail(dst);
                        nr = (short)(tail.termsCapacity/sources.length);
                    }
                    nr = (short)min(nr, rRows-rr);
                    short d = (short)(tail.rows*tail.cols);
                    tail.rows += nr;
                    long[] dIds    = tail.arr;
                    for (short r = (short)(rr*rc), re = (short)((rr+nr)*rc); r < re; r+=rc) {
                        for (int c = 0, src; c < sources.length; c++, ++d) {
                            if      ((src = sources[c]) == 0) dIds[d] = 0L;
                            else if (src > 0)                 dIds[d] = lIds[l + src - 1];
                            else                              dIds[d] = rIds[r - src - 1];
                        }
                    }
                }
            }
            assert dst.validate();
            return dst.releaseOwnership(this);
        }

        @Override public final Orphan<B> project(Orphan<B> dst, B in) {
            if (dst == in)
                return projectInPlace(dst);
            return project0(setupDst(dst, false), in, in.cols).releaseOwnership(this);
        }

        private B project0(B dst, B in, short ic) {
            short[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            boolean inPlace = dst == in;
            if (cols.length == 0)
                return mergeThin(dst, in);
            B tail = inPlace ? dst : dst.tail();
            for (; in != null; in = in.next) {
                long[] dIds;
                for (short ir = 0, iRows = in.rows, d, nr; ir < iRows; ir += nr) {
                    if (inPlace) {
                        d              = 0;
                        (tail=in).rows = nr = iRows;
                        tail.cols      = (short)cols.length;
                    } else {
                        d = (short)(tail.rows*tail.cols);
                        if (d+tail.cols > tail.termsCapacity) {
                            tail = createTail(dst);
                            d = 0;
                        }
                        tail.rows += nr = (short)min((tail.termsCapacity-d)/cols.length, iRows-ir);
                    }
                    dIds = tail.arr;
                    for (short i=(short)(ir*ic), ie=(short)((ir+nr)*ic); i < ie; i+=ic) {
                        for (int c = 0, src; c < cols.length; c++, ++d)
                            dIds[d] = (src = cols[c]) < 0 ? 0L : in.arr[i + src];
                    }
                }
            }
            assert dst != null;
            assert dst.validate();
            return dst;
        }

        @Override public final Orphan<B> projectInPlace(Orphan<B> orphan) {
            if (peekRows(orphan) == 0 || outColumns == 0)
                return projectInPlaceEmpty(orphan);
            short ic = peekColumns(orphan);
            var dst = setupDst(safeInPlaceProject ? orphan : null, safeInPlaceProject);
            var in = safeInPlaceProject ? dst : orphan.takeOwnership(this);
            try {
                return project0(dst, in, ic).releaseOwnership(this);
            } finally {
                if (in != dst) in.recycle(this);
            }
        }

        @Override public Orphan<B> processInPlace(Orphan<B> b) { return projectInPlace(b); }

        @Override public void onBatch(Orphan<B> batch) {
            if (batch != null) {
                int rcvRows = peekTotalRows(batch);
                if (beforeOnBatch(batch))
                    afterOnBatch(projectInPlace(batch), rcvRows);
            }
        }
        @Override public void onBatchByCopy(B batch) {
            if (batch != null) {
                int rcvRows = batch.totalRows();
                if (beforeOnBatch(batch))
                    afterOnBatch(project(fillingBatch(), batch), rcvRows);
            }
        }

        @Override public final Orphan<B> projectRow(@Nullable Orphan<B> dstOffer, B in, int row) {
            var cols = columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            in.requireAlive();
            if (row >= in.rows)
                throw new IndexOutOfBoundsException(row);
            B dst = setupDst(dstOffer, false), tail = dst.tail;
            int d = tail.rows*tail.cols;
            if (d+tail.cols > tail.termsCapacity) {
                tail = createTail(dst);
                d = 0;
            }
            ++tail.rows;

            long[] dIds = tail.arr;
            for (int c = 0, src, i = row*in.cols; c < cols.length; c++)
                dIds   [d+c] = (src=cols[c]) < 0 ? 0L : in.arr[i+src];
            assert tail.validate();
            return dst.releaseOwnership(this);
        }

        @Override public Orphan<B> mergeRow(@Nullable Orphan<B> dstOffer,
                                            B left, int leftRow, B right, int rightRow) {
            left.requireAlive();
            right.requireAlive();
            if ( leftRow >=  left.rows) throw new IndexOutOfBoundsException( leftRow);
            if (rightRow >= right.rows) throw new IndexOutOfBoundsException(rightRow);
            B dst = setupDst(dstOffer, false), tail = dst.tail;
            if (tail.cols > 0) {
                int d = tail.rows * tail.cols;
                short l = (short)(leftRow * left.cols), r = (short)(rightRow * right.cols);
                long[] dIds = tail.arr;
                if (d + tail.cols > dIds.length) {
                    dIds = (tail = createTail(dst)).arr;
                    d = 0;
                }
                short s;
                for (int c = 0; c < sources.length; c++, d++) {
                    if      ((s = sources[c]) > 0) dIds[d] =  left.arr[l+s-1];
                    else if ( s < 0)               dIds[d] = right.arr[r-s-1];
                    else                           dIds[d] = 0L;
                }
            }
            tail.rows++;
            assert tail.validate();
            return dst.releaseOwnership(this);
        }
    }

    public static abstract sealed class Filter<B extends IdBatch<B>>
            extends BatchFilter<B, Filter<B>> {
        private final Filter<B> beforeFilter;
        private final Merger<B> projector;

        @SuppressWarnings("unchecked")
        public Filter(BatchType<B> batchType, Vars vars, @Nullable Orphan<Merger<B>> projector,
                      Orphan<? extends RowFilter<B, ?>> rowFilter,
                      @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
            super(batchType, vars, rowFilter, before);
            this.projector = Orphan.takeOwnership(projector, this);
            assert this.projector == null || this.projector.vars.equals(vars);
            this.beforeFilter = (Filter<B>)this.before;
        }

        @Override protected void doRelease() {
            Owned.safeRecycle(projector, this);
            super.doRelease();
        }

        protected static final class Concrete<B extends IdBatch<B>> extends Filter<B> implements Orphan<Filter<B>> {
            public Concrete(BatchType<B> batchType, Vars vars,
                            @Nullable Orphan<Merger<B>> projector,
                            Orphan<? extends RowFilter<B, ?>> rowFilter,
                            @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
                super(batchType, vars, projector, rowFilter, before);
            }
            @Override public Filter<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public Orphan<B> processInPlace(Orphan<B> b) { return filterInPlace(b); }

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
                short cols   = filtered.cols, rows;
                var decision = DROP;
                B next       = filtered;
                filtered     = null;
                while (next != null) {
                    var b    = next;
                    next     = Orphan.takeOwnership(next.detachHead(), this);
                    rows     = b.rows;
                    decision = DROP;
                    int d    = 0;
                    for (short r = 0, start; r < rows && decision != TERMINATE; r++) {
                        start = r;
                        while (r < rows && (decision = rowFilter.drop(b, r)) == KEEP) ++r;
                        if (r > start) {
                            int n = (r-start)*cols, srcPos = start*cols;
                            arraycopy(b.arr, srcPos, b.arr, d, n);
                            d += (short) n;
                        }
                    }
                    b.rows = (short) (d / cols);
                    if      (d == 0)           Batch.safeRecycle(b, this);
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
