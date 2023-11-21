package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.onSpinWait;

public abstract class IdBatch<B extends IdBatch<B>> extends Batch<B> {
    private static final VarHandle CACHED_LOCK;
    static {
        try {
            CACHED_LOCK = MethodHandles.lookup().findVarHandle(IdBatch.class, "plainCachedLock", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public    final  long[] arr;
    protected final   int[] hashes;
    public    final short   termsCapacity;
    protected       short   offerRowBase = -1;
    @SuppressWarnings("unused") // access through CACHED_LOCK
    private boolean plainCachedLock;
    final boolean special;
    private short cachedAddr = -1;
    private Term cachedTerm;

    protected IdBatch(int terms, short cols, boolean special) {
        super((short)0, cols);
        this.arr           = longsAtLeastUpcycle(Math.max(terms, cols));
        this.termsCapacity = (short)min(arr.length, Short.MAX_VALUE);
        this.hashes        = intsAtLeast(termsCapacity);
        this.special       = special;
    }

    @Override final void markGarbage() {
        if (MARK_POOLED && (int)P.getAndSetRelease(this, P_GARBAGE) != P_POOLED)
            throw new IllegalStateException("marked non-pooled as garbage");
        for (var b = this; b != null; b = b.next) {
            LONG.offer(b.arr, b.termsCapacity);
            INT.offer(b.hashes, b.hashes.length);
            //b.arr           = EMPTY_LONG;
            //b.hashes        = EMPTY_INT;
            //b.termsCapacity =  0;
            b.rows            =  0;
            b.cols            =  0;
            b.offerRowBase    = -1;
        }
    }

    @Override protected boolean validateNode(Validation validation) {
        if (!SELF_VALIDATE || validation == Validation.NONE)
            return true;
        if (termsCapacity != arr.length)
            return false;
        if (hashes.length < termsCapacity)
            return false;
        return super.validateNode(validation);
    }

    /* --- --- --- batch accessors --- --- --- */

    public long[] arr() { return arr; }

    @Override public int       rowsCapacity() { return cols == 0 ? Integer.MAX_VALUE : termsCapacity/cols; }
    @Override public int      termsCapacity() { return termsCapacity; }
    @Override public int totalBytesCapacity() { return termsCapacity*8 + hashes.length*4; }

    @Override public boolean hasCapacity(int terms, int local) { return terms <= termsCapacity; }

    @Override public final BatchType<B> type() { return idType(); }

    public abstract IdBatchType<B> idType();

    /* --- --- --- helpers --- --- --- */

    protected Term cachedTerm(int address) {
        // try returning a cached value
        int cachedAddr;
        Term cachedTerm;
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, false, true)) onSpinWait();
        try {
            cachedAddr = this.cachedAddr;
            cachedTerm = this.cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, false); }
        return cachedAddr == address ? cachedTerm : null;
    }

    protected void cacheTerm(int address, Term term) {
        while (CACHED_LOCK.weakCompareAndSetAcquire(this, false, true)) onSpinWait();
        try {
            this.cachedAddr = (short)address;
            this.cachedTerm = term;
        } finally { CACHED_LOCK.setRelease(this, false); }
    }


    protected B createTail() {
        B b = idType().create(cols), tail = this.tail;
        if (tail == null)
            throw new UnsupportedOperationException("intermediary batch");
        tail.next = b;
        tail.tail = null;
        this.tail = b;
        return b;
    }

    /* --- --- --- term-level access --- --- --- */

    public long id(int row, int col) {
        requireUnpooled();
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

    /* --- --- --- mutators --- --- --- */

    @SuppressWarnings("unchecked") @Override public final void clear() {
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
        this.tail       = (B)this;
        if (next != null)
            next = idType().recycle(next);
    }

    @SuppressWarnings("unchecked") @Override public final @This B clear(int cols) {
        if (cols > termsCapacity) {
            B bigger = idType().create(cols);
            recycle();
            return bigger;
        }
        this.cols       = (short)cols;
        this.rows       =  0;
        this.cachedAddr = -1;
        this.cachedTerm = null;
        this.tail       = (B)this;
        if (next != null)
            this.next = idType().recycle(next);
        return (B)this;
    }

    public void dropCachedHashes() {
        for (var b = this; b != null; b = b.next) {
            int[] hashes = b.hashes;
            for (int i = 0, end = b.rows*b.cols; i < end; i++)
                hashes[i] = 0;
        }
    }

    @Override public void abortPut() throws IllegalStateException {
        B tail = tail();
        tail.requireUnpooled();
        if (tail.offerRowBase < 0) return;
        tail.offerRowBase = -1;
        if (tail != this && tail.rows == 0)
            dropTail(tail);
        assert validate();
    }


    protected final void doPut(B src, int srcPos, int dstPos, short nRows, short cols) {
        int nTerms = nRows*cols;
        arraycopy(src.arr,    srcPos, arr,    dstPos, nTerms);
        arraycopy(src.hashes, srcPos, hashes, dstPos, nTerms);
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
                o.requireUnpooled();
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

    @Override public void append(B other) {
        short cols = this.cols;
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (rows == 0)
            other = copyFirstNodeToEmpty(other);
        B dst = tail(), src = other, prev = null;
        for (; src != null; src = (prev=src).next) {
            src.requireUnpooled();
            short dstPos = (short)(dst.rows*cols), srcRows = src.rows;
            if (dstPos + (src.rows)*cols > dst.termsCapacity)
                break;
            dst.doPut(src, 0, dstPos, srcRows, cols); // copy contents
        }
        if (src != null)
            other = appendRemainder(other, prev, src);
        idType().recycle(other); // append() caller always looses ownership of other
        assert validate();
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
        int [] hashes = dst.hashes;
        for (int i = begin; i < end; i++)     arr[i] = 0;
        for (int i = begin; i < end; i++)  hashes[i] = 0;
        dst.offerRowBase = begin;
    }

    @Override public void putTerm(int destCol, Term t) {
        B tail = tail();
        if (tail.offerRowBase < 0) throw new IllegalStateException();
        if (t != null) throw new UnsupportedOperationException("Cannot put non-null terms. Use putConverting(Batch, int) instead");
        tail.doPutTerm(destCol, 0);
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

    @Override public final void putRow(B other, int row) {
        short cols = this.cols;
        other.requireUnpooled();
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
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        Term t = Term.pooledMutable();
        for (short oRows; other != null; other = other.next) {
            other.requireUnpooled();
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
        t.recycle();
    }

    @Override public final void putRowConverting(Batch<?> other, int row) {
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        Term t = Term.pooledMutable();
        beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getView(row, c, t))
                putTerm(c, t);
        }
        commitPut();
        t.recycle();
    }

    /* --- --- --- operation objects --- --- --- */

    @SuppressWarnings("UnnecessaryLocalVariable")
    public static class Merger<B extends IdBatch<B>> extends BatchMerger<B> {
        private final IdBatchType<B> idBatchType;
        private final short outColumns;

        public Merger(BatchType<B> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            this.idBatchType = (IdBatchType<B>)batchType;
            this.outColumns  = (short)sources.length;
        }

        private B setupDst(B offer, boolean inplace) {
            int cols = outColumns;
            if (offer != null) {
                if (offer.rows == 0 || inplace)
                    offer.cols = (short)cols;
                else if (offer.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return offer;
            }
            return idBatchType.create(cols);
        }

        private B createTail(B root) {
            return root.setTail(idBatchType.create(outColumns));
        }

        protected B mergeWithMissing(B dst, B left, int leftRow, B right) {
            long[] lIds    = left.arr;
            int [] lHashes = left.hashes;
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
                int [] dHashes = tail.hashes;
                tail.rows += (short)(nr=min(nr, rows));
                for (int e = d+nr*sources.length; d < e; d += sources.length) {
                    for (int c = 0, s; c < sources.length; c++) {
                        if ((s=sources[c]) > 0) {
                            dIds   [d+c] = lIds   [l+(--s)];
                            dHashes[d+c] = lHashes[l+s];
                        } else {
                            dIds   [d+c] = 0L;
                            dHashes[d+c] = 0;
                        }
                    }
                }
            }
            assert tail.validate();
            return dst;
        }

        @SuppressWarnings("UnnecessaryLocalVariable")
        @Override public final B merge(@Nullable B dst, B left, int leftRow, @Nullable B right) {
            dst = setupDst(dst, false);
            if (sources.length == 0)
                return mergeThin(dst, right);
            if (right == null || right.rows*right.cols == 0)
                return mergeWithMissing(dst, left, leftRow, right);

            long[] lIds    = left.arr;
            int [] lHashes = left.hashes;
            short l = (short)(leftRow*left.cols), rc = right.cols;
            B tail = dst.tail();
            for (; right != null; right = right.next) {
                long[] rIds    = right.arr;
                int [] rHashes = right.hashes;
                for (short rr = 0, rRows = right.rows, nr; rr < rRows; rr += nr) {
                    if ((nr=(short)(tail.termsCapacity/sources.length-tail.rows)) <= 0) {
                        tail = createTail(dst);
                        nr = (short)(tail.termsCapacity/sources.length);
                    }
                    nr = (short)min(nr, rRows-rr);
                    short d = (short)(tail.rows*tail.cols);
                    tail.rows += nr;
                    long[] dIds    = tail.arr;
                    int [] dHashes = tail.hashes;
                    for (short r = (short)(rr*rc), re = (short)((rr+nr)*rc); r < re; r+=rc) {
                        for (int c = 0, src; c < sources.length; c++, ++d) {
                            if ((src = sources[c]) == 0) {
                                dIds   [d] = 0L;
                                dHashes[d] = 0;
                            } else if (src > 0) {
                                dIds   [d] = lIds[src=l+src-1];
                                dHashes[d] = lHashes[src];
                            } else {
                                dIds   [d] = rIds[src=r-src-1];
                                dHashes[d] = rHashes[src];
                            }
                        }
                    }
                }
            }
            assert dst.validate();
            return dst;
        }

        @Override public final B project(B dst, B in) {
            short[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            boolean inplace = dst == in;
            short ic = in.cols;
            dst = setupDst(dst, inplace);
            if (cols.length == 0)
                return mergeThin(dst, in);
            B tail = inplace ? dst : dst.tail();
            for (; in != null; in = in.next) {
                long[]    iIds = in.arr,    dIds;
                int [] iHashes = in.hashes, dHashes;
                for (short ir = 0, iRows = in.rows, d, nr; ir < iRows; ir += nr) {
                    if (inplace) {
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
                    dIds    = tail.arr;
                    dHashes = tail.hashes;
                    for (short i=(short)(ir*ic), ie=(short)((ir+nr)*ic); i < ie; i+=ic) {
                        for (int c = 0, src, iPos; c < cols.length; c++, ++d) {
                            if ((src = cols[c]) < 0) {
                                dIds   [d] = 0L;
                                dHashes[d] = 0;
                            } else {
                                dIds   [d] = iIds   [iPos=i+src];
                                dHashes[d] = iHashes[iPos];
                            }
                        }
                    }
                }
            }
            assert dst.validate();
            return dst;
        }

        @Override public final B projectInPlace(B b) {
            if (b == null || b.rows == 0 || outColumns == 0)
                return projectInPlaceEmpty(b);
            var dst = project(safeInPlaceProject ? b : null, b);
            if (dst != b)
                b.recycle();
            return dst;
        }

        @Override public B processInPlace(B b) { return projectInPlace(b); }

        @Override public @Nullable B onBatch(B batch) {
            if (batch == null) return null;
            onBatchPrologue(batch);
            return onBatchEpilogue(projectInPlace(batch));
        }

        @Override public final B projectRow(@Nullable B dst, B in, int row) {
            var cols = columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            B tail = (dst = setupDst(dst, false)).tail();
            int d = tail.rows*tail.cols;
            if (d+tail.cols > tail.termsCapacity) {
                tail = createTail(dst);
                d = 0;
            }
            ++tail.rows;

            long[] dIds    = tail.arr   , iIds    = in.arr   ;
            int [] dHashes = tail.hashes, iHashes = in.hashes;
            for (int c = 0, src, i = row*in.cols; c < cols.length; c++) {
                dIds   [d+c] = (src=cols[c]) < 0 ? 0L : iIds   [i+src];
                dHashes[d+c] =  src          < 0 ? 0  : iHashes[i+src];
            }
            assert tail.validate();
            return dst;
        }

        @Override public B mergeRow(@Nullable B dst, B left, int leftRow, B right, int rightRow) {
            B tail = (dst = setupDst(dst, false)).tailUnchecked();
            if (tail.cols > 0) {
                int d = tail.rows * tail.cols;
                short l = (short)(leftRow * left.cols), r = (short)(rightRow * right.cols);
                long[] dIds = tail.arr, lIds = left.arr, rIds = right.arr;
                if (d + tail.cols > dIds.length) {
                    dIds = (tail = createTail(dst)).arr;
                    d = 0;
                }
                int[] dHsh = tail.hashes, lHsh = left.hashes, rHsh = right.hashes;
                short s, i;
                for (int c = 0; c < sources.length; c++, d++) {
                    if ((s = sources[c]) > 0) {
                        dIds[d] = lIds[i=(short)(l+s-1)];
                        dHsh[d] = lHsh[i];
                    } else if (s < 0) {
                        dIds[d] = rIds[i=(short)(r-s-1)];
                        dHsh[d] = rHsh[i];
                    } else {
                        dIds[d] = 0L;
                        dHsh[d] = 0;
                    }
                }
            }
            tail.rows++;
            assert tail.validate();
            return dst;
        }
    }

    public static final class Filter<B extends IdBatch<B>> extends BatchFilter<B> {
        private final Filter<B> before;
        private final Merger<B> projector;
        private final IdBatchType<B> idBatchType;

        public Filter(BatchType<B> batchType, Vars vars, @Nullable Merger<B> projector,
                      RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
            super(batchType, vars, rowFilter, before);
            assert projector == null || projector.vars.equals(vars);
            this.projector = projector;
            this.before = (Filter<B>)before;
            this.idBatchType = (IdBatchType<B>)batchType;
        }

        @Override public B processInPlace(B b) { return filterInPlace(b); }

        @Override public @Nullable B onBatch(B batch) {
            if (batch == null) return null;
            onBatchPrologue(batch);
            return onBatchEpilogue(filterInPlace(batch));
        }

        @Override public B filterInPlace(B in) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(in, in, in);
            var p = this.projector;
            if (p != null && rowFilter.targetsProjection()) {
                in = p.projectInPlace(in);
                p = null;
            }
            if (rowFilter.isNoOp())
                return in;
            B b = in, prev = in;
            short cols = in.cols, rows;
            var decision = DROP;
            while (b != null) {
                rows = b.rows;
                int d = 0;
                long[] ids = b.arr;
                int [] hashes = b.hashes;
                decision = DROP;
                for (short r=0, start; r < rows && decision != TERMINATE; r++) {
                    start = r;
                    while (r < rows && (decision=rowFilter.drop(b, r)) == KEEP) ++r;
                    if (r > start) {
                        int n = (r-start)*cols, srcPos = start*cols;
                        arraycopy(ids,    srcPos, ids,    d, n);
                        arraycopy(hashes, srcPos, hashes, d, n);
                        d += (short)n;
                    }
                }
                b.rows = (short)(d/cols);
                if (d == 0 && b != in)  // remove b from linked list
                    b = filterInPlaceSkipEmpty(b, prev);
                if (decision == TERMINATE) {
                    cancelUpstream();
                    if (b.next  != null) b.next = idBatchType.recycle(b.next);
                    if (in.rows ==    0) in     = idBatchType.recycle(in);
                }
                b = (prev = b).next;
            }
            in = filterInPlaceEpilogue(in, prev);
            if (p != null && in != null && in.rows > 0)
                in = p.projectInPlace(in);
            return in;
        }
    }
}
