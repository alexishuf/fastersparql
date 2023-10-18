package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.System.arraycopy;
import static java.lang.Thread.onSpinWait;

public abstract class IdBatch<B extends IdBatch<B>> extends Batch<B> {
    private static final VarHandle CACHED_LOCK;
    static {
        try {
            CACHED_LOCK = MethodHandles.lookup().findVarHandle(IdBatch.class, "plainCachedLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public long[] arr;
    protected int[] hashes;
    protected int termsCapacity;
    protected int offerRowBase = -1;
    @SuppressWarnings("unused") // access through CACHED_LOCK
    protected int plainCachedLock;
    protected int cachedAddr = -1;
    protected Term cachedTerm;


    protected IdBatch(int terms, int cols) {
        super(0, cols);
        this.arr    = longsAtLeast(terms);
        this.termsCapacity = arr.length;
        this.hashes =  intsAtLeast(termsCapacity);
    }

    public void markGarbage() {
        if (MARK_POOLED && (int)P.getAndSetRelease(this, P_GARBAGE) != P_POOLED)
            throw new IllegalStateException("marked non-pooled as garbage");
        LONG.offer(arr,    termsCapacity);
        INT .offer(hashes, hashes.length);
        arr           = EMPTY_LONG;
        hashes        = EMPTY_INT;
        termsCapacity =  0;
        rows          =  0;
        cols          =  0;
        offerRowBase  = -1;
    }

    @Override public boolean validate() {
        if (!SELF_VALIDATE)
            return true;
        return termsCapacity == arr.length && hashes.length >= termsCapacity && super.validate();
    }

    protected abstract B grown(int terms, @Nullable VarHandle rec, @Nullable Object holder);

    /* --- --- --- batch accessors --- --- --- */

    public long[] arr() { return arr; }

    @Override public int       rowsCapacity() { return cols == 0 ? Integer.MAX_VALUE : termsCapacity/cols; }
    @Override public int      termsCapacity() { return termsCapacity; }
    @Override public int totalBytesCapacity() { return termsCapacity*8 + hashes.length*4; }

    @Override public boolean hasCapacity(int terms, int local) { return terms <= termsCapacity; }

    /* --- --- --- helpers --- --- --- */

    protected Term cachedTerm(int address) {
        // try returning a cached value
        int cachedAddr;
        Term cachedTerm;
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) onSpinWait();
        try {
            cachedAddr = this.cachedAddr;
            cachedTerm = this.cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, 0); }
        return cachedAddr == address ? cachedTerm : null;
    }

    protected void cacheTerm(int address, Term term) {
        while (CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) onSpinWait();
        try {
            this.cachedAddr = address;
            this.cachedTerm = term;
        } finally { CACHED_LOCK.setRelease(this, 0); }
    }

    /* --- --- --- term-level access --- --- --- */

    public long id(int row, int col) {
        requireUnpooled();
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException("(row, col) out of bounds");
        return arr[row * cols + col];
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void clear() {
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
    }

    @Override public @This B clear(int cols) {
        this.cols       = cols;
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
        //noinspection unchecked
        return (B)this;
    }

    public void dropCachedHashes() {
        int[] hashes = this.hashes;
        for (int i = 0, end = rows*cols; i < end; i++)
            hashes[i] = 0;
    }

    @Override public void abortPut() throws IllegalStateException {
        requireUnpooled();
        if (offerRowBase < 0) return;
        offerRowBase = -1;
        assert validate();
    }

//    @Override public void put(B other) {
//        int cols = other.cols;
//        if (cols != this.cols)
//            throw new IllegalArgumentException();
//        int oRows = other.rows;
//        reserve(oRows, 0);
//        int items = oRows * cols;
//        arraycopy(other.arr, 0, arr, rows*cols, items);
//        arraycopy(other.hashes, 0, hashes, rows*cols,
//                Math.min(other.hashes.length, items));
//        rows += oRows;
//    }

    protected @This B doAppend(B src, int srcPos, int dstPos, int nRows, int cols) {
        int nTerms = nRows*cols;
        arraycopy(src.arr,    srcPos, arr,    dstPos, nTerms);
        arraycopy(src.hashes, srcPos, hashes, dstPos, nTerms);
        rows += nRows;
        assert validate();
        //noinspection unchecked
        return (B)this;
    }

    @SuppressWarnings("unchecked")
    @Override public final B put(B other, @Nullable VarHandle rec, Object holder) {
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int oRows = other.rows, cols = this.cols;
        if (oRows      <=    0) return (B)this; // no-op
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");

        int dstPos = rows*cols, nTerms = dstPos + oRows*cols;
        B dst = nTerms > termsCapacity ? grown(nTerms, rec, holder) : (B)this;
        return dst.doAppend(other, 0, dstPos, oRows, cols);
    }

    @Override public final B put(B other) { return put(other, null, null); }

    @Override public final B beginPut() {
        requireUnpooled();
        int begin = rows*cols, end = begin+cols;
        //noinspection unchecked
        B dst = end > termsCapacity ? grown(end, null, null) : (B)this;
        long[] arr    = dst.arr;
        int [] hashes = dst.hashes;
        for (int i = begin; i < end; i++)     arr[i] = 0;
        for (int i = begin; i < end; i++)  hashes[i] = 0;
        dst.offerRowBase = begin;
        return dst;
    }

    @Override public void putTerm(int destCol, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (t != null) throw new UnsupportedOperationException("Cannot put non-null terms. Use putConverting(Batch, int) instead");
        putTerm(destCol, 0);
    }

    @Override public final void putTerm(int d, B b, int r, int c) {
        if (offerRowBase < 0) throw new IllegalStateException();
        int oCols = b.cols;
        if (r < 0 || c < 0 || r >= b.rows || c >= oCols || d < 0 || d >= cols)
            throw new IndexOutOfBoundsException();
        arr[offerRowBase+ d] = b.arr[r *oCols + c];
    }

    public final void putTerm(int col, long sourcedId) {
        if (offerRowBase < 0)      throw new IllegalStateException();
        if (col < 0 || col > cols) throw new IndexOutOfBoundsException();
        arr[offerRowBase+col] = sourcedId;
    }

    @Override public final void commitPut() {
        requireUnpooled();
        if (offerRowBase < 0) throw new IllegalStateException();
        ++rows;
        offerRowBase = -1;
        assert validate();
    }

    @Override public final B putRow(B other, int row) {
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException();
        if (row >= other.rows) throw new IndexOutOfBoundsException();
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int dstPos = rows*cols, reqTerms = dstPos+cols;
        //noinspection unchecked
        B dst = reqTerms > termsCapacity ? grown(reqTerms, null, null) : (B)this;
        return dst.doAppend(other, row*cols, dstPos, 1, cols);
    }

    @SuppressWarnings("unchecked")
    public final B putConverting(Batch<?> other, @Nullable VarHandle rec, Object holder) {
        int cols = this.cols, oRows = other.rows, terms = (rows+oRows)*cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        B dst = terms > termsCapacity ? grown(terms, rec, holder) : (B)this;
        Term t = Term.pooledMutable();
        for (int r = 0; r < oRows; r++) {
            dst = dst.beginPut();
            for (int c = 0; c < cols; c++) {
                if (other.getView(r, c, t))
                    dst.putTerm(c, t);
            }
            dst.commitPut();
        }
        t.recycle();
        return dst;
    }

    @Override public final B putRowConverting(Batch<?> other, int row) {
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        Term t = Term.pooledMutable();
        B dst = beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getView(row, c, t))
                dst.putTerm(c, t);
        }
        t.recycle();
        dst.commitPut();
        return dst;
    }

    /* --- --- --- operation objects --- --- --- */

    public static abstract class Merger<B extends IdBatch<B>> extends BatchMerger<B> {
        public Merger(BatchType<B> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        protected B mergeWithMissing(B dst, B left, int leftRow) {
            long[] dIds    = dst.arr   , lIds    = left.arr   ;
            int [] dHashes = dst.hashes, lHashes = left.hashes;
            int d = dst.rows*dst.cols, l = leftRow*left.cols;
            ++dst.rows;
            for (int c = 0, src; c < sources.length; c++, ++d) {
                if ((src=sources[c]) > 0) {
                    dIds   [d] = lIds   [l+src];
                    dHashes[d] = lHashes[l+src];
                } else {
                    dIds   [d] = 0L;
                    dHashes[d] = 0;
                }
            }
            assert dst.validate();
            return dst;
        }

        protected final B mergeInto(B dst, B left, int leftRow, B right) {
            long[] dIds    = dst.arr   , lIds    = left.arr   , rIds    = right.arr   ;
            int [] dHashes = dst.hashes, lHashes = left.hashes, rHashes = right.hashes;
            int l = leftRow*left.cols, d = dst.rows*dst.cols, de = right.rows*dst.cols;
            dst.rows += right.rows;
            for (int r = 0, rc = right.cols; d < de; r+=rc) {
                for (int c = 0, src, pos; c < sources.length; c++, ++d) {
                    if ((src = sources[c]) > 0) {
                        dIds   [d] = lIds   [pos=l+src-1];
                        dHashes[d] = lHashes[pos];
                    } else if (src < 0) {
                        dIds   [d] = rIds   [pos=r-src-1];
                        dHashes[d] = rHashes[pos];
                    } else {
                        dIds   [d] = 0L;
                        dHashes[d] = 0;
                    }
                }
            }
            assert dst.validate();
            return dst;
        }

        protected final B projectInto(B dst, int[] cols, B in) {
            int ic = in.cols, iEnd = in.rows*ic, d;
            if (dst == in) {
                d = 0;
                dst.cols = cols.length;
            } else {
                d = dst.rows*dst.cols;
                dst.rows += in.rows;
            }
            long[] dIds    = dst.arr,    iIds    = in.arr;
            int [] dHashes = dst.hashes, iHashes = in.hashes;
            for (int i=0; i < iEnd; i+=ic) {
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
            assert dst.validate();
            return dst;
        }

        protected final B projectRowInto(B dst, B in, int row, int[] columns) {
            long[] dIds    = dst.arr   , iIds    = in.arr   ;
            int [] dHashes = dst.hashes, iHashes = in.hashes;
            int i = row*in.cols, d = dst.rows*dst.cols;
            ++dst.rows;
            for (int c = 0, src; c < columns.length; c++) {
                dIds   [d+c] = (src=columns[c]) < 0 ? 0L : iIds   [i+src];
                dHashes[d+c] =  src             < 0 ? 0  : iHashes[i+src];
            }
            assert dst.validate();
            return dst;
        }
    }

    public static abstract class Filter<B extends IdBatch<B>> extends BatchFilter<B> {

        public Filter(BatchType<B> batchType, Vars vars, RowFilter<B> rowFilter,
                      @Nullable BatchFilter<B> before) {
            super(batchType, vars, rowFilter, before);
        }

        protected final B filterInto(B dst, B in) {
            long[] dIds    = dst.arr   , iIds    = in.arr   ;
            int [] dHashes = dst.hashes, iHashes = in.hashes;
            int cols = dst.cols, d = dst == in ? 0 : dst.rows*cols;
            RowFilter.Decision decision = DROP;
            for (int r = 0, rows = in.rows; r < rows && decision != TERMINATE; r++) {
                int start = r;
                while (r < rows && (decision=rowFilter.drop(in, r)) == KEEP) ++r;
                if (r > start) {
                    int i = start*cols, n = (r-start)*cols;
                    arraycopy(iIds   , i, dIds   , d, n);
                    arraycopy(iHashes, i, dHashes, d, n);
                    d += n;
                }
            }
            return endFilter(dst, d, cols, decision==TERMINATE);
        }

        protected final B projectingFilterInto(B dst, B in, Merger<B> projector) {
            int [] columns = projector.columns;
            assert columns != null;
            long[] dIds    = dst.arr   , iIds    = in.arr   ;
            int [] dHashes = dst.hashes, iHashes = in.hashes;
            int rows = in.rows, iCols = in.cols, d = dst == in ? 0 : dst.rows*dst.cols;
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(in, r)) {
                    case KEEP -> {
                        for (int c = 0, src, i; c < columns.length; c++, ++d) {
                            if ((src = columns[c]) < 0) {
                                dIds   [d] = 0L;
                                dHashes[d] = 0;
                            } else {
                                dIds   [d] = iIds   [i=r*iCols+src];
                                dHashes[d] = iHashes[i];
                            }
                        }
                    }
                    case TERMINATE -> rows = -1;
                }
            }
            return endFilter(dst, d, columns.length, rows == -1);
        }
    }
}
