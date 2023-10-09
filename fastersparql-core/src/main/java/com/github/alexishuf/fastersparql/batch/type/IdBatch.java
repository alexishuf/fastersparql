package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.KEEP;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.TERMINATE;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

public abstract class IdBatch<B extends IdBatch<B>> extends Batch<B> {
    private static final long[] EMPTY_ARR = new long[0];
    private static final  int[] EMPTY_HASHES = new int[0];
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
    protected int offerRowBase = -1;
    @SuppressWarnings("unused") // access through CACHED_LOCK
    protected int plainCachedLock;
    protected int cachedAddr = -1;
    protected Term cachedTerm;


    public IdBatch(int rowsCapacity, int cols) {
        super(0, cols);
        this.arr = longsAtLeast(rowsCapacity*cols);
        this.hashes = intsAtLeast(arr.length);
    }

    public IdBatch(long[] arr, int rows, int cols, int[] hashes) {
        super(rows, cols);
        this.arr = arr;
        this.hashes = hashes;
    }

    protected final B doCopy(B dest) {
        int n = rows * cols;
        arraycopy(arr, 0, dest.arr, 0, n);
        arraycopy(hashes, 0, dest.hashes, 0, n);
        dest.rows = rows;
        return dest;
    }
    protected final B doCopy(int row, B dst) {
        int cols = this.cols, srcPos = row*cols;
        arraycopy(arr,    srcPos, dst.arr,    0, cols);
        arraycopy(hashes, srcPos, dst.hashes, 0, cols);
        dst.rows = 1;
        return dst;
    }

    public void recycleInternals() {
        LONG.offer(arr, arr.length);
        INT.offer(hashes, hashes.length);
        arr = EMPTY_ARR;
        hashes = EMPTY_HASHES;
        rows = 0;
        cols = 0;
        offerRowBase = -1;
    }

    /* --- --- --- batch accessors --- --- --- */

    public long[] arr() { return arr; }

    @Override public int directBytesCapacity() { return arr.length*8;}
    @Override public int rowsCapacity()        { return arr.length/Math.max(1, cols); }

    @Override public boolean hasCapacity(int terms, int localBytes) {
        return terms <= arr.length && terms <= hashes.length;
    }

    @Override public boolean hasCapacity(int rows, int cols, int localBytes) {
        int terms = rows*cols;
        return terms <= arr.length && terms <= hashes.length;
    }

    /* --- --- --- helpers --- --- --- */
    protected Term cachedTerm(int addr) {
        // try returning a cached value
        int cachedAddr;
        Term cachedTerm;
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            cachedAddr = this.cachedAddr;
            cachedTerm = this.cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, 0); }
        return cachedAddr == addr ? cachedTerm : null;
    }

    protected void cacheTerm(int addr, Term term) {
        while (CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            this.cachedAddr = addr;
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

    @Override public void reserve(int additionalRows, int additionalBytes) {
        int required = (rows+additionalRows) * cols;
        if (arr.length < required)
            arr = grow(arr, required);
        if (hashes.length < required)
            hashes = grow(hashes, required);
    }

    @Override public void clear(int cols) {
        this.cols       = cols;
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
    }

    public @This B clearAndUnpool(int cols) {
        this.cols       = cols;
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
        unmarkPooled();
        //noinspection unchecked
        return (B)this;
    }

    public @This B clearAndReserveAndUnpool(int rows, int cols) {
        this.cols       = cols;
        this.rows       =    0;
        this.cachedAddr =   -1;
        this.cachedTerm = null;
        int terms = rows*cols;
        if (terms >    arr.length) arr    = longsAtLeast(terms,    arr);
        if (terms > hashes.length) hashes =  intsAtLeast(terms, hashes);
        //noinspection unchecked
        return (B)this;
    }

    public void dropCachedHashes() { Arrays.fill(hashes, 0, rows*cols, 0); }

    @Override public void abortPut() throws IllegalStateException {
        if (offerRowBase < 0) return;
        offerRowBase = -1;
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

    @SuppressWarnings("unchecked")
    protected final B put0(B other, @Nullable VarHandle rec, Object holder, BatchType<B> type) {
        if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int oRows = other.rows, cols = this.cols, rows = this.rows;
        if (oRows <= 0) return (B)this; // no-op
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");

        B dst = choosePutDst(rows, oRows, cols, rec, holder, type);
        int thisTerms = rows*cols, otherTerms = oRows*cols;
        arraycopy(other.arr,    0, dst.arr,    thisTerms, otherTerms);
        arraycopy(other.hashes, 0, dst.hashes, thisTerms, otherTerms);
        dst.rows += oRows;
        return dst;
    }

    @Override public void beginPut() {
        reserve(1, 0);
        int base = rows * cols, required = base + cols;
        if (arr.length < required) return;
        for (int i = base; i < required; i++) {
            arr[i] = 0;
            hashes[i] = 0;
        }
        offerRowBase = base;
    }

    @Override public void putTerm(int destCol, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (t != null) throw new UnsupportedOperationException("Cannot put non-null terms. Use putConverting(Batch, int) instead");
        putTerm(destCol, 0);
    }

    @Override public void putTerm(int d, B b, int r, int c) {
        if (offerRowBase < 0) throw new IllegalStateException();
        int oCols = b.cols;
        if (r < 0 || c < 0 || r >= b.rows || c >= oCols || d < 0 || d >= cols)
            throw new IndexOutOfBoundsException();
        arr[offerRowBase+ d] = b.arr[r *oCols + c];
    }

    public void putTerm(int col, long sourcedId) {
        if (offerRowBase < 0)      throw new IllegalStateException();
        if (col < 0 || col > cols) throw new IndexOutOfBoundsException();
        arr[offerRowBase+ col] = sourcedId;
    }

    @Override public void commitPut() {
        if (offerRowBase < 0) throw new IllegalStateException();
        ++rows;
        offerRowBase = -1;
    }

    @Override public void putRow(B other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException();
        if (row >= other.rows) throw new IndexOutOfBoundsException();
        reserve(1, 0);
        arraycopy(other.arr, row*other.cols, arr, cols*rows++, cols);
    }

    @SuppressWarnings("unchecked")
    protected B choosePutDst(int rows, int oRows, int cols, @Nullable VarHandle rec,
                             @Nullable Object holder, BatchType<B> type) {
        int nRows = rows+oRows, req = nRows*cols;
        if (req <= arr.length && req <= hashes.length)
            return (B)this;
        B dst = type.poll(nRows, cols, 0);
        if (dst == null) {
            if (arr   .length < req) arr    = grow(arr,    req);
            if (hashes.length < req) hashes = grow(hashes, req);
            return (B)this;
        }
        int terms = rows*cols;
        arraycopy(arr,    0, dst.arr,    0, terms);
        arraycopy(hashes, 0, dst.hashes, 0, terms);
        dst.rows = rows;
        markPooled();
        if (rec == null || rec.compareAndExchangeRelease(holder, null, this) != null)
            type.recycle(untracedUnmarkPooled());
        return dst;
    }

    @SuppressWarnings("unchecked")
    public final B putConverting0(Batch<?> other, @Nullable VarHandle rec, Object holder,
                                  BatchType<B> type) {
        if (other.type() == type)
            return put0((B)other, rec, holder, type);
        int cols = this.cols, oRows = other.rows;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");

        B dst = choosePutDst(rows, oRows, cols, rec, holder, type);
        Term t = Term.pooledMutable();
        for (int r = 0; r < oRows; r++)
            dst.putRowConverting(other, r, cols, t);
        t.recycle();
        return dst;
    }

    void putRowConverting(Batch<?> other, int row, int cols, Term tmp) {
        beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getView(row, c, tmp))
                putTerm(c, tmp);
        }
        commitPut();
    }

    @Override public void putRowConverting(Batch<?> other, int row) {
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        Term tmp = Term.pooledMutable();
        putRowConverting(other, row, cols, tmp);
        tmp.recycle();
    }

    /* --- --- --- operation objects --- --- --- */

    public static final class Merger<B extends IdBatch<B>> extends BatchMerger<B> {
        private long @Nullable [] tmpIds;
        private int  @Nullable [] tmpHashes;

        public Merger(BatchType<B> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public void doRelease() {
            if (tmpIds    != null) tmpIds    = LONG.offer(tmpIds,       tmpIds.length);
            if (tmpHashes != null) tmpHashes =  INT.offer(tmpHashes, tmpHashes.length);
            super.doRelease();
        }

        @Override public B projectInPlace(B b) {
            int w = b.cols;
            if (columns == null || columns.length > w) {
                var projected = project(null, b);
                if (projected != b && recycle(b) != null) batchType.recycle(b);
                return projected;
            }
            long[] tmpIds    = this.tmpIds    = longsAtLeast(w, this.tmpIds);
            int[]  tmpHashes = this.tmpHashes =  intsAtLeast(w, this.tmpHashes);
            b.cols = columns.length;
            long[] arr = b.arr;
            int[] hashes = b.hashes;
            for (int in = 0, out = 0, inEnd = w*b.rows; in < inEnd; in += w) {
                arraycopy(arr, in, tmpIds, 0, w);
                arraycopy(hashes, in, tmpHashes, 0, w);
                for (int src : columns) {
                    if (src >= 0) {
                        arr   [out] = tmpIds[src];
                        hashes[out] = tmpHashes[src];
                    }
                    ++out;
                }
            }
            return b;
        }
    }

    public static final class Filter<B extends IdBatch<B>> extends BatchFilter<B> {
        private long @Nullable [] tmpIds;
        private int  @Nullable [] tmpHashes;

        public Filter(BatchType<B> batchType, Vars vars, @Nullable BatchMerger<B> projector,
                      RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
            super(batchType, vars, projector, rowFilter, before);
        }

        @Override public void doRelease() {
            if (tmpIds    != null) tmpIds    = LONG.offer(tmpIds,       tmpIds.length);
            if (tmpHashes != null) tmpHashes =  INT.offer(tmpHashes, tmpHashes.length);
            super.doRelease();
        }

        private B filterInPlaceEmpty(B b) {
            int survivors = 0, rows = b.rows;
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(b, r)) {
                    case KEEP -> ++survivors;
                    case DROP -> {}
                    case TERMINATE -> rows = -1;
                }
            }
            if (rows == -1) {
                cancelUpstream();
                if (survivors == 0) return batchType.recycle(b);
            }
            if (projector != null)
                b.cols = requireNonNull(projector.columns).length;
            b.rows = survivors;
            Arrays.fill(b.arr, 0L);
            Arrays.fill(b.hashes, 0);
            return b;
        }

        @Override public B filterInPlace(B b, BatchMerger<B> projector) {
            if (before != null)
                b = before.filterInPlace(b);
            long[] arr = b.arr;
            int[] hashes = b.hashes;
            int r = 0, rows = b.rows, w = b.cols, out = 0;
            int @Nullable [] columns = projector == null ? null : projector.columns;
            if (rows == 0 || w == 0 || (columns != null && columns.length == 0))
                return filterInPlaceEmpty(b);
            if (columns != null && rowFilter.targetsProjection()) {
                b = projector.projectInPlace(b);
                columns = null;
            }
            if (columns == null) {
                RowFilter.Decision decision = null;
                //move r until we find a gap start (1+ rows that must be dropped)
                while (r < rows && (decision = rowFilter.drop(b, r)) == KEEP) ++r;
                out = r*w; // rows in [0, r) must be kept
                ++r; // r==rows or must be dropped, do not call drop(b, r) again
                for (int keep, kTerms; r < rows; out += kTerms) {
                    // skip over rows to be dropped
                    while (r < rows &&  (decision = rowFilter.drop(b, r)) != KEEP) ++r;
                    // find keep region [keep, keep+kTerms). ++r because either r==rows or is a keep
                    kTerms = (keep = r++) < rows ? w : 0;
                    for (; r < rows && (decision = rowFilter.drop(b, r)) == KEEP; ++r) kTerms += w;
                    // copy keep rows
                    arraycopy(arr, keep*w, arr, out, kTerms);
                    arraycopy(hashes, keep*w, hashes, out, kTerms);
                    if (decision == TERMINATE) rows = -1; // stop looping on TERMINATE
                }
                b.rows = out/w;
            } else {
                long[] tmpIds    = this.tmpIds    = longsAtLeast(w, this.tmpIds);
                int[]  tmpHashes = this.tmpHashes =  intsAtLeast(w, this.tmpHashes);
                // when projecting and filtering, there is no gain in copying regions
                for (int inRowStart = 0; r < rows; ++r, inRowStart += w) {
                    switch (rowFilter.drop(b, r)) {
                        case KEEP -> {
                            arraycopy(arr, inRowStart, tmpIds, 0, w);
                            arraycopy(hashes, inRowStart, tmpHashes, 0, w);
                            int required = out+columns.length;
                            if (required > arr.length)
                                b.arr = arr = grow(arr, columns.length*rows);
                            if (required > hashes.length)
                                b.hashes = hashes = grow(b.hashes, required);
                            for (int src : columns) {
                                if (src >= 0) {
                                    arr[out] = tmpIds[src];
                                    hashes[out] = tmpHashes[src];
                                } else {
                                    arr[out] = 0;
                                    hashes[out] = FNV_BASIS;
                                }
                                ++out;
                            }
                        }
                        case DROP      -> {}
                        case TERMINATE -> rows = -1;
                    }
                }
                b.cols = columns.length;
                b.rows = out/columns.length;
            }
            if (rows == -1) {
                cancelUpstream();
                if (out == 0) return batchType.recycle(b);
            }
            return b;
        }
    }
}
