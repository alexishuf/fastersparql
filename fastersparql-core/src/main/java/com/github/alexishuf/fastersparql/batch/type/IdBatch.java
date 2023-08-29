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
        dest.reserve(rows, 0);
        arraycopy(arr, 0, dest.arr, 0, n);
        arraycopy(hashes, 0, dest.hashes, 0, n);
        dest.rows = rows;
        dest.cols = cols;
        return dest;
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

    @Override public int      directBytesCapacity()                            { return arr.length*8;}
    @Override public int      rowsCapacity()                                   { return arr.length/Math.max(1, cols); }
    @Override public boolean  hasCapacity(int rowsCapacity, int bytesCapacity) { return arr.length >= rowsCapacity*cols; }
    @Override public boolean  hasMoreCapacity(B other)                         { return arr.length > other.arr.length; }

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
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
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

    @Override public void clear(int newColumns) {
        rows = 0;
        cols = newColumns;
        cacheTerm(-1, null);
    }

    public void dropCachedHashes() {
        Arrays.fill(hashes, 0, rows*cols, 0);
    }

    @Override public boolean beginOffer() {
        int base = rows * cols, required = base + cols;
        if (arr.length < required) return false;
        for (int i = base; i < required; i++) {
            arr[i] = 0;
            hashes[i] = 0;
        }
        offerRowBase = base;
        return true;
    }

    @SuppressWarnings("UnusedReturnValue")
    public boolean offerTerm(int col, long sourcedId) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (col < 0 || col > cols) throw new IndexOutOfBoundsException();
        arr[offerRowBase+col] = sourcedId;
        return true;
    }

    @Override public boolean offerTerm(int col, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (t != null) return false;
        putTerm(col, 0);
        return true;
    }

    @Override public boolean offerTerm(int destCol, B o, int row, int col) {
        if (offerRowBase < 0) throw new IllegalStateException();
        int oCols = o.cols;
        if (row < 0 || col < 0 || row >= o.rows || col >= oCols || destCol < 0 || destCol >= cols)
            throw new IndexOutOfBoundsException();
        arr[offerRowBase+destCol] = o.arr[row*oCols + col];
        return true;
    }

    @Override public boolean commitOffer() {
        if (offerRowBase < 0) throw new IllegalStateException();
        ++rows;
        offerRowBase = -1;
        return true;
    }

    @Override public boolean abortOfferOrPut() throws IllegalStateException {
        if (offerRowBase < 0) return false;
        offerRowBase = -1;
        return true;
    }

    @Override public boolean offerRow(B other, int row) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (row < 0 || row >= other.rows) throw new IndexOutOfBoundsException();
        if (beginOffer()) {
            arraycopy(other.arr, row*cols, arr, offerRowBase, cols);
            ++rows;
            offerRowBase = -1;
            return true;
        }
        return false;
    }

    @Override public boolean fits(B other) {
        return (rows+other.rows)*cols <= arr.length;
    }

    @Override public boolean offer(B other) {
        int out = rows * cols, nTerms = other.rows*other.cols;
        if (nTerms > arr.length-out) return false;
        put(other);
        return true;
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

    @Override public void putRange(B other, int begin, int end) {
        int oRows = end-begin, cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (oRows <= 0) return; // no-op

        reserve(oRows, 0);
        int src = begin*cols, dst = rows*cols, terms = oRows*cols;
        arraycopy(other.arr,    src, arr,    dst, terms);
        arraycopy(other.hashes, src, hashes, dst, terms);
        rows += oRows;
    }

    @Override public void beginPut() {
        reserve(1, 0);
        beginOffer();
    }

    @Override public void putTerm(int destCol, Term t) {
        if (!offerTerm(destCol, t))
            throw new UnsupportedOperationException("Cannot put non-null terms. Use putConverting(Batch, int) instead");
    }
    @Override public void putTerm(int d, B b, int r, int c) { offerTerm(d, b, r, c); }
    public           void putTerm(int col, long sourcedId)           { offerTerm(col, sourcedId); }
    @Override public void commitPut()                                { commitOffer(); }

    @Override public void putRow(B other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException();
        if (row >= other.rows) throw new IndexOutOfBoundsException();
        reserve(1, 0);
        arraycopy(other.arr, row*other.cols, arr, cols*rows++, cols);
    }

    @SuppressWarnings("unchecked") @Override
    public <O extends Batch<O>> @This B putConverting(O other) {
        if (other.type() == type()) {
            put((B)other);
        } else {
            int cols = this.cols, rows = other.rows;
            if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
            reserve(rows, 0);
            Term t = Term.pooledMutable();
            for (int r = 0; r < rows; r++)
                putRowConverting(other, r, cols, t);
            t.recycle();
        }
        return (B) this;
    }

    private <O extends Batch<O>> void putRowConverting(O other, int row, int cols, Term tmp) {
        beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getView(row, c, tmp))
                putTerm(c, tmp);
        }
        commitPut();
    }

    @Override public <O extends Batch<O>> void putRowConverting(O other, int row) {
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
