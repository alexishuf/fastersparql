package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static java.lang.System.arraycopy;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class HdtBatch extends Batch<HdtBatch> {
    public static final HdtBatchType TYPE = HdtBatchType.INSTANCE;

    private static final VarHandle CACHED_LOCK;
    static {
        try {
            CACHED_LOCK = lookup().findVarHandle(HdtBatch.class, "plainCachedLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    long[] arr;
    private int offerIdx = Integer.MAX_VALUE, offerEnd = 0;
    @SuppressWarnings("unused") // access through CACHED_LOCK
    private int plainCachedLock;
    private int cachedAddr = -1;
    private Term cachedTerm;

    /* --- --- --- lifecycle --- --- --- */

    public HdtBatch(int rowsCapacity, int cols) {
        super(0, cols);
        this.arr = new long[Math.min(1, rowsCapacity)*cols];
    }

    public HdtBatch(long[] arr, int rows, int cols) {
        super(rows, cols);
        this.arr = arr;
    }

    /* --- --- --- batch accessors --- --- --- */

    public long[] arr() { return arr; }

    @Override public HdtBatch copy()          { return new HdtBatch(Arrays.copyOf(arr, arr.length), rows, cols); }
    @Override public int      rowsCapacity()  { return arr.length/Math.max(1, cols); }
    @Override public boolean  hasCapacity(int rowsCapacity, int bytesCapacity) { return arr.length >= rowsCapacity*cols; }
    @Override public boolean  hasMoreCapacity(HdtBatch other)                  { return arr.length > other.arr.length; }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == 0)
            return null;

        // try returning a cached value
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            if (cachedAddr == addr) return cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, 0); }

        // create a Term instance and cache it

        Term term = IdAccess.toTerm(id);
        while (CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            cachedAddr = addr;
            cachedTerm = term;
        } finally { CACHED_LOCK.setRelease(this, 0); }
        return term;
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void reserve(int additionalRows, int additionalBytes) {
        int required = (rows+additionalRows) * cols;
        if (arr.length < required)
            arr = Arrays.copyOf(arr, Math.max(required, arr.length+(arr.length>>2)));
    }

    @Override public void clear(int newColumns) {
        rows = 0;
        cols = newColumns;
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            cachedAddr = -1;
            cachedTerm = null;
        } finally { CACHED_LOCK.setRelease(this, 0); }
    }

    @Override public boolean beginOffer() {
        int required = (rows + 1) * cols;
        if (arr.length < required) return false;
        for (int i = required-cols; i < required; i++) arr[i] = 0;
        offerEnd = required;
        offerIdx = required-cols;
        return true;
    }

    public boolean offerTerm(long sourcedId) {
        if (offerIdx >= offerEnd) throw new IllegalStateException();
        arr[offerIdx++] = sourcedId;
        return true;
    }

    @Override public boolean offerTerm(Term t) {
        if (offerIdx >= offerEnd) throw new IllegalStateException();
        ++offerIdx;
        return t == null;
    }

    @Override public boolean offerTerm(HdtBatch o, int row, int col) {
        if (offerIdx >= offerEnd) throw new IllegalStateException();
        int oCols = o.cols;
        if (row < 0 || col < 0 || row >= o.rows || col >= oCols)
            throw new IndexOutOfBoundsException();
        arr[offerIdx++] = o.arr[row*oCols + col];
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

    @Override public boolean offerRow(HdtBatch other, int row) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (row < 0 || row >= other.rows) throw new IndexOutOfBoundsException();
        if (beginOffer()) {
            arraycopy(other.arr, row*cols, arr, offerIdx, cols);
            ++rows;
            offerEnd = 0;
            offerIdx = Integer.MAX_VALUE;
            return true;
        }
        return false;
    }

    @Override public boolean offer(HdtBatch other) {
        if (other.cols != cols) throw new IllegalArgumentException();
        int out = rows * cols, nTerms = other.rows*other.cols;
        if (nTerms > arr.length-out) return false;
        arraycopy(other.arr, 0, arr, out, nTerms);
        rows += other.rows;
        return true;
    }

    @Override public void put(HdtBatch other) {
        int cols = other.cols;
        if (cols != this.cols)
            throw new IllegalArgumentException();
        int oRows = other.rows;
        reserve(oRows, 0);
        arraycopy(other.arr, 0, arr, rows* this.cols, oRows* cols);
        rows += oRows;
    }

    @Override public void beginPut() {
        reserve(1, 0);
        beginOffer();
    }

    @Override public void putTerm(Term t) {
        if (!offerTerm(t))
            throw new UnsupportedOperationException("Cannot put non-null terms. Use putConverting(Batch, int) instead");
    }
    @Override public void putTerm(HdtBatch batch, int row, int col) { offerTerm(batch, row, col); }
              public void putTerm(long sourcedId)                   { offerTerm(sourcedId); }
    @Override public void commitPut()                               { commitOffer(); }

    @Override public void putRow(HdtBatch other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException();
        reserve(1, 0);
        arraycopy(other.arr, row*other.cols, arr, cols*rows++, cols);
    }

    @Override public void putRow(Term[] row) {
        if (row.length != cols) throw new IllegalArgumentException();
        throw new UnsupportedOperationException();
    }

    /**
     * Similar to {@link Batch#putConverting(Batch)}, but converts {@link Term}s into
     * {@code sourcedIds} referring to {@code dictId} rather than throwing
     * {@link UnsupportedOperationException}.
     *
     * @param other a {@link Batch} whose rows are to be added to {@code this}
     * @param dictId The dictionary previously {@link IdAccess#register(Dictionary)}ed to
     *               which generated {@code sourcedId}s will point
     * @return {@code this}
     */
    public HdtBatch putConverting(Batch<?> other, int dictId) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (other.getClass() == getClass()) {
            put((HdtBatch) other);
        } else {
            var dict = IdAccess.dict(dictId);
            int rows = other.rows;
            reserve(rows, other.bytesUsed());
            for (int r = 0; r < rows; r++) {
                beginPut();
                for (int c = 0; c < cols; c++)
                    putTerm(IdAccess.encode(dictId, dict, other.get(r, c)));
                commitPut();
            }
        }
        return this;
    }

    /* --- --- --- operation objects --- --- --- */

    static final class Merger extends BatchMerger<HdtBatch> {
        private long @MonotonicNonNull [] tmp;

        public Merger(BatchType<HdtBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public HdtBatch projectInPlace(HdtBatch b) {
            int w = b.cols;
            if (columns == null || columns.length > w) {
                var projected = project(null, b);
                if (projected != b) recycle(b);
                return projected;
            }
            long[] tmp = this.tmp;
            if (tmp == null || tmp.length < w)
                this.tmp = tmp = new long[w];
            b.cols = columns.length;
            long[] arr = b.arr;
            for (int in = 0, out = 0, inEnd = w*b.rows; in < inEnd; in += w) {
                arraycopy(arr, in, tmp, 0, w);
                for (int src : columns)
                    arr[out++] = src >= 0 ? tmp[src] : 0;
            }
            return b;
        }
    }

    static final class Filter extends BatchFilter<HdtBatch> {
        private long @MonotonicNonNull [] tmp;

        public Filter(BatchType<HdtBatch> batchType, @Nullable BatchMerger<HdtBatch> projector,
                      RowFilter<HdtBatch> rowFilter) {
            super(batchType, projector, rowFilter);
        }

        private HdtBatch filterInPlaceEmpty(HdtBatch b) {
            int survivors = 0, rows = b.rows;
            for (int r = 0; r < rows; r++) {
                if (!rowFilter.drop(b, r)) ++survivors;
            }
            if (projector != null)
                b.cols = requireNonNull(projector.columns).length;
            b.rows = survivors;
            b.offerIdx = Integer.MAX_VALUE;
            Arrays.fill(b.arr, 0L);
            return b;
        }

        @Override public HdtBatch filterInPlace(HdtBatch b, BatchMerger<HdtBatch> projector) {
            long[] arr = b.arr;
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
                long[] tmp = this.tmp;
                if (tmp == null || tmp.length < w)
                    this.tmp = tmp = new long[w];
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
                        arr[out++] = src >= 0 ? tmp[src] : 0L;
                }
                b.cols = columns.length;
                b.rows = out/columns.length;
            }
            return b;
        }
    }

}
