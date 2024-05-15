package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.IdBatch;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType.HDT;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.NOT_FOUND;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.encode;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.longsAtLeast;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;

public abstract sealed class HdtBatch extends IdBatch<HdtBatch> {

    /* --- --- --- lifecycle --- --- --- */

    public HdtBatch(long[] ids, short cols) {
        super(ids, cols);
        BatchEvent.Created.record(this);
    }

    public static Orphan<HdtBatch> of(int rows, int cols, long... ids) {
        int terms = rows*cols;
        if (terms > Short.MAX_VALUE)
            throw new IllegalArgumentException("rows*cols > Short.MAX_VALUE");
        long[] dstIds = longsAtLeast(rows*cols);
        arraycopy(ids, 0, dstIds, 0, rows*cols);
        var b = new Concrete(dstIds, (short)cols);
        b.rows = (short)rows;
        return b;
    }

    protected static final class Concrete extends HdtBatch implements Orphan<HdtBatch> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
        public Concrete(long[] ids, short cols) {super(ids, cols);}
        @Override public HdtBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public HdtBatchType idType() { return HDT; }

    @Override public Orphan<HdtBatch> dup() {return dup((int)currentThread().threadId());}
    @Override public Orphan<HdtBatch> dup(int threadId) {
        HdtBatch b = HDT.createForThread(threadId, cols).takeOwnership(this);
        b.copy(this);
        return b.releaseOwnership(this);
    }

    @Override public Orphan<HdtBatch> dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public Orphan<HdtBatch> dupRow(int row, int threadId) {
        short cols = this.cols;
        var b = HDT.createForThread(threadId, cols).takeOwnership(this);
        b.doPut(this, (short)(row*cols), 0, (short)1, cols);
        return b.releaseOwnership(this);
    }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public boolean equals(@NonNegative int row, long[] ids, int idsOffset) {
        short rows = this.rows, cols = this.cols;
        //noinspection ConstantValue
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(row);
        for (int i = 0, offset = row*cols; i < cols; i++) {
            if (!equals(arr[offset+i], ids[idsOffset+i])) return false;
        }
        return true;
    }

    @Override public boolean equals(@NonNegative int row, @NonNegative int col, long id) {
        return equals(id(row, col), id);
    }

    public static boolean equals(long id, long oId) {
        if (IdAccess.sameSource(id, oId)) return id == oId;
        if (id == NOT_FOUND || oId == NOT_FOUND) return false;

        // compare by string
        CharSequence str = IdAccess.toString(id), oStr = IdAccess.toString(oId);
        int len = str == null ? -1 : str.length();
        if (oStr == null || len != oStr.length())
            return false; // short-circuit on null or length mismatch
        // try comparing byte arrays
        byte[] u8 = IdAccess.peekU8(str);
        byte[] oU8 = IdAccess.peekU8(oStr);
        if (u8 != null && oU8 != null)
            return Arrays.mismatch(u8, 0, len, oU8, 0, len) == 0;
        // this line should not be reached: fallback to slow but always correct comparison
        return CharSequence.compare(str, oStr) == 0;
    }

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        requireAlive();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == 0)
            return null;

        // try returning a cached value
        FinalTerm term = cachedTerm(addr);
        if (term == null) cacheTerm(addr, term = IdAccess.toTerm(id));
        return term;
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

    /**
     * Similar to {@link Batch#putConverting(Batch)}, but converts
     * {@link Term}s into {@code sourcedIds} referring to {@code dictId} rather than throwing
     * {@link UnsupportedOperationException}.
     *
     * @param o a {@link Batch} whose rows are to be added to {@code this}
     * @param dictId The dictionary previously {@link IdAccess#register(Dictionary)}ed to
     *               which generated {@code sourcedId}s will point
     * @return {@code this}
     */
    public HdtBatch putConverting(Batch<?> o, int dictId) {
        if (o instanceof HdtBatch b) {
            copy(b);
            return this;
        }
        short cols = this.cols;
        if (o.cols != cols)
            throw new IllegalArgumentException("other.cols != cols");
        var tail = tail();
        var dict = IdAccess.dict(dictId);
        try (var t = PooledTermView.ofEmptyString()) {
            for (; o != null; o = o.next) {
                o.requireAlive();
                for (short nr, rb = 0, oRows = o.rows; rb < oRows; rb += nr) {
                    int dPos = tail.rows*cols;
                    nr = (short)( (tail.termsCapacity-dPos)/cols );
                    if (nr <= 0) {
                        nr = (short)( (tail = createTail()).termsCapacity/cols );
                        dPos = 0;
                    }
                    if (rb+nr > oRows) nr = (short)(oRows-rb);
                    tail.rows += nr;
                    long[] ids = tail.arr;
                    for (int r = rb; r < nr; r++) {
                        for (int c = 0; c < cols; c++, ++dPos)
                            ids[dPos] = o.getView(r, c, t) ? encode(dictId, dict, t) : 0L;
                    }
                }
            }
        }
        assert validate();
        return this;
    }

    public HdtBatch putRowConverting(Batch<?> other, int row, int dictId) {
        if (other instanceof HdtBatch b) {
            putRow(b, row);
            return this;
        }
        int cols = this.cols;
        if (other.rows >=  row) throw new IndexOutOfBoundsException("row >= other.rows");
        if (other.cols != cols) throw new  IllegalArgumentException("other.cols != cols");

        var dict = IdAccess.dict(dictId);
        var dst = tail();
        try (var t = PooledTermView.ofEmptyString()) {
            int dPos = dst.rows*cols;
            if (dPos+cols > dst.termsCapacity) {
                dst = createTail();
                dPos = 0;
            }
            ++dst.rows;
            long[] ids = dst.arr;
            for (int c = 0; c < cols; c++, ++dPos)
                ids[dPos] = other.getView(row, c, t) ? encode(dictId, dict, t) : 0L;
        }
        assert dst.validate();
        return this;
    }
}
