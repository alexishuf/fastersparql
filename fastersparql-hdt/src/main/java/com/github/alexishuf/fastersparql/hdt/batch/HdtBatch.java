package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.IdBatch;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType.HDT;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.NOT_FOUND;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.encode;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;

public class HdtBatch extends IdBatch<HdtBatch> {

    /* --- --- --- lifecycle --- --- --- */

    public HdtBatch(int terms, short cols, boolean special) {
        super(terms, cols, special);
        BatchEvent.Created.record(this);
    }

    public static HdtBatch of(int rows, int cols, long... ids) {
        HdtBatch b = new HdtBatch((short) (rows * cols), (short) cols, true);
        arraycopy(ids, 0, b.arr, 0, rows*cols);
        b.rows = (short)rows;
        return b;
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public HdtBatchType idType() { return HDT; }

    @Override public HdtBatch dup() {return dup((int)currentThread().threadId());}
    @Override public HdtBatch dup(int threadId) {
        HdtBatch b = HDT.createForThread(threadId, cols);
        b.copy(this);
        return b;
    }

    @Override public HdtBatch dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public HdtBatch dupRow(int row, int threadId) {
        short cols = this.cols;
        var b = HDT.createForThread(threadId, cols);
        b.doPut(this, (short)(row*cols), 0, (short)1, cols);
        return b;
    }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public int hash(int row, int col) {
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        int idx = row*cols + col, hash = hashes[idx];
        if (hash == 0) {
            CharSequence cs = IdAccess.toString(arr[idx]);
            int len = cs == null ? 0 : cs.length();
            byte[] u8 = IdAccess.peekU8(cs);
            if (u8 == null) { // manual hashing as HDT strings use FNV hashes
                for (int i = 0; i < len; i++) hash = 31 * hash + cs.charAt(i);
            } else {
                hash = SegmentRope.hashCode(Rope.FNV_BASIS, u8, 0, len);
            }
            hashes[idx] = hash;
        }
        return hash;
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col, HdtBatch other,
                          int oRow, int oCol) {
        int cols = this.cols, oCols = other.cols;
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols || oRow < 0 || oCol < 0
                    || oRow >= other.rows || oCol >= oCols) throw new IndexOutOfBoundsException();
        long id = arr[row*cols + col], oId = other.arr[oRow*oCols + oCol];
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

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        requireUnpooled();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == 0)
            return null;

        // try returning a cached value
        Term term = cachedTerm(addr);
        if (term == null) cacheTerm(addr, term = IdAccess.toTerm(id));
        return term;
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, Term dest) {
        Term t = get(row, col);
        if (t == null) return false;
        dest.set(t.shared(), t.local(), t.sharedSuffixed());
        return true;
    }

    @Override public @Nullable PlainRope getRope(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? null : new TwoSegmentRope(t.first(), t.second());
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

    @Override public @Nullable HdtBatch recycle() {
        return HDT.recycle(this);
    }

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
        var t = Term.pooledMutable();
        for (; o != null; o = o.next) {
            o.requireUnpooled();
            for (short nr, rb = 0, oRows = o.rows; rb < oRows; rb += nr) {
                int dPos = tail.rows*cols;
                nr = (short)( (tail.termsCapacity-dPos)/cols );
                if (nr <= 0) {
                    tail.tail = null;
                    nr = (short)( (tail = createTail()).termsCapacity/cols );
                    dPos = 0;
                }
                if (rb+nr > oRows) nr = (short)(oRows-rb);
                tail.rows += nr;
                long[] ids = tail.arr;
                Arrays.fill(tail.hashes, dPos, dPos + nr*cols, 0);
                for (int r = rb; r < nr; r++) {
                    for (int c = 0; c < cols; c++, ++dPos)
                        ids[dPos] = o.getView(r, c, t) ? encode(dictId, dict, t) : 0L;
                }
            }
        }
        t.recycle();
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
        var t = Term.pooledMutable();

        var dst = tail();
        int dPos = dst.rows*cols;
        if (dPos+cols > dst.termsCapacity) {
            dst = createTail();
            dPos = 0;
        }
        ++dst.rows;
        long[] ids = dst.arr;
        Arrays.fill(dst.hashes, dPos, dPos+cols, 0);
        for (int c = 0; c < cols; c++, ++dPos)
            ids[dPos] = other.getView(row, c, t) ? encode(dictId, dict, t) : 0L;
        t.recycle();
        assert dst.validate();
        return this;
    }
}
