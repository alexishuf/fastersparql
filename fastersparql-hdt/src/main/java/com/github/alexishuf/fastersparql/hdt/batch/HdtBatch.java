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
import org.checkerframework.common.returnsreceiver.qual.This;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.NOT_FOUND;
import static java.util.Arrays.copyOf;

public class HdtBatch extends IdBatch<HdtBatch> {
    public static final HdtBatchType TYPE = HdtBatchType.INSTANCE;

    /* --- --- --- lifecycle --- --- --- */

    public HdtBatch(int rowsCapacity, int cols) {
        super(rowsCapacity, cols);
        BatchEvent.Created.record(this);
    }

    HdtBatch(long[] arr, int rows, int cols) {
        super(arr, rows, cols, new int[arr.length]);
        BatchEvent.Created.record(this);
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public HdtBatchType type() { return TYPE; }

    @Override public HdtBatch copy(@Nullable HdtBatch offer) {
        return doCopy(TYPE.reserved(offer, rows, cols, 0));
    }

    @Override public HdtBatch copyRow(int row, @Nullable HdtBatch offer) {
        return doCopy(row, TYPE.reserved(offer, 1, cols, 0));
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
            if (hashes.length < idx)
                hashes = copyOf(hashes, arr.length);
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
        return HdtBatchType.INSTANCE.recycle(this);
    }

    /**
     * Similar to {@link Batch#putConverting(Batch, VarHandle, Object)}, but converts
     * {@link Term}s into {@code sourcedIds} referring to {@code dictId} rather than throwing
     * {@link UnsupportedOperationException}.
     *
     * @param other a {@link Batch} whose rows are to be added to {@code this}
     * @param dictId The dictionary previously {@link IdAccess#register(Dictionary)}ed to
     *               which generated {@code sourcedId}s will point
     * @return {@code this}
     */
    public HdtBatch putConverting(Batch<?> other, int dictId, @Nullable VarHandle rec,
                                  @Nullable Object holder) {
        if (other.getClass() == getClass())
            return put((HdtBatch) other, rec, holder);
        int cols = other.cols, oRows = other.rows;
        if (cols != this.cols) throw new IllegalArgumentException();

        var dst = choosePutDst(rows, oRows, cols, rec, holder, TYPE);
        var dict = IdAccess.dict(dictId);
        var tmp = Term.pooledMutable();
        for (int r = 0; r < oRows; r++)
            putRowConverting(other, r, dictId, cols, tmp, dict);
        tmp.recycle();
        return dst;
    }

    public @This HdtBatch putRowConverting(Batch<?> other, int row, int dictId) {
        if (other.type() == TYPE) {
            putRow((HdtBatch)other, row);
        } else {
            int cols = this.cols;
            if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
            if (row < 0 || row > other.rows) throw new IndexOutOfBoundsException(row);
            Dictionary dict = IdAccess.dict(dictId);
            Term tmp = Term.pooledMutable();
            putRowConverting(other, row, dictId, cols, tmp, dict);
            tmp.recycle();
        }
        return this;
    }

    private void putRowConverting(Batch<?> other, int row, int dictId, int cols,
                                  Term tmp, Dictionary dict) {
        beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getView(row, c, tmp))
                putTerm(c, IdAccess.encode(dictId, dict, tmp));
        }
        commitPut();
    }

    @Override
    public HdtBatch put(HdtBatch other, @Nullable VarHandle rec, @Nullable Object holder) {
        return put0(other, rec, holder, TYPE);
    }

    @Override
    public HdtBatch putConverting(Batch<?> other, @Nullable VarHandle rec,
                                  @Nullable Object holder) {
        return putConverting0(other, rec, holder, TYPE);
    }
}
