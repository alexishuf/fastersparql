package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.IdBatch;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.NOT_FOUND;
import static java.util.Arrays.copyOf;

public class HdtBatch extends IdBatch<HdtBatch> {
    public static final HdtBatchType TYPE = HdtBatchType.INSTANCE;

    /* --- --- --- lifecycle --- --- --- */

    public HdtBatch(int rowsCapacity, int cols) {super(rowsCapacity, cols);}
    public HdtBatch(long[] arr, int rows, int cols) {super(arr, rows, cols, new int[arr.length]);}
    public HdtBatch(long[] arr, int rows, int cols, int[] hashes) {super(arr, rows, cols, hashes);}

    /* --- --- --- batch accessors --- --- --- */

    @Override public HdtBatch copy() { return new HdtBatch(copyOf(arr, arr.length), rows, cols, copyOf(hashes, hashes.length)); }

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


    /* --- --- --- mutators --- --- --- */

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
                    putTerm(c, IdAccess.encode(dictId, dict, other.get(r, c)));
                commitPut();
            }
        }
        return this;
    }
}
