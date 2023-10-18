package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.NOT_FOUND;
import static java.lang.System.arraycopy;

public class HdtBatch extends IdBatch<HdtBatch> {
    public static final HdtBatchType TYPE = HdtBatchType.INSTANCE;

    /* --- --- --- lifecycle --- --- --- */

    public HdtBatch(int terms, int cols) {
        super(terms, cols);
        BatchEvent.Created.record(this);
    }

    public HdtBatch(int rows, int cols, long... ids) {
        super(rows*cols, cols);
        arraycopy(ids, 0, arr, 0, rows*cols);
        this.rows = rows;
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public HdtBatchType type() { return TYPE; }

    @Override public HdtBatch copy(@Nullable HdtBatch offer) {
        int rows = this.rows, cols = this.cols;
        return TYPE.emptyForTerms(offer, rows > 0 ? rows*cols : cols, cols)
                   .doAppend(this, 0, 0, rows, cols);
    }

    @Override
    protected HdtBatch grown(int terms, @Nullable VarHandle rec, @Nullable Object holder) {
        int c = this.cols;
        var copy = TYPE.createForTerms(terms, c).doAppend(this, 0, 0, rows, c);
        BatchEvent.TermsGrown.record(copy);
        if (rec != null) {
            if (rec.compareAndExchangeRelease(holder, null, markPooled()) == null) return copy;
            markUnpooledNoTrace();
        }
        TYPE.recycle(this);
        return copy;
    }

    @Override public HdtBatch copyRow(int row, @Nullable HdtBatch offer) {
        int c = this.cols, srcPos = row*c;
        return TYPE.emptyForTerms(offer, c, c).doAppend(this, srcPos, 0, 1, c);
    }

    @Override public HdtBatch withCapacity(int addRows) {
        int terms = (rows+addRows)*cols;
        return terms > termsCapacity ? grown(terms, null, null) : this;
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
        int cols = other.cols, oRows = other.rows, terms = (rows+oRows)*cols;
        if (cols != this.cols) throw new IllegalArgumentException();

        var dst = terms > termsCapacity ? grown(terms, rec, holder) : this;
        var dict = IdAccess.dict(dictId);
        var t = Term.pooledMutable();
        for (int r = 0; r < oRows; r++) {
            dst = beginPut();
            for (int c = 0; c < cols; c++) {
                if (other.getView(r, c, t))
                    dst.putTerm(c, IdAccess.encode(dictId, dict, t));
            }
            dst.commitPut();
        }
        t.recycle();
        return dst;
    }

    public HdtBatch putRowConverting(Batch<?> other, int row, int dictId) {
        if (other.type() == TYPE)
            return putRow((HdtBatch)other, row);
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        if (row < 0 || row > other.rows) throw new IndexOutOfBoundsException(row);

        Dictionary dict = IdAccess.dict(dictId);
        Term t = Term.pooledMutable();
        var dst = beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getView(row, c, t))
                dst.putTerm(c, IdAccess.encode(dictId, dict, t));
        }
        dst.commitPut();
        t.recycle();
        return dst;
    }

    /* --- --- --- merger --- --- --- */
    public static final class Merger extends IdBatch.Merger<HdtBatch> {
        public Merger(BatchType<HdtBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public HdtBatch projectInPlace(HdtBatch b) {
            int[] cols = this.columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            int terms = b == null ? 0 : b.rows*cols.length;
            if (terms == 0)
                return projectInPlaceEmpty(b);
            var dst = safeInPlaceProject ? b : TYPE.createForTerms(terms, cols.length);
            projectInto(dst, cols, b);
            if (dst != b)
                TYPE.recycle(b);
            return dst;
        }

        @Override public HdtBatch project(HdtBatch dest, HdtBatch in) {
            int[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            return projectInto(TYPE.withCapacity(dest, in.rows, cols.length), cols, in);
        }

        @Override public HdtBatch projectRow(@Nullable HdtBatch dst, HdtBatch in, int row) {
            int[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projectin merger");
            return projectRowInto(TYPE.withCapacity(dst, 1, cols.length), in, row, cols);
        }

        @Override
        public HdtBatch merge(@Nullable HdtBatch dst, HdtBatch left, int leftRow, @Nullable HdtBatch right) {
            int rows = right == null ? 0 : right.rows;
            dst = TYPE.withCapacity(dst, rows, sources.length);
            if (rows == 0)
                return mergeWithMissing(dst, left, leftRow);
            return mergeInto(dst, left, leftRow, right);
        }
    }

    /* --- --- --- filter --- --- --- */

    public static final class Filter extends IdBatch.Filter<HdtBatch> {
        private final @Nullable Merger projector;
        public Filter(BatchType<HdtBatch> batchType, Vars vars,
                      @Nullable Merger projector, RowFilter<HdtBatch> rowFilter,
                      @Nullable BatchFilter<HdtBatch> before) {
            super(batchType, vars, rowFilter, before);
            assert projector == null || projector.vars.equals(vars);
            this.projector = projector;
        }

        @Override public HdtBatch filter(@Nullable HdtBatch dst, HdtBatch in) {
            if (before != null)
                in = before.filter(null, in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(dst, in);
            HdtBatch garbage = null;
            var projector = this.projector;
            if (projector != null && rowFilter.targetsProjection()) {
                in = projector.projectInPlace(in);
                projector = null;
                if (dst == null || dst.rows == 0) {
                    garbage = dst;
                    dst = in;
                } else {
                    garbage = in;
                }
            }
            dst = TYPE.withCapacity(dst, in.rows, outColumns);
            dst = projector == null ? filterInto(dst, in)
                                    : projectingFilterInto(dst, in, projector);
            if (garbage != null)
                TYPE.recycle(garbage);
            return dst;
        }

        @Override public HdtBatch filterInPlace(HdtBatch in) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(in, in);
            var projector = this.projector;
            if (projector != null & rowFilter.targetsProjection()) {
                in = projector.projectInPlace(in);
                projector = null;
            }
            return projector == null ? filterInto(in, in)
                                     : filterInPlaceProjecting(in, projector);
        }

        private HdtBatch filterInPlaceProjecting(HdtBatch in, Merger projector) {
            HdtBatch dst, garbage;
            if (projector.safeInPlaceProject) {
                dst = in;
                garbage = null;
            } else {
                dst = TYPE.createForTerms(in.rows*outColumns, outColumns);
                garbage = in;
            }
            dst = projectingFilterInto(dst, in, projector);
            if (garbage != null)
                TYPE.recycle(garbage);
            return dst;
        }
    }
}
