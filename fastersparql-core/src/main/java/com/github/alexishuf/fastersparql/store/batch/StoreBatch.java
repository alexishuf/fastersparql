package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.MIN_INTERNED_LEN;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.util.Objects.requireNonNull;

public class StoreBatch extends IdBatch<StoreBatch> {
    public static final StoreBatchType TYPE = StoreBatchType.INSTANCE;
    public static int TEST_DICT = 0;

    /* --- --- --- lifecycle --- --- -- */

    StoreBatch(int terms, int cols) {
        super(terms, cols);
        BatchEvent.Created.record(this);
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public StoreBatchType type() { return TYPE; }

    @Override public StoreBatch copy(@Nullable StoreBatch offer) {
        int rows = this.rows, cols = this.cols;
        return TYPE.emptyForTerms(offer, rows > 0 ? rows*cols : cols, cols)
                   .doAppend(this, 0, 0, rows, cols);
    }

    @Override
    protected StoreBatch grown(int terms, @Nullable VarHandle rec, @Nullable Object holder) {
        int c = this.cols;
        var copy = TYPE.createForTerms(terms, c).doAppend(this, 0, 0, rows, c);
        BatchEvent.TermsGrown.record(copy);
        if (rec != null) {
            if (rec.compareAndExchangeRelease(holder, null, markPooled()) == null)
                return copy;
            markUnpooledNoTrace();
        }
        TYPE.recycle(this);
        return copy;
    }

    @Override public StoreBatch copyRow(int row, @Nullable StoreBatch offer) {
        int cols = this.cols;
        return TYPE.emptyForTerms(offer, cols, cols)
                   .doAppend(this, row*cols, 0, 1, cols);
    }

    @Override public StoreBatch withCapacity(int addRows) {
        int terms = (rows+addRows)*cols;
        return terms > termsCapacity ? grown(terms, null, null) : this;
    }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public int hash(int row, int col) {
        TwoSegmentRope tmp = tmpRope(row, col);
        if (tmp == null || tmp.len == 0) return FNV_BASIS;
        if (tmp.sndLen > MIN_INTERNED_LEN && tmp.snd.get(JAVA_BYTE, tmp.sndOff) == '"') {
            SegmentRope sh = SHARED_ROPES.internDatatype(tmp, tmp.fstLen, tmp.len);
            if (Term.isNumericDatatype(sh))
                return requireNonNull(get(row, col)).hashCode();
        }
        return tmp.hashCode();
    }

    private static @Nullable SegmentRope datatypeSuff(TwoSegmentRope nt) {
        if (nt.sndLen > MIN_INTERNED_LEN &&  nt.snd.get(JAVA_BYTE, nt.sndOff) == '"')
            return SHARED_ROPES.internDatatype(nt, nt.fstLen, nt.len);
        return null;
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col, StoreBatch other,
                          int oRow, int oCol) {
        int cols = this.cols, oCols = other.cols;
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols || oRow < 0 || oCol < 0
                || oRow >= other.rows || oCol >= oCols) throw new IndexOutOfBoundsException();
        long lId = arr[row*cols + col], rId = other.arr[oRow*oCols + oCol];
        int ldId = dictId(lId), rdId = dictId(rId);
        if (ldId == rdId) return lId == rId;
        if (lId == NOT_FOUND || rId == NOT_FOUND) return false;

        TwoSegmentRope left = lookup(ldId).get(lId), right = lookup(rdId).get(rId);
        // nulls only appear here if the dictId was recycled and the id referred to
        // the deregistered dict or if the id was above the dict size (garbage)
        if (left  == null) return right == null;
        if (right == null) return false;

        // numeric datatypes require parsing the numbers, compare as terms
        SegmentRope leftDt = datatypeSuff(left), rightDt = datatypeSuff(right);
        if (Term.isNumericDatatype(leftDt))
            return Term.isNumericDatatype(rightDt) && tsr2term(left).equals(tsr2term(right));
        if (Term.isNumericDatatype(rightDt))
            return false;

        // non-null, non-numeric, different dicts, compare by string
        return left.equals(right);
    }

    private @Nullable TwoSegmentRope tmpRope(int row, int col) {
        requireUnpooled();
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        long sourcedId = arr[row * cols + col];
        if (sourcedId == NOT_FOUND) return null;
        return lookup(dictId(sourcedId)).get(unsource(sourcedId));
    }

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        requireUnpooled();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == 0)
            return null;

        // try returning a cached value
        Term term = cachedTerm(addr);
        if (term == null)
            cacheTerm(addr, term = tsr2term(lookup(dictId(id)).get(id & ID_MASK)));
        return term;
    }

    @PolyNull private static Term tsr2term(@PolyNull TwoSegmentRope tsr) {
        if (tsr == null) return null;
        return Term.wrap(new SegmentRope(tsr.fst, tsr.fstOff, tsr.fstLen),
                         new SegmentRope(tsr.snd, tsr.sndOff, tsr.sndLen));
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, Term dest) {
        requireUnpooled();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == NOT_FOUND)
            return false;

        // try using a cached value
        Term cachedTerm = cachedTerm(addr);
        if (cachedTerm != null) {
            dest.set(cachedTerm.shared(), cachedTerm.local(), cachedTerm.sharedSuffixed());
            return true;
        }

        // load from dict
        TwoSegmentRope r = lookup(dictId(id)).get(id&ID_MASK);
        if (r == null || r.len <= 0)
            return false;
        int fLen = r.fstLen, sLen = r.sndLen;
        SegmentRope sh, local = SegmentRope.pooled();
        boolean isLit = r.get(0) == '"';
        if (isLit && fLen > 0) local.wrapSegment(r.fst, r.fstU8, r.fstOff, fLen);
        else                   local.wrapSegment(r.snd, r.sndU8, r.sndOff, sLen);
        if (!isLit) {
            sh = new SegmentRope(r.fst, r.fstU8, r.fstOff, fLen);
        } else if (fLen > 0) {
            if (sLen == 0)                    sh = EMPTY;
            else if (sLen < MIN_INTERNED_LEN) sh = new SegmentRope(r.snd, r.sndU8, r.sndOff, sLen);
            else                              sh = SHARED_ROPES.internDatatype(r, fLen, r.len);
        } else {
            sh = EMPTY;
        }
        dest.set(sh, local, isLit);
        local.recycle();
        return true;
    }

    @Override public @Nullable PlainRope getRope(@NonNegative int row, @NonNegative int col) {
        var tmp = tmpRope(row, col);
        if (tmp == null)
            return null;
        var out = new TwoSegmentRope();
        out.shallowCopy(tmp);
        return out;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        var tmp = tmpRope(row, col);
        if (tmp == null)
            return false;
        dest.shallowCopy(tmp);
        return true;
    }

    @Override
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRope dest) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        long sourcedId = arr[row * cols + col];
        if (sourcedId == NOT_FOUND) return false;
        var lookup = lookup(dictId(sourcedId));
        long id = unsource(sourcedId);
        SegmentRope tmp = lookup.getLocal(id);
        if (tmp == null) return false;
        dest.wrap(tmp);
        return true;
    }

    @Override public @NonNull SegmentRope shared(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        long sourcedId = arr[row * cols + col];
        if (sourcedId == NOT_FOUND)
            return EMPTY;
        var lookup = lookup(dictId(sourcedId));
        long id = unsource(sourcedId);
        SegmentRope tmp = lookup.getShared(id);
        if (tmp == null) return EMPTY;
        if (lookup.sharedSuffixed(id) && tmp.len > MIN_INTERNED_LEN)
            return SHARED_ROPES.internDatatype(tmp, 0, tmp.len);
        var out = new SegmentRope(); // this copy is required as tmp is mutated by lookup
        out.wrap(tmp);
        return out;
    }

    @Override public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        long sourcedId = arr[row * cols + col];
        if (sourcedId == NOT_FOUND)
            return false;
        return lookup(dictId(sourcedId)).sharedSuffixed(unsource(sourcedId));
    }

    @Override public int len(@NonNegative int row, @NonNegative int col) {
        var tmp = tmpRope(row, col);
        return tmp == null ? 0 : tmp.len;
    }

    @Override public int lexEnd(@NonNegative int row, @NonNegative int col) {
        var tmp = tmpRope(row, col);
        if (tmp == null || tmp.len == 0) return 0;
        if (tmp.fstLen == 0)
            return tmp.snd.get(JAVA_BYTE, tmp.sndOff) == '"' ? tmp.sndLen-1 : 0;
        return tmp.fst.get(JAVA_BYTE, tmp.fstOff) == '"'
                ? tmp.fstLen-(tmp.sndLen == 0 ? 1 : 0)
                : 0;
    }

    @Override public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
        long sourcedId = arr[row * cols + col], id;
        if ((id = unsource(sourcedId)) == NOT_FOUND)
            return 0;
        var lookup = lookup(dictId(sourcedId));
        TwoSegmentRope tsr = lookup.get(id);
        return tsr == null ? 0 : lookup.sharedSuffixed(id) ? tsr.fstLen : tsr.sndLen;
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        long sourcedId = arr[row * cols + col], id;
        if ((id = unsource(sourcedId)) == NOT_FOUND)
            return 0;
        var lookup = lookup(dictId(sourcedId));
        TwoSegmentRope tsr = lookup.get(id);
        return tsr == null ? 0 : lookup.sharedSuffixed(id) ? tsr.fstLen : tsr.sndLen;
    }

    @Override public Term. @Nullable Type termType(int row, int col) {
        TwoSegmentRope tmp = tmpRope(row, col);
        if (tmp == null || tmp.len == 0) return null;
        return switch (tmp.get(0)) {
            case '"'      -> Term.Type.LIT;
            case '_'      -> Term.Type.BLANK;
            case '<'      -> Term.Type.IRI;
            case '?', '$' -> Term.Type.VAR;
            default       -> throw new InvalidTermException(toString(), 0, "bad start");
        };
    }

    @Override
    public int writeSparql(ByteSink<?, ?> dest, int row, int column, PrefixAssigner prefixAssigner) {
        TwoSegmentRope tmp = tmpRope(row, column);
        if (tmp == null || tmp.len == 0) return 0;
        SegmentRope sh;
        MemorySegment local;
        byte[] localU8;
        long localOff;
        int localLen;
        boolean isLit = tmp.get(0) == '"';
        if (isLit) {
            sh = tmp.sndLen == 0 ? EMPTY : new SegmentRope(tmp.snd, tmp.sndOff, tmp.sndLen);
            if (sh.len >= MIN_INTERNED_LEN)
                sh = SHARED_ROPES.internDatatype(sh, 0, sh.len);
            local = tmp.fst;
            localU8 = tmp.fstU8;
            localOff = tmp.fstOff;
            localLen = tmp.fstLen;
        } else {
            sh = tmp.fstLen == 0 ? EMPTY : new SegmentRope(tmp.fst, tmp.fstOff, tmp.fstLen);
            local = tmp.snd;
            localU8 = tmp.sndU8;
            localOff = tmp.sndOff;
            localLen = tmp.sndLen;
        }
        return Term.toSparql(dest, prefixAssigner, sh, local, localU8, localOff, localLen, isLit);
    }

    @Override public void writeNT(ByteSink<?, ?> dest, int row, int col) {
        TwoSegmentRope tmp = tmpRope(row, col);
        if (tmp == null || tmp.len == 0) return;
        dest.append(tmp.fst, tmp.fstU8, tmp.fstOff, tmp.fstLen);
        dest.append(tmp.snd, tmp.sndU8, tmp.sndOff, tmp.sndLen);
    }

    @Override public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        TwoSegmentRope tmp = tmpRope(row, col);
        if (end <= begin) return;
        if (begin < 0 || tmp == null || end > tmp.len) throw new IndexOutOfBoundsException();
        dest.append(tmp, begin, end);
    }

    /* --- --- --- mutators --- --- --- */

    @Override public @Nullable StoreBatch recycle() {
        return StoreBatchType.INSTANCE.recycle(this);
    }

    @Override public void putTerm(int destCol, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (TEST_DICT == 0)
            throw new UnsupportedOperationException("putTerm(int, Term) is only supported during testing.");
        putTerm(destCol, t == null ? 0 : source(lookup(TEST_DICT).find(t), TEST_DICT));
    }

    /**
     * Similar to {@link Batch#putConverting(Batch, VarHandle, Object)}, but converts
     * {@link Term}s into {@code sourcedIds} referring to {@code dictId} rather than throwing
     * {@link UnsupportedOperationException}.
     *
     * @param other a {@link Batch} whose rows are to be added to {@code this}
     * @param dictId The previously {@link IdTranslator#register(LocalityCompositeDict)}ed dict
     *               that generated {@code sourcedId}s will point
     * @return {@code this}
     */
    public StoreBatch putConverting(Batch<?> other, int dictId, @Nullable VarHandle rec,
                                    @Nullable Object holder) {
        if (other.getClass() == getClass())
            return put((StoreBatch) other, rec, holder);
        int cols = other.cols, oRows = other.rows, terms = (rows+oRows)*cols;
        if (cols != this.cols) throw new IllegalArgumentException("cols mismatch");

        var dst = terms > termsCapacity ? grown(terms, rec, holder) : this;
        var lookup = lookup(dictId);
        var t = TwoSegmentRope.pooled();
        for (int r = 0; r < oRows; r++) {
            dst = dst.beginPut();
            for (int c = 0; c < cols; c++) {
                if (other.getRopeView(r, c, t))
                    dst.putTerm(c, IdTranslator.source(lookup.find(t), dictId));
            }
            dst.commitPut();
        }
        t.recycle();
        return dst;
    }

    public StoreBatch putRowConverting(Batch<?> other, int row, int dictId) {
        if (type() == other.type())
            return putRow((StoreBatch) other, row);
        int cols = this.cols;
        if (row > other.rows) throw new IndexOutOfBoundsException(row);
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");

        var lookup = lookup(dictId);
        var t = TwoSegmentRope.pooled();
        var dst = beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getRopeView(row, c, t))
                dst.putTerm(c, IdTranslator.source(lookup.find(t), dictId));
        }
        dst.commitPut();
        t.recycle();
        return dst;
    }

    /* -- --- --- merger --- --- --- */

    public static final class Merger extends IdBatch.Merger<StoreBatch> {
        public Merger(BatchType<StoreBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public StoreBatch projectInPlace(StoreBatch b) {
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

        @Override public StoreBatch project(StoreBatch dst, StoreBatch in) {
            int[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            return projectInto(TYPE.withCapacity(dst, in.rows, cols.length), cols, in);
        }

        @Override public StoreBatch projectRow(@Nullable StoreBatch dst, StoreBatch in, int row) {
            if (columns == null) throw new UnsupportedOperationException("not a projecting merger");
            dst = TYPE.withCapacity(dst, 1, columns.length);
            return projectRowInto(dst, in, row, columns);
        }

        @Override
        public StoreBatch merge(@Nullable StoreBatch dst, StoreBatch left, int leftRow,
                                @Nullable StoreBatch right) {
            int rows = right == null ? 0 : right.rows;
            dst = TYPE.withCapacity(dst, rows, sources.length);
            if (rows == 0)
                return mergeWithMissing(dst, left, leftRow);
            return mergeInto(dst, left, leftRow, right);
        }
    }

    /* -- --- --- filter --- --- --- */
    public final static class Filter extends IdBatch.Filter<StoreBatch> {
        private final @Nullable Merger projector;
        public Filter(BatchType<StoreBatch> batchType, Vars vars,
                      @Nullable Merger projector, RowFilter<StoreBatch> rowFilter,
                      @Nullable BatchFilter<StoreBatch> before) {
            super(batchType, vars, rowFilter, before);
            assert projector == null || projector.vars.equals(vars);
            this.projector = projector;
        }

        @Override public StoreBatch filter(@Nullable StoreBatch dst, StoreBatch in) {
            if (before != null)
                in = before.filter(null, in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(dst, in);
            StoreBatch garbage = null;
            var projector = this.projector;
            if (projector != null && rowFilter.targetsProjection()) {
                in = projector.project(null, in);
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

        @Override public StoreBatch filterInPlace(StoreBatch in) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(in, in);
            var projector = this.projector;
            if (projector != null && rowFilter.targetsProjection()) {
                in = projector.projectInPlace(in);
                projector = null;
            }
            return projector == null ? filterInto(in, in)
                                     : filterInPlaceProjecting(in, projector);
        }

        private StoreBatch filterInPlaceProjecting(StoreBatch in, Merger projector) {
            StoreBatch dst, garbage;
            if (projector.safeInPlaceProject) {
                garbage = null;
                dst = in;
            } else {
                garbage = in;
                dst = TYPE.createForTerms(in.rows*outColumns, outColumns);
            }
            dst = projectingFilterInto(dst, in, projector);
            if (garbage != null)
                TYPE.recycle(garbage);
            return dst;
        }
    }
}
