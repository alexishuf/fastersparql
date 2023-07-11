package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.IdBatch;
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

    public StoreBatch(int rowsCapacity, int cols) {super(rowsCapacity, cols);}
    public StoreBatch(long[] arr, int rows, int cols) { super(arr, rows, cols, new int[arr.length]); }
    public StoreBatch(long[] arr, int rows, int cols, int[] hashes) {super(arr, rows, cols, hashes);}

    /* --- --- --- batch accessors --- --- --- */

    @Override public StoreBatch copy(@Nullable StoreBatch offer) {
        return doCopy(offer == null ? TYPE.create(rows, cols, 0) : offer);
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
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

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
        TwoSegmentRope tsr = lookup(dictId(id)).get(id & ID_MASK);
        if (tsr == null)
            return false;
        SegmentRope shared = new SegmentRope(tsr.fst, tsr.fstOff, tsr.fstLen);
        SegmentRope local = new SegmentRope(tsr.snd, tsr.sndOff, tsr.sndLen);
        boolean isLit = tsr.fstLen > 0 && tsr.get(0) == '"';
        if (isLit) {
            var tmp = shared;
            shared = local;
            local = tmp;
        }
        dest.set(shared, local, isLit);
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

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        long sourcedId = arr[row * cols + col];
        if (sourcedId == NOT_FOUND)
            return 0;
        long id = unsource(sourcedId);
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

    @Override public boolean offerTerm(int col, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (TEST_DICT == 0)
            throw new UnsupportedOperationException("putTerm(int, Term) is only supported during testing.");
        putTerm(col, t == null ? 0 : source(lookup(TEST_DICT).find(t), TEST_DICT));
        return true;
    }

    /**
     * Similar to {@link Batch#putConverting(Batch)}, but converts {@link Term}s into
     * {@code sourcedIds} referring to {@code dictId} rather than throwing
     * {@link UnsupportedOperationException}.
     *
     * @param other a {@link Batch} whose rows are to be added to {@code this}
     * @param dictId The previously {@link IdTranslator#register(LocalityCompositeDict)}ed dict
     *               that generated {@code sourcedId}s will point
     * @return {@code this}
     */
    public StoreBatch putConverting(Batch<?> other, int dictId) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (other.getClass() == getClass()) {
            put((StoreBatch) other);
        } else {
            var lookup = lookup(dictId);
            TwoSegmentRope tmp = new TwoSegmentRope();
            int rows = other.rows;
            reserve(rows, other.bytesUsed());
            for (int r = 0; r < rows; r++) {
                beginPut();
                for (int c = 0; c < cols; c++) {
                    if (other.getRopeView(r, c, tmp))
                        putTerm(c, IdTranslator.source(lookup.find(tmp), dictId));
                }
                commitPut();
            }
        }
        return this;
    }

}
