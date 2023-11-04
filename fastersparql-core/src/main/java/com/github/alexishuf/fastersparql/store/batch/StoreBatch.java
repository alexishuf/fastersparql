package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
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
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.MIN_INTERNED_LEN;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.batch.StoreBatchType.STORE;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.lang.Thread.currentThread;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.util.Objects.requireNonNull;

public class StoreBatch extends IdBatch<StoreBatch> {
    public static int TEST_DICT = 0;

    /* --- --- --- lifecycle --- --- -- */

    StoreBatch(int terms, short cols, boolean special) {
        super(terms, cols, special);
        BatchEvent.Created.record(this);
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public StoreBatchType idType() { return STORE; }

    @Override public StoreBatch dup() {return dup((int)currentThread().threadId());}
    @Override public StoreBatch dup(int threadId) {
        StoreBatch b = STORE.createForThread(threadId, cols);
        b.copy(this);
        return b;
    }

    @Override public StoreBatch dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }
    @Override public StoreBatch dupRow(int row, int threadId) {
        short cols = this.cols;
        var b = STORE.createForThread(threadId, cols);
        b.doPut(this, row*cols, 0, (short)1, cols);
        return b;
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
        return STORE.recycle(this);
    }

    @Override public void putTerm(int destCol, Term t) {
        StoreBatch dst = tailUnchecked();
        if (dst.offerRowBase < 0) throw new IllegalStateException();
        if (TEST_DICT == 0)
            throw new UnsupportedOperationException("putTerm(int, Term) is only supported during testing.");
        dst.doPutTerm(destCol, t == null ? 0 : source(lookup(TEST_DICT).find(t), TEST_DICT));
    }

    /**
     * Similar to {@link Batch#putConverting(Batch)}, but converts
     * {@link Term}s into {@code sourcedIds} referring to {@code dictId} rather than throwing
     * {@link UnsupportedOperationException}.
     *
     * @param o a {@link Batch} whose rows are to be added to {@code this}
     * @param dictId The previously {@link IdTranslator#register(LocalityCompositeDict)}ed dict
     *               that generated {@code sourcedId}s will point
     * @return {@code this}
     */
    public StoreBatch putConverting(Batch<?> o, int dictId) {
        if (o instanceof StoreBatch b) {
            copy(b);
            return this;
        }
        short cols = this.cols;
        if (o.cols != cols)
            throw new IllegalArgumentException("other.cols != cols");
        var tail = tail();
        var lookup = lookup(dictId);
        var t = TwoSegmentRope.pooled();
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
                        ids[dPos] = o.getRopeView(r, c, t) ? source(lookup.find(t), dictId) : 0L;
                }
            }
        }
        t.recycle();
        assert validate();
        return this;
    }

    public StoreBatch putRowConverting(Batch<?> other, int row, int dictId) {
        if (other instanceof StoreBatch b) {
            putRow(b, row);
            return this;
        }
        int cols = this.cols;
        if (other.rows >=  row) throw new IndexOutOfBoundsException("row >= other.rows");
        if (other.cols != cols) throw new  IllegalArgumentException("other.cols != cols");

        var lookup = lookup(dictId);
        var t = TwoSegmentRope.pooled();

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
            ids[dPos] = other.getRopeView(row, c, t) ? source(lookup.find(t), dictId) : 0L;
        t.recycle();
        assert dst.validate();
        return this;
    }
}
