package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.IdBatch;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.*;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.lang.foreign.MemorySegment;

import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.MIN_INTERNED_LEN;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.batch.StoreBatchType.STORE;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.lang.Thread.currentThread;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public abstract sealed class StoreBatch extends IdBatch<StoreBatch> {
    public static int TEST_DICT = 0;

    /* --- --- --- lifecycle --- --- -- */

    StoreBatch(long[] ids, short cols) {
        super(ids, cols);
        BatchEvent.Created.record(this);
    }

    protected static final class Concrete extends StoreBatch implements Orphan<StoreBatch> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        public Concrete(long[] ids, short cols) {super(ids, cols);}
        @Override public StoreBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public StoreBatchType idType() { return STORE; }

    @Override public Orphan<StoreBatch> dup() {return dup((int)currentThread().threadId());}
    @Override public Orphan<StoreBatch> dup(int threadId) {
        StoreBatch b = STORE.createForThread(threadId, cols).takeOwnership(this);
        b.copy(this);
        return b.releaseOwnership(this);
    }

    @Override public Orphan<StoreBatch> dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }
    @Override public Orphan<StoreBatch> dupRow(int row, int threadId) {
        short cols = this.cols;
        var b = STORE.createForThread(threadId, cols).takeOwnership(this);
        b.doPut(this, row*cols, 0, (short)1, cols);
        return b.releaseOwnership(this);
    }

    /* --- --- --- term-level accessors --- --- --- */

    public static int hashId(long id) {
        if (id == NOT_FOUND)
            return FNV_BASIS;
        var lookup = dict(dictId(id)).lookup().takeOwnership(HASH_ID);
        try {
            var r = lookup.get(unsource(id));
            if (r == null || r.len == 0)
                return FNV_BASIS;
            if (r.sndLen > MIN_INTERNED_LEN && r.snd.get(JAVA_BYTE, r.sndOff) == '"') {
                var sh = SHARED_ROPES.internDatatype(r, r.fstLen, r.len);
                if (Term.isNumericDatatype(sh)) {
                    try (var tmp = PooledTermView.of(sh, r.fst, r.fstU8, r.fstOff,
                            r.fstLen, true)) {
                        return tmp.hashCode();
                    }
                }
            }
            return r.hashCode();
        } finally {
            lookup.recycle(HASH_ID);
        }
    }
    private static final StaticMethodOwner HASH_ID = new StaticMethodOwner("StoreBatch.hashId");

    @Override public int hash(int row, int col) {return hashId(id(row, col));}

    private static @Nullable SegmentRope datatypeSuff(TwoSegmentRope nt) {
        if (nt.sndLen > MIN_INTERNED_LEN &&  nt.snd.get(JAVA_BYTE, nt.sndOff) == '"')
            return SHARED_ROPES.internDatatype(nt, nt.fstLen, nt.len);
        return null;
    }

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
    @Override public boolean equals(@NonNegative int row, @NonNegative int col, long rId) {
        return equals(id(row, col), rId);
    }

    public static boolean equals(long lId, long rId) {
        int ldId = dictId(lId), rdId = dictId(rId);
        if (ldId == rdId) return lId == rId;
        if (lId == NOT_FOUND || rId == NOT_FOUND) return false;

        var lLookup = dict(ldId).lookup().takeOwnership(EQUALS);
        var rLookup = dict(rdId).lookup().takeOwnership(EQUALS);
        try {
            TwoSegmentRope left = lLookup.get(lId), right = rLookup.get(rId);
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
        } finally {
            lLookup.recycle(EQUALS);
            rLookup.recycle(EQUALS);
        }
    }
    private static final StaticMethodOwner EQUALS = new StaticMethodOwner("StoreBatch.equals");

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        requireAlive();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == 0)
            return null;

        // try returning a cached value
        FinalTerm term = cachedTerm(addr);
        if (term == null) {
            var lookup = dict(dictId(id)).lookup().takeOwnership(this);
            try {
                term = tsr2term(lookup.get(unsource(id)));
            } finally {
                lookup.recycle(this);
            }
            cacheTerm(addr, term);
        }
        return term;
    }

    @PolyNull private static FinalTerm tsr2term(@PolyNull TwoSegmentRope tsr) {
        if (tsr == null) return null;
        return Term.wrap(new FinalSegmentRope(tsr.fst, tsr.fstOff, tsr.fstLen),
                         new FinalSegmentRope(tsr.snd, tsr.sndOff, tsr.sndLen));
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        requireAlive();
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
            dest.wrap(cachedTerm.shared(), cachedTerm.local(), cachedTerm.sharedSuffixed());
            return true;
        }

        // load from dict
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try (var local = PooledSegmentRopeView.ofEmpty()) {
            TwoSegmentRope tmp = lookup.get(unsource(id));
            if (tmp == null || tmp.len <= 0)
                return false;
            int fLen = tmp.fstLen, sLen = tmp.sndLen;
            FinalSegmentRope sh;
            boolean isLit = tmp.get(0) == '"';
            if (isLit && fLen > 0) local.wrap(tmp.fst, tmp.fstU8, tmp.fstOff, fLen);
            else                   local.wrap(tmp.snd, tmp.sndU8, tmp.sndOff, sLen);
            if (!isLit) {
                sh = new FinalSegmentRope(tmp.fst, tmp.fstU8, tmp.fstOff, fLen);
            } else if (fLen > 0) {
                if (sLen == 0) sh = FinalSegmentRope.EMPTY;
                else if (sLen < MIN_INTERNED_LEN)
                    sh = new FinalSegmentRope(tmp.snd, tmp.sndU8, tmp.sndOff, sLen);
                else
                    sh = SHARED_ROPES.internDatatype(tmp, fLen, tmp.len);
            } else {
                sh = FinalSegmentRope.EMPTY;
            }
            dest.wrap(sh, local, isLit);
        } finally {
            lookup.recycle(this);
        }
        return true;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        long id = id(row, col);
        if (id != NOT_FOUND) {
            var lookup = dict(dictId(id)).lookup().takeOwnership(this);
            try {
                TwoSegmentRope tmp = lookup.get(unsource(id));
                if (tmp != null) {
                    dest.shallowCopy(tmp);
                    return true;
                }
            } finally {
                lookup.recycle(this);
            }
        }
        return false;
    }

    @Override
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRopeView dest) {
        long id = id(row, col);
        if (id == NOT_FOUND)
            return false;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            SegmentRope tmp = lookup.getLocal(unsource(id));
            if (tmp == null)
                return false;
            dest.wrap(tmp);
            return true;
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public @NonNull FinalSegmentRope shared(@NonNegative int row, @NonNegative int col) {
        long id = id(row, col);
        if (id == NOT_FOUND)
            return FinalSegmentRope.EMPTY;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            long unsourcedId = unsource(id);
            var tmp = lookup.getShared(unsourcedId);
            if (lookup.sharedSuffixed(unsourcedId) && tmp.len > MIN_INTERNED_LEN)
                return SHARED_ROPES.internDatatype(tmp, 0, tmp.len);
            return FinalSegmentRope.asFinalByReference(tmp); // tmp is mutated by pooled lookup
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        long sourcedId = id(row, col);
        if (sourcedId == NOT_FOUND)
            return false;
        var lookup = dict(dictId(sourcedId)).lookup().takeOwnership(this);
        try {
            return lookup.sharedSuffixed(unsource(sourcedId));
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public int len(@NonNegative int row, @NonNegative int col) {
        long id = id(row, col);
        if (id == NOT_FOUND)
            return 0;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            var tmp = lookup.get(unsource(id));
            return tmp == null ? 0 : tmp.len;
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public int lexEnd(@NonNegative int row, @NonNegative int col) {
        long id = id(row, col);
        if (id == NOT_FOUND)
            return 0;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            var tmp = lookup.get(unsource(id));
            if (tmp == null || tmp.len == 0)
                return 0;
            if (tmp.fstLen == 0)
                return tmp.snd.get(JAVA_BYTE, tmp.sndOff) == '"' ? tmp.sndLen-1 : 0;
            return tmp.fst.get(JAVA_BYTE, tmp.fstOff) == '"'
                    ? tmp.fstLen-(tmp.sndLen == 0 ? 1 : 0)
                    : 0;
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
        long sourcedId = arr[row * cols + col], id;
        if ((id = unsource(sourcedId)) == NOT_FOUND)
            return 0;
        var lookup = dict(dictId(sourcedId)).lookup().takeOwnership(this);
        try {
            var tsr = lookup.get(id);
            return tsr == null ? 0 : lookup.sharedSuffixed(id) ? tsr.fstLen : tsr.sndLen;
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        long sourcedId = id(row, col), id;
        if ((id = unsource(sourcedId)) == NOT_FOUND)
            return 0;
        var lookup = dict(dictId(sourcedId)).lookup().takeOwnership(this);
        try {
            TwoSegmentRope tsr = lookup.get(id);
            return tsr == null ? 0 : lookup.sharedSuffixed(id) ? tsr.fstLen : tsr.sndLen;
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public Term. @Nullable Type termType(int row, int col) {
        long id = id(row, col);
        if (id == NOT_FOUND)
            return null;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            TwoSegmentRope tmp = lookup.get(unsource(id));
            if (tmp == null || tmp.len == 0)
                return null;
            return switch (tmp.get(0)) {
                case '"'      -> Term.Type.LIT;
                case '_'      -> Term.Type.BLANK;
                case '<'      -> Term.Type.IRI;
                case '?', '$' -> Term.Type.VAR;
                default       -> throw new InvalidTermException(toString(), 0, "bad start");
            };
        } finally {
            lookup.recycle(this);
        }
    }

    @Override
    public int writeSparql(ByteSink<?, ?> dest, int row, int column, PrefixAssigner prefixAssigner) {
        long id = id(row, column);
        if (id == NOT_FOUND)
            return 0;
        PooledSegmentRopeView shView = null;
        SegmentRope sh = FinalSegmentRope.EMPTY;
        MemorySegment local;
        byte[] localU8;
        long localOff;
        int localLen;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            TwoSegmentRope tmp = lookup.get(unsource(id));
            if (tmp == null || tmp.len == 0)
                return 0;
            boolean isLit = tmp.get(0) == '"';
            if (isLit) {
                if (tmp.fstLen == 0)
                    tmp.flipSegments();
                else if (tmp.sndLen > 0)
                    sh = (shView = PooledSegmentRopeView.of(tmp.snd, tmp.sndOff, tmp.sndLen));
                if (sh.len >= MIN_INTERNED_LEN)
                    sh = SHARED_ROPES.internDatatype(sh, 0, sh.len);
                local = tmp.fst;
                localU8 = tmp.fstU8;
                localOff = tmp.fstOff;
                localLen = tmp.fstLen;
            } else {
                if (tmp.fstLen > 0)
                    sh = shView = PooledSegmentRopeView.of(tmp.fst, tmp.fstOff, tmp.fstLen);
                local = tmp.snd;
                localU8 = tmp.sndU8;
                localOff = tmp.sndOff;
                localLen = tmp.sndLen;
            }
            return Term.toSparql(dest, prefixAssigner, sh,
                                 local, localU8, localOff, localLen, isLit);
        } finally {
            lookup.recycle(this);
            if (shView != null)
                shView.close();
        }
    }

    @Override public void writeNT(ByteSink<?, ?> dest, int row, int col) {
        long id = id(row, col);
        if (id == NOT_FOUND)
            return;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            var tmp = lookup.get(unsource(id));
            if (tmp == null || tmp.len == 0)
                return;
            dest.append(tmp.fst, tmp.fstU8, tmp.fstOff, tmp.fstLen);
            dest.append(tmp.snd, tmp.sndU8, tmp.sndOff, tmp.sndLen);
        } finally {
            lookup.recycle(this);
        }
    }

    @Override public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        if (end <= begin)
            return;
        long id = id(row, col);
        if (id == NOT_FOUND)
            return;
        var lookup = dict(dictId(id)).lookup().takeOwnership(this);
        try {
            TwoSegmentRope tmp = lookup.get(unsource(id));
            if (tmp == null || begin < 0 || end > tmp.len)
                throw new IndexOutOfBoundsException();
            dest.append(tmp, begin, end);
        } finally {
            lookup.recycle(this);
        }
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void putTerm(int destCol, Term t) {
        StoreBatch dst = tail;
        if (dst.offerRowBase < 0) throw new IllegalStateException();
        if (TEST_DICT == 0)
            throw new UnsupportedOperationException("putTerm(int, Term) is only supported during testing.");
        long sourcedId = NOT_FOUND;
        if (t != null) {
            var lookup = dict(TEST_DICT).lookup().takeOwnership(this);
            try {
                sourcedId = source(lookup.find(t), TEST_DICT);
            } finally {
                lookup.recycle(this);
            }
        }
        dst.doPutTerm(destCol, sourcedId);
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
        var lu = dict(dictId).lookup().takeOwnership(this);
        try (var t = PooledTwoSegmentRope.ofEmpty()) {
            for (; o != null; o = o.next) {
                o.requireAlive();
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
                    for (int r = rb; r < nr; r++) {
                        for (int c = 0; c < cols; c++, ++dPos)
                            ids[dPos] = o.getRopeView(r, c, t) ? source(lu.find(t), dictId) : 0L;
                    }
                }
            }
        } finally {
            lu.recycle(this);
        }
        assert validate();
        return this;
    }

    public StoreBatch putRowConverting(Batch<?> other, int row, int dictId) {
        if (other instanceof StoreBatch b) {
            putRow(b, row);
            return this;
        }
        int cols = this.cols;
        if (other.rows >=  row)
            throw new IndexOutOfBoundsException("row >= other.rows");
        if (other.cols != cols)
            throw new  IllegalArgumentException("other.cols != cols");

        var dst = tail();
        var lookup = dict(dictId).lookup().takeOwnership(this);
        try (var t = PooledTwoSegmentRope.ofEmpty()) {
            int dPos = dst.rows * cols;
            if (dPos + cols > dst.termsCapacity) {
                dst = createTail();
                dPos = 0;
            }
            ++dst.rows;
            long[] ids = dst.arr;
            for (int c = 0; c < cols; c++, ++dPos)
                ids[dPos] = other.getRopeView(row, c, t) ? source(lookup.find(t), dictId) : 0L;
        } finally {
            lookup.recycle(this);
        }
        assert dst.validate();
        return this;
    }
}
