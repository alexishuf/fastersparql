package com.github.alexishuf.fastersparql.store.batch;

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
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.MIN_INTERNED_LEN;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.lang.System.arraycopy;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.util.Arrays.copyOf;
import static java.util.Objects.requireNonNull;

public class StoreBatch extends Batch<StoreBatch> {
    public static final StoreBatchType TYPE = StoreBatchType.INSTANCE;
    private static final VarHandle CACHED_LOCK;
    static {
        try {
            CACHED_LOCK = MethodHandles.lookup().findVarHandle(StoreBatch.class, "plainCachedLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    public static int TEST_DICT = 0;

    long[] arr;
    int[] hashes;
    private int offerRowBase = -1;
    @SuppressWarnings("unused") // access through CACHED_LOCK
    private int plainCachedLock;
    private int cachedAddr = -1;
    private Term cachedTerm;

    /* --- --- --- lifecycle --- --- -- */

    public StoreBatch(int rowsCapacity, int cols) {
        super(0, cols);
        this.arr = new long[Math.min(1, rowsCapacity)*cols];
        this.hashes = new int[arr.length];
    }

    public StoreBatch(long[] arr, int rows, int cols) { this(arr, rows, cols, new int[arr.length]); }

    public StoreBatch(long[] arr, int rows, int cols, int[] hashes) {
        super(rows, cols);
        this.arr = arr;
        this.hashes = hashes;
    }

    /* --- --- --- batch accessors --- --- --- */

    public long[] arr() { return arr; }

    @Override public StoreBatch copy()        { return new StoreBatch(copyOf(arr, arr.length), rows, cols, copyOf(hashes, hashes.length)); }
    @Override public int      rowsCapacity()  { return arr.length/Math.max(1, cols); }
    @Override public boolean  hasCapacity(int rowsCapacity, int bytesCapacity) { return arr.length >= rowsCapacity*cols; }
    @Override public boolean  hasMoreCapacity(StoreBatch other)                  { return arr.length > other.arr.length; }


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

    public long sourcedId(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        return arr[row*cols + col];
    }

    private @Nullable TwoSegmentRope tmpRope(int row, int col) {
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        long sourcedId = arr[row * cols + col];
        if (sourcedId == NOT_FOUND) return null;
        return lookup(dictId(sourcedId)).get(unsource(sourcedId));
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
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            if (cachedAddr == addr && cachedTerm != null)
                return cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, 0); }

        // create a Term instance and cache it
        int dId = dictId(id);
        TwoSegmentRope tsr = lookup(dId).get(id & ID_MASK);
        Term term = tsr2term(tsr);
        while (CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            cachedAddr = addr;
            cachedTerm = term;
        } finally { CACHED_LOCK.setRelease(this, 0); }
        return term;
    }

    @PolyNull private static Term tsr2term(@PolyNull TwoSegmentRope tsr) {
        if (tsr == null) return null;
        return Term.wrap(new SegmentRope(tsr.fst, tsr.fstOff, tsr.fstLen),
                         new SegmentRope(tsr.snd, tsr.sndOff, tsr.sndLen));
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, Term dest) {
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();

        // check for null
        int addr = row * cols + col;
        long id = arr[addr];
        if (id == NOT_FOUND)
            return false;

        // try using a cached value
        int cachedAddr;
        Term cachedTerm;
        while (!CACHED_LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            cachedAddr = this.cachedAddr;
            cachedTerm = this.cachedTerm;
        } finally { CACHED_LOCK.setRelease(this, 0); }
        if (cachedAddr == addr) {
            if (cachedTerm == null) return false;
            dest.set(cachedTerm.shared(), cachedTerm.local(), cachedTerm.sharedSuffixed());
            return true;
        }

        // load from dict
        TwoSegmentRope tsr = lookup(dictId(id)).get(id & ID_MASK);

        if (tsr == null)
            return false;
        SegmentRope shared = new SegmentRope(tsr.fst, tsr.fstOff, tsr.fstLen);
        SegmentRope local = new SegmentRope(tsr.snd, tsr.sndOff, tsr.sndLen);
        boolean isLit = tsr.fstLen > 0 && tsr.fst.get(JAVA_BYTE, 0) == '"';
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
    public void writeSparql(ByteSink<?> dest, int row, int column, PrefixAssigner prefixAssigner) {
        TwoSegmentRope tmp = tmpRope(row, column);
        if (tmp == null || tmp.len == 0) return;
        SegmentRope sh;
        MemorySegment local;
        long localOff;
        int localLen;
        boolean isLit = tmp.get(0) == '"';
        if (isLit) {
            sh = tmp.sndLen == 0 ? EMPTY : new SegmentRope(tmp.snd, tmp.sndOff, tmp.sndLen);
            if (sh.len >= MIN_INTERNED_LEN)
                sh = SHARED_ROPES.internDatatype(sh, 0, sh.len);
            local = tmp.fst;
            localOff = tmp.fstOff;
            localLen = tmp.fstLen;
        } else {
            sh = tmp.fstLen == 0 ? EMPTY : new SegmentRope(tmp.fst, tmp.fstOff, tmp.fstLen);
            local = tmp.snd;
            localOff = tmp.sndOff;
            localLen = tmp.sndLen;
        }
        Term.toSparql(dest, prefixAssigner, sh, local, localOff, localLen, isLit);
    }

    @Override public void writeNT(ByteSink<?> dest, int row, int col) {
        TwoSegmentRope tmp = tmpRope(row, col);
        if (tmp == null || tmp.len == 0) return;
        dest.append(tmp.fst, tmp.fstOff, tmp.fstLen);
        dest.append(tmp.snd, tmp.sndOff, tmp.sndLen);
    }

    @Override public void write(ByteSink<?> dest, int row, int col, int begin, int end) {
        TwoSegmentRope tmp = tmpRope(row, col);
        if (end <= begin) return;
        if (begin < 0 || tmp == null || end > tmp.len) throw new IndexOutOfBoundsException();
        dest.append(tmp, begin, end);
    }



    /* --- --- --- mutators --- --- --- */

    @Override public void reserve(int additionalRows, int additionalBytes) {
        int required = (rows+additionalRows) * cols;
        if (arr.length < required)
            arr = copyOf(arr, Math.max(required, arr.length+(arr.length>>2)));
        if (hashes.length < required)
            hashes = copyOf(hashes, arr.length);
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
        int base = rows*cols, required = base+cols;
        if (arr.length < required) return false;
        Arrays.fill(arr, base, required, 0);
        if (hashes.length >= base)
            Arrays.fill(hashes, base, Math.min(hashes.length, required), 0);
        offerRowBase = base;
        return true;
    }

    @SuppressWarnings("UnusedReturnValue")
    public boolean offerTerm(int col, long sourcedId) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (col < 0 || col > cols) throw new IndexOutOfBoundsException();
        arr[offerRowBase+col] = sourcedId;
        return true;
    }

    @Override public boolean offerTerm(int col, Term t) {
        if (offerRowBase < 0) throw new IllegalStateException();
        if (TEST_DICT == 0)
            throw new UnsupportedOperationException("putTerm(int, Term) is only supported during testing.");
        putTerm(col, t == null ? 0 : source(lookup(TEST_DICT).find(t), TEST_DICT));
        return true;
    }

    @Override public boolean offerTerm(int destCol, StoreBatch o, int row, int col) {
        if (offerRowBase < 0) throw new IllegalStateException();
        int oCols = o.cols;
        if (row < 0 || col < 0 || row >= o.rows || col >= oCols || destCol < 0 || destCol >= cols)
            throw new IndexOutOfBoundsException();
        arr[offerRowBase+destCol] = o.arr[row*oCols + col];
        return true;
    }

    @Override public boolean commitOffer() {
        if (offerRowBase < 0) throw new IllegalStateException();
        ++rows;
        offerRowBase = -1;
        return true;
    }

    @Override public boolean offerRow(StoreBatch other, int row) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (row < 0 || row >= other.rows) throw new IndexOutOfBoundsException();
        if (beginOffer()) {
            arraycopy(other.arr, row*cols, arr, offerRowBase, cols);
            ++rows;
            offerRowBase = -1;
            return true;
        }
        return false;
    }

    @Override public boolean offer(StoreBatch other) {
        int out = rows * cols, nTerms = other.rows*other.cols;
        if (nTerms > arr.length-out) return false;
        put(other);
        return true;
    }

    @Override public void put(StoreBatch other) {
        int cols = other.cols;
        if (cols != this.cols)
            throw new IllegalArgumentException();
        int oRows = other.rows;
        reserve(oRows, 0);
        int items = oRows * cols;
        arraycopy(other.arr, 0, arr, rows*cols, items);
        arraycopy(other.hashes, 0, hashes, rows*cols,
                Math.min(other.hashes.length, items));
        rows += oRows;
    }

    @Override public void beginPut() {
        reserve(1, 0);
        beginOffer();
    }

    @Override public void putTerm(int destCol, Term t) {
        if (TEST_DICT == 0)
            throw new UnsupportedOperationException("putTerm(int, Term) is only supported during testing.");
        putTerm(destCol, t == null ? 0 : source(lookup(TEST_DICT).find(t), TEST_DICT));
    }
    @Override public void putTerm(int d, StoreBatch b, int r, int c) { offerTerm(d, b, r, c); }
    public           void putTerm(int col, long sourcedId)           { offerTerm(col, sourcedId); }
    @Override public void commitPut()                                { commitOffer(); }

    @Override public void putRow(StoreBatch other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException();
        if (row >= other.rows) throw new IndexOutOfBoundsException();
        reserve(1, 0);
        arraycopy(other.arr, row*other.cols, arr, cols*rows++, cols);
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

    /* --- --- --- operation objects --- --- --- */

    static final class Merger extends BatchMerger<StoreBatch> {
        private long @MonotonicNonNull [] tmpIds;
        private int  @MonotonicNonNull [] tmpHashes;

        public Merger(BatchType<StoreBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public StoreBatch projectInPlace(StoreBatch b) {
            int w = b.cols;
            if (columns == null || columns.length > w) {
                var projected = project(null, b);
                if (projected != b) recycle(b);
                return projected;
            }
            long[] tmpIds = this.tmpIds;
            int[] tmpHashes = this.tmpHashes;
            if (tmpIds == null || tmpIds.length < w)
                this.tmpIds = tmpIds = new long[w];
            if (tmpHashes == null || tmpHashes.length < w)
                this.tmpHashes = tmpHashes = new int[w];
            b.cols = columns.length;
            long[] arr = b.arr;
            int[] hashes = b.hashes;
            for (int in = 0, out = 0, inEnd = w*b.rows; in < inEnd; in += w) {
                arraycopy(arr, in, tmpIds, 0, w);
                arraycopy(hashes, in, tmpHashes, 0, w);
                for (int src : columns) {
                    if (src >= 0) {
                        arr   [out] = tmpIds[src];
                        hashes[out] = tmpHashes[src];
                    }
                    ++out;
                }
            }
            return b;
        }
    }

    static final class Filter extends BatchFilter<StoreBatch> {
        private long @MonotonicNonNull [] tmpIds;
        private int  @MonotonicNonNull [] tmpHashes;

        public Filter(BatchType<StoreBatch> batchType, @Nullable BatchMerger<StoreBatch> projector,
                      RowFilter<StoreBatch> rowFilter) {
            super(batchType, projector, rowFilter);
        }

        private StoreBatch filterInPlaceEmpty(StoreBatch b) {
            int survivors = 0, rows = b.rows;
            for (int r = 0; r < rows; r++) {
                if (!rowFilter.drop(b, r)) ++survivors;
            }
            if (projector != null)
                b.cols = requireNonNull(projector.columns).length;
            b.rows = survivors;
            Arrays.fill(b.arr, 0L);
            Arrays.fill(b.hashes, 0);
            return b;
        }

        @Override public StoreBatch filterInPlace(StoreBatch b, BatchMerger<StoreBatch> projector) {
            long[] arr = b.arr;
            int[] hashes = b.hashes;
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
                    arraycopy(hashes, keep*w, hashes, out, kTerms);
                }
                b.rows = out/w;
            } else {
                long[] tmpIds = this.tmpIds;
                int[] tmpHashes = this.tmpHashes;
                if (tmpIds == null || tmpIds.length < w)
                    this.tmpIds = tmpIds = new long[w];
                if (tmpHashes == null || tmpHashes.length < w)
                    this.tmpHashes = tmpHashes = new int[w];
                // when projecting and filtering, there is no gain in copying regions
                for (int inRowStart = 0; r < rows; ++r, inRowStart += w) {
                    if (rowFilter.drop(b, r)) continue;
                    arraycopy(arr, inRowStart, tmpIds, 0, w);
                    arraycopy(hashes, inRowStart, tmpHashes, 0, w);
                    int required = out+columns.length;
                    if (required > arr.length) {
                        int newLen = Math.max(columns.length*rows, arr.length+(arr.length>>1));
                        b.arr = arr = copyOf(arr, newLen);
                    }
                    if (required > hashes.length)
                        b.hashes = hashes = copyOf(b.hashes, arr.length);
                    for (int src : columns) {
                        if (src >= 0) {
                            arr[out] = tmpIds[src];
                            hashes[out] = tmpHashes[src];
                        } else {
                            arr[out] = 0;
                            hashes[out] = FNV_BASIS;
                        }
                        ++out;
                    }
                }
                b.cols = columns.length;
                b.rows = out/columns.length;
            }
            return b;
        }
    }
}
