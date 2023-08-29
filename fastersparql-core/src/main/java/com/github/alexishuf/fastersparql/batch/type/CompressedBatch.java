package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.isNumericDatatype;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.HAS_UNSAFE;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.I_LEN;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.System.arraycopy;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class CompressedBatch extends Batch<CompressedBatch> {
    static final int SH_SUFF_MASK = 0x80000000;
    static final int     LEN_MASK = 0x7fffffff;
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;

    public static boolean DISABLE_VALIDATE = false;

    /** Storage for local parts of terms. */
    private byte[] locals;
    /** {@code MemorySegment.ofArray(locals)} */
    private MemorySegment localsSeg;

    /**
     * For the term at row {@code r} and column {@code c}:
     * <ul>
     *     <li>index {@code r*slRowInts + (c<<1) + SL_OFF} store the offset into {@code locals} where the local
     *         segment of the term starts.</li>
     *     <li>index {@code r*slRowInts + (c<<1) + SL_LEN} stores the length of the local segment of the
     *         term in bits [0,31) and whether the shared segment comes before (0) or after (1)
     *         at bit 31 (see {@code SH_SUFFIX_MASK})</li>
     * </ul>
     *
     * For every row {@code r}:
     * <ul>
     *     <li>index {@code r*slRowInts + (cols<<1) + SL_OFF} contains the offset into {@code locals}
     *     of the first local byte stored for row {@code r} (note that columns may be stored
     *     out-of-order).</li>
     *     <li>index {@code r*slRowInts + (cols<<1) + SL_LEN} contains the number of local bytes
     *     stored for row {@code r}.c</li>
     * </ul>
     */
    private int[] slices;

    /** Number of ints per row in {@code slices}, i.e., {@code (cols+1)<<1}. */
    private int slRowInts;

    /**
     * Array with the shared segments of all terms in this batch. The segment for term at
     * {@code (row, col)} is stored at index {@code row*cols + col}.
     */
    private SegmentRope[] shared;

    /** {@code -1} if not in a {@link #beginOffer()}/{@link #beginPut()}. Else this is the index
     * into {@code locals} where local bytes for the next column shall be written to. */
    private int offerNextLocals = -1;
    /** {@code col} in last {@code offerTerm}/{@code putTerm} call in the current
     *  {@link #beginOffer()}/{@link #beginPut()}, else {@code -1}. */
    private int offerLastCol = -1;


    /* --- --- --- helpers --- --- --- */

    @SuppressWarnings("unused") String dump() {
        var sb = new StringBuilder();
        sb.append(String.format("""
                CompressedBatch{
                  rows=%d, cols=%d, slRowInts=%d
                  offerNextLocals=%d, offerLastCol=%d
                """,
                rows, cols, slRowInts, offerNextLocals, offerLastCol));
        int dumpRows = offerNextLocals == -1 ? rows : rows+1;
        for (int r = 0; r < dumpRows; r++) {
            sb.append("  row=").append(r)
                    .append(", off=").append(slices[(r+1)*slRowInts-2+SL_OFF])
                    .append(", len=").append(slices[(r+1)*slRowInts-2+SL_LEN])
                    .append('\n');
            for (int c = 0; c < cols; c++) {
                int off        = slices[r*slRowInts + (c<<1) + SL_OFF];
                int flaggedLen = slices[r*slRowInts + (c<<1) + SL_LEN];
                sb.append("    col=").append(c)
                        .append(", sh=").append(shared[r*cols+c])
                        .append(", locals[").append(off)
                        .append(":+").append(flaggedLen&LEN_MASK).append(']');
                if ((flaggedLen&SH_SUFF_MASK) != 0)
                    sb.append("(suffixed)");
                sb.append('=')
                        .append(new String(locals, off, flaggedLen&LEN_MASK, UTF_8))
                        .append('\n');
            }
        }
        return sb.append('}').toString();
    }

    boolean validate() {
        if (cols == 0 || DISABLE_VALIDATE)
            return true;
        if (rows < 0 || cols < 0)
            return false;
        Term view = Term.pooledMutable();
        try {
            int lastRowAlignedEnd = 0;
            for (int r = 0; r < rows; r++) {
                int rOff = slices[slRowBase(r) + SL_OFF];
                int rEnd = rOff + slices[slRowBase(r) + SL_LEN];
                if (rEnd < rOff)
                    return false;
                if (rEnd > rOff && rOff < lastRowAlignedEnd)
                    return false;
                int rowAlignedEnd = rOff + localBytesUsed(r);
                if (rEnd > rowAlignedEnd)
                    return false;
                for (int c = 0; c < cols; c++) {
                    int off = slices[r * slRowInts + (c << 1) + SL_OFF];
                    int len = slices[r * slRowInts + (c << 1) + SL_LEN];
                    if (len > 0 && off < lastRowAlignedEnd)
                        return false;
                    if (len > 0 && off+len > rowAlignedEnd)
                        return false;
                    Term term = getView(r, c, view) ? view : null;
                    if (!equals(r, c, term) || !equals(r, c, this, r, c))
                        return false;
                }
                if (!equals(r, this, r))
                    return false;
                lastRowAlignedEnd = Math.max(lastRowAlignedEnd, rowAlignedEnd);
            }
        } finally {
            view.recycle();
        }
        return true;
    }

    private void growLocals(int required) {
        byte[] locals = this.locals;
        this.locals = locals = grow(locals, required);
        this.localsSeg = MemorySegment.ofArray(locals);
        BatchEvent.Grown.record(this);
    }

    private int slBase(int row, int col) {
        requireUnpooled();
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row, col));
        return row* slRowInts + (col<<1);
    }
    private int slRowBase(int row) {
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row));
        return row*slRowInts + (cols<<1);
    }

    private static final int SL_ALIGN = I_LEN;
    private static final int SL_FLOOR = -(SL_ALIGN); // ~(SL_ALIGN-1)
    private static int slAlign(int size) {
        int withPutVectorSlack = size + I_LEN; // avoids some bounds checking in vectorized put(B)
        return (withPutVectorSlack&SL_FLOOR) + SL_ALIGN; // branchless roundup to I_LEN
    }

    /**
     * Tries to set a term at (rows, offerCol) and if succeeds increments {@code offerCol}.
     *
     * @param forbidGrow if true, this call will produce no side effects and will return
     *                   {@code false} if setting the term would require locals to be re-allocated
     * @param shared A shared suffix/prefix of the term to be kept by reference
     * @param flaggedLocalLen length (in bytes) of the term local part, possibly {@code |}'ed
     *                        with {@code SH_SUFFIX_MASK}
     * @return the offset into {@code this.locals} where {@code flaggedLocalLen&LEN_MASK} bytes
     *         MUST be copied after this method return, or {@code -1} if {@code forbidGrow}
     *         and there was not enough space in {@code this.locals}
     */
    private int allocTerm(boolean forbidGrow, int destCol, SegmentRope shared,
                          int flaggedLocalLen) {
        if (offerNextLocals < 0) throw new IllegalStateException();
        if (destCol < 0 || destCol >= cols)
            throw new IndexOutOfBoundsException("destCol="+destCol+", cols="+cols);
        // find write location in md and grow if needed
        int slBase = rows*slRowInts + (destCol<<1), dest = offerNextLocals;
        if (slices[slBase+SL_OFF] != 0)
            throw new IllegalStateException("Column already set");
        int len = flaggedLocalLen & LEN_MASK;
        if (len > 0) {
            offerLastCol = destCol;
            int required  = dest+len;
            if (required > locals.length) {
                if (forbidGrow) return -1;
                growLocals(required);
            }
            offerNextLocals = dest+len;
        } else if (shared != null && shared.len > 0) {
            throw new IllegalArgumentException("Empty local with non-empty shared");
        }
        this.shared[rows*cols + destCol] = shared != null && shared.len == 0 ? null : shared;
        slices[slBase+SL_OFF] = dest;
        slices[slBase+SL_LEN] = flaggedLocalLen;
        return dest;
    }

    /* --- --- --- lifecycle --- --- --- */

    CompressedBatch(int rowsCapacity, int cols, int bytesCapacity) {
        super(0, cols);
        this.locals = bytesAtLeast(Math.max(bytesCapacity, 16));
        this.localsSeg = MemorySegment.ofArray(this.locals);
        int slRowInts = (cols + 1) << 1;
        this.slRowInts = slRowInts;
        this.slices = intsAtLeast(slAlign(rowsCapacity*slRowInts));
        this.shared = segmentRopesAtLeast(rowsCapacity*cols);
        BatchEvent.Created.record(this);
    }

    public void recycleInternals() {
        rows            = 0;
        offerNextLocals = -1;
        offerLastCol    = -1;
        if ((slices = INT.offer(slices, slices.length)) == null)
            slices = EMPTY_INT;
        if ((locals = BYTE.offer(locals, locals.length)) == null) {
            locals = EMPTY.utf8;
            localsSeg = EMPTY.segment;
        }
        if ((shared = SEG_ROPE.offer(shared, shared.length)) == null)
            shared = EMPTY_SEG_ROPE;
    }

    void hydrate(int rows, int cols, int bytes) {
        byte[] locals = bytesAtLeast(bytes, this.locals);
        if (locals != this.locals) {
            this.locals = locals;
            this.localsSeg = MemorySegment.ofArray(this.locals);
        }
        this.rows       = 0;
        this.cols       = cols;
        this.slRowInts = (cols+1)<<1;
        this.offerNextLocals = -1;
        this.offerLastCol    = -1;
        this.shared  = segmentRopesAtLeast(rows*cols, this.shared);
        this.slices  = intsAtLeast(slAlign(rows*slRowInts), this.slices);
    }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public CompressedBatchType type() { return COMPRESSED; }

    public CompressedBatch copy(@Nullable CompressedBatch dest) {
        int bytes = localBytesUsed();
        dest = COMPRESSED.reserved(dest, rows, cols, bytes);
        arraycopy(locals, 0, dest.locals, 0, bytes);
        arraycopy(slices, 0, dest.slices, 0, rows*slRowInts);
        arraycopy(shared, 0, dest.shared, 0, rows*cols);
        dest.rows = rows;
        dest.cols = cols;
        return dest;
    }

    @Override public int localBytesUsed() {
        if (rows == 0) return 0;
        int base = slRowBase(rows - 1);
        return slices[base+SL_OFF] + slices[base+SL_LEN];
    }

    public int localsFreeCapacity() {
        return locals.length- localBytesUsed();
    }

    @Override public int directBytesCapacity() {
        return ((slices.length + shared.length)<<2) + locals.length;
    }

    @Override public int rowsCapacity() {
        return cols == 0 ? Integer.MAX_VALUE : shared.length/cols;
    }

    @Override public boolean hasCapacity(int rowsCapacity, int bytesCapacity) {
        return cols == 0 || (shared.length/cols >= rowsCapacity && locals.length >= bytesCapacity);
    }

    @Override public boolean hasMoreCapacity(CompressedBatch other) {
        if (locals.length <= other.locals.length) return false;
        return rowsCapacity() > other.rowsCapacity();
    }

    /* --- --- --- row-level accessors --- --- --- */

    @Override public int hash(int row) {
        if (cols == 0)
            return FNV_BASIS;
        if (!HAS_UNSAFE)
            return safeHash(row);
        int h = 0, termHash;
        int slb = slBase(row, 0);
        int[] slices = this.slices;
        for (int c = 0, cols = this.cols; c < cols; c++) {
            int cslb = slb+(c<<1), fstLen, sndLen = slices[cslb+SL_LEN];
            long fstOff, sndOff = slices[cslb+SL_OFF];
            var sh = shared[row*cols+c];
            if (isNumericDatatype(sh)) {
                termHash = hashTerm(row, c);
            } else if (sh == null) {
                termHash = SegmentRope.hashCode(FNV_BASIS, locals, sndOff, sndLen&LEN_MASK);
            } else {
                byte[] fst, snd;
                long shOff = sh.segment.address() + sh.offset;
                if ((sndLen & SH_SUFF_MASK) == 0) {
                    fst = sh.utf8; fstOff = shOff; fstLen = sh.len;
                    snd = locals;                  sndLen &= LEN_MASK;
                } else {
                    fst =  locals; fstOff = sndOff; fstLen = sndLen &LEN_MASK;
                    snd = sh.utf8; sndOff =  shOff; sndLen = sh.len;
                }
                termHash = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
                termHash = SegmentRope.hashCode(termHash,  snd, sndOff, sndLen);
            }
            h ^= termHash;
        }
        return h;
    }

    private int safeHash(int row) {
        int h = 0, slb = slBase(row, 0);
        int[] slices = this.slices;
        for (int c = 0, cols = this.cols; c < cols; c++) {
            var sh = shared[row*cols+c];
            if (isNumericDatatype(sh)) {
                h ^= hashTerm(row, c);
            } else {
                int base = slb+(c<<1);
                h ^= safeHashString(sh, slices[base+SL_OFF], slices[base+SL_LEN]);
            }
        }
        return h;
    }

    @Override public int localBytesUsed(int row) {
        return cols == 0 ? 0 : slices[slRowBase(row)+SL_LEN];
    }

    @Override public boolean equals(int row, CompressedBatch other, int oRow) {
        if (!HAS_UNSAFE)
            return safeEquals(row, other, oRow);
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row));
        if (oRow < 0 || oRow >= other.rows)
            throw new IndexOutOfBoundsException(other.mkOutOfBoundsMsg(oRow));
        int cols = this.cols;
        if (cols != other.cols) return false;
        if (cols == 0) return true;
        MemorySegment localsSeg = this.localsSeg, oLocalsSeg = other.localsSeg;
        byte[] locals = this.locals, oLocals = other.locals;
        SegmentRope[] shared = this.shared, oshared = other.shared;
        int[] sl = this.slices, osl = other.slices;
        int slb = row*slRowInts, oslb = oRow*slRowInts;
        int shBase = row*cols, oShBase = oRow*cols;
        for (int c = 0, c2, cslb, ocslb; c < cols; ++c) {
            SegmentRope sh = shared[shBase+c], osh = oshared[oShBase+c];
            cslb = slb + (c2 = c << 1);
            ocslb = oslb + c2;
            if (!termEquals( sh,  localsSeg,  locals,  sl[ cslb+SL_OFF],  sl[ cslb+SL_LEN],
                            osh, oLocalsSeg, oLocals, osl[ocslb+SL_OFF], osl[ocslb+SL_LEN]))
                return false;
        }
        return true;
    }

    private boolean safeEquals(int row, CompressedBatch other, int oRow) {
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row));
        if (oRow < 0 || oRow >= other.rows)
            throw new IndexOutOfBoundsException(other.mkOutOfBoundsMsg(oRow));
        int cols = this.cols;
        if (cols != other.cols) return false;
        if (cols == 0) return true;
        MemorySegment localsSeg = this.localsSeg, oLocalsSeg = other.localsSeg;
        byte[] locals = this.locals, oLocals = other.locals;
        SegmentRope[] shared = this.shared, oshared = other.shared;
        int[] sl = this.slices, osl = other.slices;
        int slb = row*slRowInts, oslb = oRow*slRowInts;
        int shBase = row*cols, oShBase = oRow*cols;
        for (int c = 0, c2, cslb, ocslb; c < cols; ++c) {
            SegmentRope sh = shared[shBase+c], osh = oshared[oShBase+c];
            cslb = slb + (c2 = c << 1);
            ocslb = oslb + c2;
            if (!safeTermEquals( sh,  localsSeg,  locals,  sl[ cslb+SL_OFF],  sl[ cslb+SL_LEN],
                                osh, oLocalsSeg, oLocals, osl[ocslb+SL_OFF], osl[ocslb+SL_LEN]))
                return false;
        }
        return true;
    }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        int i2 = slBase(row, col), len = slices[i2+SL_LEN];
        boolean suffix = len < 0;
        len &= LEN_MASK;
        SegmentRope sh = shared[row*cols + col];
        if (sh == null) {
            if (len == 0) return null;
            sh = EMPTY;
        }
        int off = slices[i2 + SL_OFF];
        var localCopy = new SegmentRope(copyOfRange(locals, off, off+len), 0, len);
        return new Term(sh, localCopy, suffix);
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, Term dest) {
        int i2 = slBase(row, col), len = slices[i2+SL_LEN];
        boolean suffix = len < 0;
        len &= LEN_MASK;
        SegmentRope sh = shared[row*cols + col];
        if (sh == null) {
            if (len == 0) return false;
            sh = EMPTY;
        }
        dest.set(sh, localsSeg, locals, slices[i2+SL_OFF], len, suffix);
        return true;
    }

    @Override public @Nullable TwoSegmentRope getRope(@NonNegative int row, @NonNegative int col) {
        int i2 = slBase(row, col), len = slices[i2+SL_LEN];
        boolean suffix = len < 0;
        len &= LEN_MASK;
        SegmentRope sh = shared[row*cols + col];
        if (sh == null) {
            if (len == 0)
                return null;
            sh = EMPTY;
        }
        TwoSegmentRope tsr = new TwoSegmentRope();
        tsr.wrapFirst(sh);
        int off = slices[i2+SL_OFF];
        byte[] u8 = copyOfRange(locals, off, off + len);
        tsr.wrapSecond(MemorySegment.ofArray(u8), u8, 0, len);
        if (suffix)
            tsr.flipSegments();
        return tsr;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        int i2 = slBase(row, col), len = slices[i2+SL_LEN];
        boolean suffix = len < 0;
        len &= LEN_MASK;
        SegmentRope sh = shared[row*cols+col];
        if (sh == null) {
            if (len == 0) return false;
            sh = EMPTY;
        }
        dest.wrapFirst(sh);
        dest.wrapSecond(localsSeg, locals, slices[i2+SL_OFF], len);
        if (suffix)
            dest.flipSegments();
        return true;
    }

    @Override
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRope dest) {
        int base = slBase(row, col), len = slices[base+SL_LEN]&LEN_MASK;
        if (len == 0) return false;
        dest.wrapSegment(localsSeg, locals, slices[base+SL_OFF], len);
        return true;
    }

    SegmentRope sharedUnchecked(@NonNegative int row, @NonNegative int col) {
        SegmentRope sh = shared[row * cols + col];
        return sh == null ? EMPTY : sh;
    }

    @Override public @NonNull SegmentRope shared(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || row >= rows || col < 0 || col >= cols)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row, col));
        SegmentRope sh = shared[row*cols + col];
        return sh == null ? EMPTY : sh;
    }

    @Override public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        return (slices[slBase(row, col) + SL_LEN] & SH_SUFF_MASK) != 0;
    }

    @Override public int len(@NonNegative int row, @NonNegative int col) {
        SegmentRope sh = shared[row * cols + col];
        return (slices[slBase(row, col) + SL_LEN] & LEN_MASK)
                + (sh == null ? 0 : sh.len);
    }

    @Override public int lexEnd(@NonNegative int row, @NonNegative int col) {
        int slBase = slBase(row, col), localLen = slices[slBase+SL_LEN];
        if (localLen < 0) {
            localLen &= LEN_MASK;
            SegmentRope sh = shared[row * cols + col];
            if (sh != null)
                return localLen; // suffixed literal
        }
        int off = slices[slBase + SL_OFF];
        if (localLen == 0 || locals[off] != '"') return 0; // not a literal
        var tmp = pooledWrap(localsSeg, locals, off, localLen);
        int lexEnd = tmp.reverseSkipUntil(0, localLen, '"');
        tmp.recycle();
        return lexEnd;
    }

    int copyLocal(@NonNegative int row, @NonNegative int col, byte[] dst, int dstPos) {
        int b = row * slRowInts + (col << 1), len = slices[b + SL_LEN] & LEN_MASK;
        arraycopy(locals, slices[b+SL_OFF], dst, dstPos, len);
        return len;
    }

    int flaggedLen(@NonNegative int row, @NonNegative int col) {
        return slices[row*slRowInts + (col<<1) + SL_LEN];
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        return slices[slBase(row, col)+SL_LEN] & LEN_MASK;
    }

    @Override public Term.@Nullable Type termType(int row, int col) {
        int slBase = slBase(row, col), len = slices[slBase + SL_LEN];
        if      (len <  0) return Term.Type.LIT; // suffixed
        else if (len == 0) return null;
        var sh = shared[row*cols + col];
        byte f = sh == null ? locals[slices[slBase+SL_OFF]] : sh.get(0);
        return switch (f) {
            case '"'      -> Term.Type.LIT;
            case '_'      -> Term.Type.BLANK;
            case '<'      -> Term.Type.IRI;
            case '?', '$' -> Term.Type.VAR;
            default       -> throw new IllegalStateException();
        };
    }

    @Override public @Nullable SegmentRope asDatatypeSuff(int row, int col) {
        int slBase = slBase(row, col), len = slices[slBase+SL_LEN];
        if (len <= 0)
            return null; // not an IRI
        int off = slices[slBase+SL_OFF];
        var sh = this.shared[row*cols + col];
        if (sh == SharedRopes.P_XSD) {
            for (int i = 0; i < Term.FREQ_XSD_DT.length; i++) {
                var c = Term.FREQ_XSD_DT[i].local();
                if (c.len == len && compare1_1(localsSeg, off, len, c.segment, c.offset, c.len) == 0)
                    return Term.FREQ_XSD_DT_SUFF[i];
            }
        } else if (sh == SharedRopes.P_RDF) {
            if (len == 5) { //HTML or JSON
                var c = Term.RDF_HTML.local();
                if (compare1_1(localsSeg, off, len, c.segment, c.offset, len) == 0)
                    return SharedRopes.DT_langString;
                c = Term.RDF_JSON.local();
                if (compare1_1(localsSeg, off, len, c.segment, c.offset, len) == 0)
                    return SharedRopes.DT_XMLLiteral;
            } else if (len == 11) { // langString or XMLLiteral
                var c = Term.RDF_LANGSTRING.local();
                if (compare1_1(localsSeg, off, len, c.segment, c.offset, len) == 0)
                    return SharedRopes.DT_langString;
                c = Term.RDF_XMLLITERAL.local();
                if (compare1_1(localsSeg, off, len, c.segment, c.offset, len) == 0)
                    return SharedRopes.DT_XMLLiteral;
            }
        }
        Term tmp = Term.pooledMutable();
        tmp.set(sh, localsSeg, locals, off, len, false);
        SegmentRope suff = tmp.asDatatypeSuff();
        tmp.recycle();
        return suff;
    }

    @Override public @Nullable Term datatypeTerm(int row, int col) {
        int slBase = slBase(row, col), len = slices[slBase+SL_LEN];
        if (len > 0 || len == 0) return null; // prefixed IRI or empty
        len &= LEN_MASK;
        var sh = shared[row*cols+col];
        if (sh != null && sh.len > 1)  {
            if (sh.get(1) == '@') return Term.RDF_LANGSTRING;
            return Term.splitAndWrap(sh.sub(3/*"^^*/, sh.len));
        }
        int off = slices[slBase+SL_OFF], end = off+len;
        if (locals[off] != '"')
            return null; // not a literal
        SegmentRope tmp = pooledWrap(localsSeg, locals, 0, locals.length);
        int i = tmp.reverseSkipUntil(off, end, '"');
        tmp.recycle();
        if      (i+1 == end)
            return Term.XSD_STRING;
        else if (locals[i+1] == '@')
            return Term.RDF_LANGSTRING;
        else if (end-i > 5 /*"^^<x>*/)
            return Term.splitAndWrap(SegmentRope.pooledWrap(localsSeg, locals, i+3, end-(i+3)));
        else
            throw new InvalidTermException(this, i, "Unexpected literal suffix");
    }

    @Override
    public int writeSparql(ByteSink<?, ?> dest, int row, int col, PrefixAssigner prefixAssigner) {
        int base = slBase(row, col), len = slices[base + SL_LEN];
        boolean suffix = len < 0;
        len &= LEN_MASK;
        var sh = shared[row*cols + col];
        if (len != 0 || sh != null) {
            return Term.toSparql(dest, prefixAssigner, sh, localsSeg, locals,
                                 slices[base+SL_OFF], len, suffix);
        }
        return 0;
    }

    @Override public void writeNT(ByteSink<?, ?> dest, int row, int col) {
        int base = slBase(row, col), len = slices[base+SL_LEN], off = slices[base+SL_OFF];
        var sh = shared[row*cols + col];
        if (len < 0) {
            len &= LEN_MASK;
            if (len > 0) dest.append(locals, off, len);
            if (sh != null) dest.append(sh);
        } else {
            if (sh != null) dest.append(sh);
            if (len > 0) dest.append(locals, off, len);
        }
    }

    @Override public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        int base = slBase(row, col);
        MemorySegment fst = localsSeg, snd = EMPTY.segment;
        byte[] fstU8 = locals, sndU8 = fstU8;
        long fstOff = slices[base+SL_OFF], sndOff = 0;
        int fstLen  = slices[base+SL_LEN], sndLen = 0;
        var sh = shared[row*cols + col];
        if (sh != null) {
            if (fstLen < 0) { snd = sh.segment; sndU8 = sh.utf8; sndOff = sh.offset; sndLen = sh.len; }
            else            { snd = fst;                         sndOff = fstOff;    sndLen = fstLen&LEN_MASK;
                              fst = sh.segment; fstU8 = sh.utf8; fstOff = sh.offset; fstLen = sh.len; }
        }
        fstLen &= LEN_MASK;
        if (begin < 0 || end > (fstLen+sndLen))
            throw new IndexOutOfBoundsException(begin);
        if (fstLen + sndLen == 0) return;

        if (begin < fstLen)
            dest.append(fst, fstU8, fstOff+begin, Math.min(fstLen, end)-begin);
        if (end > fstLen) {
            begin = Math.max(0, begin-fstLen);
            dest.append(snd, sndU8, sndOff+begin, Math.max(0, (end-fstLen)-begin));
        }
    }

    private int hashTerm(int row, int col) {
        Term tmp = Term.pooledMutable();
        int h = getView(row, col, tmp) ? tmp.hashCode() : FNV_BASIS;
        tmp.recycle();
        return h;
    }

    @Override public int hash(int row, int col) {
        SegmentRope sh = shared[row*cols + col];
        if (isNumericDatatype(sh))
            return hashTerm(row, col);
        int slb = slBase(row, col), fstLen, sndLen = slices[slb+SL_LEN];
        long fstOff, sndOff = slices[slb+SL_OFF];
        if (HAS_UNSAFE) {
            if (sh == null)
                return SegmentRope.hashCode(FNV_BASIS, locals, sndOff, sndLen & LEN_MASK);
            byte[] fst, snd;
            long shOff = sh.segment.address() + sh.offset;
            if ((sndLen & SH_SUFF_MASK) == 0) {
                fst = sh.utf8; fstOff = shOff; fstLen = sh.len;
                snd = locals;                  sndLen &= LEN_MASK;
            } else {
                fst =  locals; fstOff = sndOff; fstLen = sndLen &LEN_MASK;
                snd = sh.utf8; sndOff =  shOff; sndLen = sh.len;
            }
            int h = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
            return SegmentRope.hashCode(h, snd, sndOff, sndLen);
        } else {
            return safeHashString(sh, sndOff, sndLen);
        }
    }

    private int safeHashString(SegmentRope shared, long localOff, int localLen) {
        long fstOff;
        int fstLen;
        if (shared == null)
            return SegmentRope.hashCode(com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS, localsSeg, localOff, localLen&LEN_MASK);
        MemorySegment fst, snd;
        if ((localLen & SH_SUFF_MASK) == 0) {
            fst = shared.segment; fstOff = shared.offset;    fstLen = shared.len;
            snd = localsSeg;                              localLen &= LEN_MASK;
        } else {
            fst = localsSeg;        fstOff = localOff;        fstLen = localLen&LEN_MASK;
            snd = shared.segment; localOff = shared.offset; localLen = shared.len;
        }
        int h = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
        return SegmentRope.hashCode(h, snd, localOff, localLen);
    }

    @Override public boolean equals(@NonNegative int row, @NonNegative int col,
                                    @Nullable Term other) {
        if (!HAS_UNSAFE)
            return safeEquals(row, col, other);
        int[] slices = this.slices;
        int slb = slBase(row, col), len = slices[slb + SL_LEN];
        SegmentRope sh = shared[row * cols + col];
        if (sh == null)
            sh = EMPTY;
        if (other == null != (sh.len == 0 && (len&LEN_MASK) == 0))
            return false;
        else if (other == null)
            return true;
        SegmentRope ol = other.local();
        return termEquals(sh, localsSeg, locals, slices[slb+SL_OFF], len,
                          other.shared(), ol.segment, ol.utf8, ol.offset,
                          ol.len|(other.sharedSuffixed() ? SH_SUFF_MASK : 0));
    }

    public boolean safeEquals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        int[] slices = this.slices;
        int slb = slBase(row, col), len = slices[slb + SL_LEN];
        SegmentRope sh = shared[row * cols + col];
        if (sh == null)
            sh = EMPTY;
        if (other == null != (sh.len == 0 && (len&LEN_MASK) == 0))
            return false;
        else if (other == null)
            return true;
        SegmentRope ol = other.local();
        return safeTermEquals(sh, localsSeg, locals, slices[slb+SL_OFF], len,
                other.shared(), ol.segment, ol.utf8, ol.offset,
                ol.len|(other.sharedSuffixed() ? SH_SUFF_MASK : 0));
    }

    public boolean termEquals(@Nullable SegmentRope lSh, MemorySegment lSeg, byte[] lU8,
                              long lOff, int lLen,
                              @Nullable SegmentRope rSh, MemorySegment rSeg, byte[] rU8,
                              long rOff, int rLen) {
        boolean numeric = isNumericDatatype(lSh);
        if (numeric != isNumericDatatype(rSh))
            return false;
        lLen&=LEN_MASK;
        rLen&=LEN_MASK;
        if (lSh == null) lSh = EMPTY;
        if (rSh == null) rSh = EMPTY;
        if (numeric) {
            Term lTerm = Term.pooledMutable(), rTerm = Term.pooledMutable();
            lTerm.set(lSh, lSeg, lU8, lOff, lLen, true);
            rTerm.set(rSh, rSeg, rU8, rOff, rLen, true);
            boolean eq = lTerm.compareNumeric(rTerm) == 0;
            lTerm.recycle();
            rTerm.recycle();
            return eq;
        }
        return compare2_2(lSh.utf8, lSh.segment.address()+lSh.offset, lSh.len,
                          lU8, lOff, lLen,
                          rSh.utf8, rSh.segment.address()+rSh.offset, rSh.len,
                          rU8, rOff, rLen) == 0;
    }

    public boolean safeTermEquals(@Nullable SegmentRope lSh, MemorySegment lSeg, byte[] lU8,
                                  long lOff, int lLen,
                                  @Nullable SegmentRope rSh, MemorySegment rSeg, byte[] rU8,
                                  long rOff, int rLen) {
        boolean numeric = isNumericDatatype(lSh);
        if (numeric != isNumericDatatype(rSh))
            return false;
        lLen&=LEN_MASK;
        rLen&=LEN_MASK;
        if (lSh == null) lSh = EMPTY;
        if (rSh == null) rSh = EMPTY;
        if (numeric) {
            Term lTerm = Term.pooledMutable(), rTerm = Term.pooledMutable();
            lTerm.set(lSh, lSeg, lU8, lOff, lLen, true);
            rTerm.set(rSh, rSeg, rU8, rOff, rLen, true);
            boolean eq = lTerm.compareNumeric(rTerm) == 0;
            lTerm.recycle();
            rTerm.recycle();
            return eq;
        }
        return compare2_2(lSh.segment, lSh.offset, lSh.len, lSeg, lOff, lLen,
                          rSh.segment, rSh.offset, rSh.len, rSeg, rOff, rLen) == 0;
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col,
                          CompressedBatch other, int oRow, int oCol) {
        int slb = slBase(row, col), oslb = other.slBase(oRow, oCol);
        int[] sl = this.slices, osl = other.slices;
        if (HAS_UNSAFE) {
            return termEquals(shared[row*cols+col], localsSeg, locals,
                              sl[slb+SL_OFF], sl[slb+SL_LEN],
                              other.shared[oRow*other.cols+oCol], other.localsSeg, other.locals,
                              osl[oslb+SL_OFF], osl[oslb+SL_LEN]);
        } else {
            return safeTermEquals(shared[row*cols+col], localsSeg, locals,
                                  sl[slb+SL_OFF], sl[slb+SL_LEN],
                                  other.shared[oRow*other.cols+oCol], other.localsSeg, other.locals,
                                  osl[oslb+SL_OFF], osl[oslb+SL_LEN]);
        }
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void reserve(int additionalRows, int additionalBytes) {
        boolean grown = false;
        int reqRows = rows+additionalRows, req;
        if ((req =reqRows*cols) > shared.length) {
            grown = true;
            shared = grow(shared, req);
        }
        if ((req = slAlign(reqRows*slRowInts)) > slices.length) {
            grown = true;
            slices = grow(slices, req);
        }
        if ((req = localBytesUsed()+additionalBytes) > locals.length)  {
            grown = false;
            growLocals(req);
        }
        if (grown)
            BatchEvent.Grown.record(this);
    }

    @Override public @Nullable CompressedBatch recycle() {
        return CompressedBatchType.INSTANCE.recycle(this);
    }

    @Override public void clear() {
        rows            =  0;
        offerNextLocals = -1;
        offerLastCol    = -1;
        if (slices == null)
            slices = intsAtLeast(slAlign(slRowInts));
        if (shared == null)
            shared = segmentRopesAtLeast(cols);
    }

    @Override public void clear(int newColumns) {
        this.rows            =  0;
        this.offerNextLocals = -1;
        this.offerLastCol    = -1;
        this.cols      = newColumns;
        int slRowInts = (newColumns+1) << 1;
        this.slRowInts = slRowInts;
        this.slices    = intsAtLeast(slAlign(slRowInts), slices);
        this.shared    = segmentRopesAtLeast(newColumns, shared);
    }

    @Override public boolean beginOffer() {
        if (rowsCapacity() <= rows) return false;
        beginPut();
        return true;
    }

    @Override public boolean offerTerm(int col, Term t) {
        if (t == null) return true;
        var local = t.local();
        int dest = allocTerm(true, col, t.shared(),
                             local.len | (t.sharedSuffixed() ? SH_SUFF_MASK : 0));
        if (dest < 0) return !abortOfferOrPut();
        local.copy(0, local.len, locals, dest);
        return true;
    }

    @Override public boolean offerTerm(int destCol, CompressedBatch other, int oRow, int oCol) {
        int oBase = other.slBase(oRow, oCol);
        int[] oSl = other.slices;
        int fLen = oSl[oBase + SL_LEN];
        int dest = allocTerm(true, destCol, other.shared[oRow*other.cols+oCol], fLen);
        if (dest < 0) return !abortOfferOrPut();
        arraycopy(other.locals, oSl[oBase+SL_OFF], locals, dest, fLen&LEN_MASK);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, MemorySegment local, long localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return !abortOfferOrPut();
        MemorySegment.copy(local, JAVA_BYTE, localOff, locals, dest, localLen);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, byte[] local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return !abortOfferOrPut();
        arraycopy(local, localOff, locals, dest, localLen);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, SegmentRope local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return !abortOfferOrPut();
        local.copy(localOff, localOff+localLen, locals, dest);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return !abortOfferOrPut();
        local.copy(localOff, localOff+localLen, locals, dest);
        return true;
    }

    @Override public boolean commitOffer() {
        if (offerNextLocals < 0) throw new IllegalStateException();
        int base = (rows+1) * slRowInts - 2;
        int bytesUsed = localBytesUsed();
        slices[base+SL_OFF] = bytesUsed;
        slices[base+SL_LEN] = offerNextLocals - bytesUsed;
        ++rows;
        offerNextLocals = -1;
        offerLastCol = -1;
        assert validate() : "corrupted";
        return true;
    }

    @Override public boolean abortOfferOrPut() throws IllegalStateException {
        if (offerNextLocals < 0)
            return false; // not inside an uncommitted offer/put
        fill(locals, localBytesUsed(), locals.length, (byte)0);
        offerLastCol = -1;
        offerNextLocals = -1;
        return true;
    }

    @Override public boolean offerRow(CompressedBatch o, int row) {
        if ((rows+1)*slRowInts                      > slices.length) return false;
        if (o.localBytesUsed(row)+ localBytesUsed() > locals.length) return false;

        int cols = this.cols;
        if (cols != o.cols) throw new IllegalArgumentException("o.cols != cols");
        if (row  >  o.rows) throw new IndexOutOfBoundsException(o.mkOutOfBoundsMsg(row));
        int[] osl = o.slices, sl = this.slices;
        int oBase = row*o.slRowInts, lLen = o.localBytesUsed(row), lSrc = osl[oBase+(cols<<1) + SL_OFF];
        int base = rows*slRowInts, end = base+(cols<<1), lDst = localBytesUsed();
        for (int i = base, offAdj = lDst-lSrc, iAdj = oBase-base; i < end; i += 2) {
            sl[i+SL_OFF] = osl[i+iAdj+SL_OFF] + offAdj;
            sl[i+SL_LEN] = osl[i+iAdj+SL_LEN];
        }
        SegmentRope[] osh = o.shared;
        for (int i = 0, b = rows*cols, ob = row*cols; i < cols; i++)
            shared[b+i] = osh[ob+i];
        arraycopy(o.locals, lSrc, locals, lDst, lLen);
        slices[end+SL_OFF] = lDst;
        slices[end+SL_LEN] = lLen;
        ++rows;
        assert validate() : "corrupted";
        return true;
    }

    @Override public boolean fits(CompressedBatch o) {
        int nRows = rows+o.rows;
        return nRows*slRowInts <= slices.length
                && localBytesUsed()+o.localBytesUsed() <= locals.length
                && (nRows*cols) <= shared.length;
    }

    @Override public boolean offer(CompressedBatch o) {
        if ((rows+o.rows)*slRowInts     > slices.length) return false;
        if (localBytesUsed()+o.localBytesUsed() > locals.length) return false;
        put(o);
        return true;
    }

    @Override public void beginPut() {
        offerLastCol = -1;
        offerNextLocals = localBytesUsed();

        boolean grown = false;
        int reqRows = rows+1,  req;
        if ((req = reqRows*cols     ) > shared.length) { grown = true; shared = grow(shared, req); }
        fill(shared, req-cols, req, null);

        if ((req = reqRows*slRowInts) > slices.length) { grown = true; slices = grow(slices, req); }
        fill(slices, req-slRowInts, req, 0);
        if (grown) BatchEvent.Grown.record(this);
    }

    @Override public void putTerm(int col, Term t) {
        SegmentRope shared, local;
        if (t == null) { shared =      EMPTY; local =     EMPTY; }
        else           { shared = t.shared(); local = t.local(); }
        int fLen = local.len | (t != null && t.sharedSuffixed() ? SH_SUFF_MASK : 0);
        int dest = allocTerm(false, col, shared, fLen);
        local.copy(0, local.len, locals, dest);
    }

    @Override public void putTerm(int destCol, CompressedBatch other, int row, int col) {
        int[] oSl = other.slices;
        int oBase = other.slBase(row, col), fLen = oSl[oBase + SL_LEN];
        int dest = allocTerm(false, destCol, other.shared[row*other.cols+col], fLen);
        arraycopy(other.locals, oSl[oBase+SL_OFF], locals, dest, fLen&LEN_MASK);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, MemorySegment local, long localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(false, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        MemorySegment.copy(local, JAVA_BYTE, localOff, locals, dest, localLen);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, byte[] local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(false, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        arraycopy(local, localOff, locals, dest, localLen);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, SegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        int dest = allocTerm(false, col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, locals, dest);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(false, col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, locals, dest);
    }

    @Override public void commitPut() { commitOffer(); }

//    /** Mask true for SL_OFF and false for SL_LEN */
//    private static final VectorMask<Integer> PUT_MASK = fromLong(I_SP, 0x5555555555555555L);
//    static {
//        if (SL_OFF != 0) throw new AssertionError("PUT_MASK misaligned with SL_OFF");
//    }
//
//    public void put(CompressedBatch o) {
//        // handle special cases
//        int oRows = o.rows, cols = this.cols;
//        if (oRows == 0) return;
//        if (slRowInts != o.slRowInts || o.cols != cols)
//            throw new IllegalArgumentException("cols != o.cols");
//
//        int dst = slRowInts*rows, slLen = oRows*slRowInts;
//        int lDst = localBytesUsed(), lLen = o.localBytesUsed();
//        reserve(oRows, lLen);
//
//        //vectorized copy of slices, adding lDst to each SL_OFF in slices
//        int[] sl = this.slices, osl = o.slices;
//        int src = 0;
//        if (LowLevelHelper.ENABLE_VEC && slLen >= I_LEN) {
//            IntVector delta = IntVector.zero(I_SP).blend(lDst, PUT_MASK);
//            for (; src < slLen && src+ I_LEN < osl.length; src += I_LEN, dst += I_LEN)
//                fromArray(I_SP, osl, src).add(delta).intoArray(sl, dst);
//        }
//        // copy/offset leftovers. arraycopy()+for is faster than a fused copy/offset loop
//        if ((slLen -= src) > 0) {
//            arraycopy(osl, src, sl, dst, slLen);
//            for (int end = dst+slLen; dst < end; dst += 2)
//                sl[dst] += lDst;
//        }
//
//        //copy shared
//        arraycopy(o.shared, 0, shared, rows*cols, oRows*cols);
//
//        //copy locals. arraycopy() is faster than vectorized copy
//        arraycopy(o.locals, 0, locals, lDst, lLen);
//
//        // update batch-level data
//        rows += oRows;
//        assert validate() : "corrupted";
//    }

    @Override public void putRange(CompressedBatch other, int begin, int end) {
        int cols = this.cols, oRows = end-begin;
        if (oRows <= 0) return; // no work
        if (cols != other.cols) throw new IllegalArgumentException("cols != other.cols");

        int slw = slRowInts, slDst = slw*rows;
        int[] osl = other.slices;
        int lDst = localBytesUsed(), lSrc = osl[begin*slw+(cols<<1)+SL_OFF];
        int lLen = other.localBytesUsed()-lSrc;

        reserve(oRows, lLen);
        int[] sl = slices;
        arraycopy(osl, begin*slw, sl, slDst, slw*oRows);
        for (int i = slDst, slEnd = slDst + slw*oRows; i < slEnd; i += 2)
            sl[i] += lDst;

        arraycopy(other.shared, begin*cols, shared, rows*cols, oRows*cols);
        arraycopy(other.locals, lSrc, locals, lDst, lLen);
        rows += oRows;
        assert validate() : "corrupted";
    }

    //    private void scalarPut(CompressedBatch o) {
//        if (o.cols != cols)   throw new IllegalArgumentException("cols != o.cols");
//        int localsLen = o.bytesUsed(), localsDest = bytesUsed();
//        reserve(o.rows, localsLen);
//
//        //copy sl, offsetting MD_OFF by localsDest
//        int[] sl = this.slices, osl = o.slices;
//        int rInts = this.slRowInts, orInts = o.slRowInts;
//        int src = 0, dst = rows*rInts, srcEnd = o.rows*orInts, width = cols<<1;
//        for (; src < srcEnd; src += orInts-width, dst += rInts-width) {
//            for (int rowEnd = src+width; src < rowEnd; src += 2, dst += 2) {
//                sl[dst+SL_OFF] = osl[src+SL_OFF] + localsDest;
//                sl[dst+SL_LEN] = osl[src+SL_LEN];
//            }
//        }
//        // copy locals
//        arraycopy(o.locals, 0, locals, localsDest, localsLen);
//
//        //update batch-level metadata
//        vectorSafe &= o.vectorSafe; // out-of-order columns and gaps were copied
//        rows += o.rows;
//        assert !corrupted() : "corrupted";
//    }

    @Override public void putRow(CompressedBatch o, int row) {
        int cols = this.cols;
        if (cols != o.cols) throw new IllegalArgumentException("o.cols != cols");
        if (row > o.rows) throw new IndexOutOfBoundsException(o.mkOutOfBoundsMsg(row));
        int[] osl = o.slices;
        int oBase = row*o.slRowInts, lLen = o.localBytesUsed(row), lSrc = osl[oBase+(cols<<1) + SL_OFF];
        reserve(1, lLen);
        int base = rows*slRowInts, lDst = localBytesUsed();
        int[] sl = this.slices;
        for (int e = base+(cols<<1), adj = lDst-lSrc; base < e; base += 2, oBase += 2) {
            sl[base+SL_OFF] = osl[oBase+SL_OFF] + adj;
            sl[base+SL_LEN] = osl[oBase+SL_LEN];
        }
        SegmentRope[] osh = o.shared;
        for (int i = 0, b = rows*cols, ob = row*cols; i < cols; i++)
            shared[b+i] = osh[ob+i];
        arraycopy(o.locals, lSrc, locals, lDst, lLen);
        int slRowBase = (rows+1)*slRowInts - 2;
        slices[slRowBase+SL_OFF] = lDst;
        slices[slRowBase+SL_LEN] = lLen;
        ++rows;
        assert validate() : "corrupted";
    }

    @Override public <O extends Batch<O>> @This CompressedBatch putConverting(O other) {
        if (other instanceof CompressedBatch cb) {
            put(cb);
            return this;
        }
        int cols = this.cols, rows = other.rows;
        if (other.cols != cols) throw new IllegalArgumentException();
        reserve(rows, other.localBytesUsed());
        TwoSegmentRope t = TwoSegmentRope.pooled();
        for (int r = 0; r < rows; r++)
            putRowConverting(other, cols, r, t);
        t.recycle();
        return this;
    }

    private <O extends Batch<O>>
    void putRowConverting(O other, int cols, int r, TwoSegmentRope t) {
        beginPut();
        for (int c = 0; c < cols; c++) {
            if (other.getRopeView(r, c, t)) {
                byte fst = t.get(0);
                SegmentRope sh = switch (fst) {
                    case '"' -> SHARED_ROPES.internDatatypeOf(t, 0, t.len);
                    case '<' -> SHARED_ROPES.  internPrefixOf(t, 0, t.len);
                    case '_' -> EMPTY;
                    default -> throw new IllegalArgumentException("Not an RDF term: "+ t);
                };
                int localLen = t.len-sh.len, localOff = fst == '<' ? sh.len : 0;
                int dest = allocTerm(false, c, sh,
                                     localLen|(fst == '"'? SH_SUFF_MASK : 0));
                t.copy(localOff, localOff+localLen, locals, dest);
            }
        }
        commitPut();
    }

    @Override public <O extends Batch<O>> void putRowConverting(O other, int row) {
        if (other.type() == COMPRESSED) {
            putRow((CompressedBatch)other, row);
            return;
        }
        int cols = this.cols;
        if (cols != other.cols) throw new IllegalArgumentException("cols mismatch");
        TwoSegmentRope t = TwoSegmentRope.pooled();
        putRowConverting(other, cols, row, t);
        t.recycle();
    }

    /* --- --- --- operation objects --- --- --- */

    public static final class Merger extends BatchMerger<CompressedBatch> {
        private int @Nullable [] tsl;
        private SegmentRope @Nullable [] tsh;

        public Merger(BatchType<CompressedBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
        }

        @Override public void doRelease() {
            if (tsl != null) tsl =      INT.offer(tsl, tsl.length);
            if (tsh != null) tsh = SEG_ROPE.offer(tsh, tsh.length);
            super.doRelease();
        }

        @Override protected int[] makeColumns(int[] sources) {
            int[] columns = new int[sources.length];
            for (int i = 0; i < sources.length; i++)
                columns[i] = sources[i]-1;
            return columns;
        }

        @Override public CompressedBatch projectInPlace(CompressedBatch b) {
            int[] columns = requireNonNull(this.columns);
            int rows = b.rows, bCols = b.cols, bSlWidth = b.slRowInts;

            //project/compact slices
            var bsl = b.slices;
            var tsl = intsAtLeast(slAlign(rows*(columns.length+1)<<1), this.tsl);
            var bsh = b.shared;
            var tsh = segmentRopesAtLeast(rows*columns.length, this.tsh);
            for (int r = 0, slOut = 0, shOut = 0, slBase = 0; r < rows; r++, slBase += bSlWidth) {
                int rowEnd = 0, shBase = r*bCols;
                for (int src : columns) {
                    if (src < 0) {
                        tsh[shOut] = null;
                        tsl[slOut  ] = 0;
                        tsl[slOut+1] = 0;
                    } else {
                        tsh[shOut] = bsh[shBase + src];
                        src = slBase + (src<<1);
                        int off = bsl[src+SL_OFF], len = bsl[src+SL_LEN];
                        tsl[slOut+SL_OFF] = off;
                        tsl[slOut+SL_LEN] = len;
                        rowEnd = Math.max(rowEnd, off+len&LEN_MASK);
                    }
                    slOut += 2;
                    ++shOut;
                }
                // update row (off, len) slice
                int rowOff = bsl[slBase + bSlWidth - 2 + SL_OFF];
                tsl[slOut+SL_OFF] = rowOff;
                tsl[slOut+SL_LEN] = rowEnd - rowOff;
                slOut += 2;
            }

            // replace metadata
            b.slices = tsl;
            b.shared = tsh;
            b.cols = columns.length;
            b.slRowInts = (columns.length+1)<<1;
            this.tsl = bsl;
            this.tsh = bsh;
            assert b.validate() : "corrupted by projection";
            return b;
        }
    }

    public static final class Filter extends BatchFilter<CompressedBatch> {
        private int @Nullable [] tsl;
        private SegmentRope @Nullable [] tsh;
        private final boolean bogusProjection;

        public Filter(BatchType<CompressedBatch> batchType, Vars outVars,
                      BatchMerger<CompressedBatch> projector,
                      RowFilter<CompressedBatch> rowFilter,
                      @Nullable BatchFilter<CompressedBatch> before) {
            super(batchType, outVars, projector, rowFilter, before);
            if (projector != null && projector.columns != null) {
                boolean bogus = true;
                for (int c : projector.columns) {
                    if (c != -1) { bogus = false; break; }
                }
                bogusProjection = bogus;
            } else  {
                bogusProjection = false;
            }
            assert projector == null || projector.vars.equals(outVars);
        }

        @Override public void doRelease() {
            if (tsl != null) tsl =      INT.offer(tsl, tsl.length);
            if (tsh != null) tsh = SEG_ROPE.offer(tsh, tsh.length);
            super.doRelease();
        }

        private CompressedBatch filterInPlaceEmpty(CompressedBatch in, int cols) {
            int rows = in.rows, survivors = 0;
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(in, r)) {
                    case KEEP -> survivors++;
                    case DROP -> {}
                    case TERMINATE -> rows = -1;
                }
            }
            if (rows == -1) {
                cancelUpstream();
                if (survivors == 0) return batchType.recycle(in);
            }
            in.clear(cols);
            if (cols > 0 && survivors > 0) {
                in.reserve(survivors, 0);
                Arrays.fill(in.slices, 0, in.slRowInts*survivors, 0);
                Arrays.fill(in.shared, 0, survivors, null);
            }
            in.rows = survivors;
            assert in.validate() : "filterInPlaceEmpty corrupted batch";
            return in;
        }

        @Override public CompressedBatch filterInPlace(CompressedBatch in,
                                                       BatchMerger<CompressedBatch> projector) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null)
                return null;
            int @Nullable[] columns = projector == null ? null : projector.columns;
            int rows = in.rows, iCols = in.cols, cols = columns == null ? iCols : columns.length;

            //project if filter requires
            if (columns != null && rowFilter.targetsProjection()) {
                in = projector.projectInPlace(in);
                columns = null;
            }

            if (cols == 0 || rows == 0 || bogusProjection)
                return filterInPlaceEmpty(in, cols);

            // get working arrays
            int tslRowInts = (cols+1)<<1, islRowInts = in.slRowInts, slOut = 0, shOut = 0;
            var isl = in.slices;
            var tsl = intsAtLeast(slAlign(rows*tslRowInts), this.tsl);
            var ish = in.shared;
            var tsh = segmentRopesAtLeast(rows*cols, this.tsh);

            if (columns == null) { // faster code if we don't need to concurrently project
                for (int r = 0; r < rows; r++) {
                    switch (rowFilter.drop(in, r)) {
                        case KEEP -> {
                            arraycopy(isl, r*islRowInts, tsl, slOut, tslRowInts);
                            arraycopy(ish, r*cols, tsh, shOut, cols);
                            shOut += cols;
                            slOut += tslRowInts;
                        }
                        case DROP -> {}
                        case TERMINATE -> rows = -1;
                    }
                }
            } else {
                for (int r = 0; r < rows; r++) {
                    switch (rowFilter.drop(in, r)) {
                        case DROP -> {}
                        case KEEP -> {
                            int slBase = r*islRowInts, shBase = r*iCols, rowEnd = 0;
                            for (int src : columns) {
                                if (src < 0) {
                                    tsh[shOut] = null;
                                    tsl[slOut  ] = 0;
                                    tsl[slOut+1] = 0;
                                } else {
                                    tsh[shOut] = ish[shBase+src];
                                    int colBase = slBase + (src<<1);
                                    int off = isl[colBase+SL_OFF], len = isl[colBase+SL_LEN];
                                    tsl[slOut+SL_OFF] = off;
                                    tsl[slOut+SL_LEN] = len;
                                    rowEnd = Math.max(rowEnd, off+len&LEN_MASK);
                                }
                                slOut += 2;
                                ++shOut;
                            }
                            int rowOff = isl[slBase+islRowInts-2+SL_OFF];
                            tsl[slOut+SL_OFF] = rowOff;
                            tsl[slOut+SL_LEN] = rowEnd - rowOff;
                            slOut += 2;
                        }
                        case TERMINATE -> rows = -1;
                    }
                }
            }

            if (rows == -1) { //TERMINATED
                cancelUpstream();
                if (slOut == 0) {
                    this.tsl = tsl;
                    this.tsh = tsh;
                    return batchType.recycle(in);
                }
            }
            //update metadata
            in.rows = slOut/tslRowInts;
            in.slices = tsl;
            in.shared = tsh;
            in.slRowInts = tslRowInts;
            if (columns != null)
                in.cols = columns.length;
            // use original isl on next call
            this.tsl = isl;
            this.tsh = ish;
            assert in.validate() : "corrupted by projection";
            return in;
        }
    }

}
