package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.UNTIL_DQ;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.*;
import static java.util.Objects.requireNonNull;
import static jdk.incubator.vector.IntVector.fromArray;
import static jdk.incubator.vector.VectorMask.fromLong;

public class CompressedBatch extends Batch<CompressedBatch> {
    private static final int SH_SUFF_MASK = 0x80000000;
    private static final int     LEN_MASK = 0x7fffffff;
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;

    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_SP_LEN = B_SP.length();
    private static final int B_SP_MASK = B_SP_LEN-1;
    private static final VectorSpecies<Integer> I_SP = IntVector.SPECIES_PREFERRED;
    private static final int I_SP_LEN = I_SP.length();
    private static final int PUT_SLACK = I_SP_LEN;
    private static final int I_SP_MASK = I_SP_LEN-1;

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

    /** Aligned index in {@code locals} where locals of a new row shall be placed. */
    private int bytesUsed;
    /** {@code -1} if not in a {@link #beginOffer()}/{@link #beginPut()}. Else this is the index
     * into {@code locals} where local bytes for the next column shall be written to. */
    private int offerNextLocals = -1;
    /** {@code col} in last {@code offerTerm}/{@code putTerm} call in the current
     *  {@link #beginOffer()}/{@link #beginPut()}, else {@code -1}. */
    private int offerLastCol = -1;


    /* --- --- --- helpers --- --- --- */

    String dump() {
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
        try {
            if (cols == 0) return true;
            if (rows < 0 || cols < 0)
                throw new AssertionError("Negative dimensions: rows" + rows + ", cols=" + cols);
            int lastRowAlignedEnd = 0;
            for (int r = 0; r < rows; r++) {
                int rOff = slices[slRowBase(r) + SL_OFF];
                int rEnd = rOff + slices[slRowBase(r) + SL_LEN];
                if (rEnd < rOff)
                    throw new AssertionError("Negative length for row "+r);
                if (rEnd > rOff && rOff < lastRowAlignedEnd)
                    throw new AssertionError("Non-empty row (" + r + ") locals start at " + rOff + ", before end (" + lastRowAlignedEnd + ") of previous row ");
                if ((bytesUsed(r) & B_SP_MASK) != 0)
                    throw new AssertionError("bytesUsed(" + r + ")=" + bytesUsed(r) + " is not aligned");
                int rowAlignedEnd = rOff + bytesUsed(r);
                if (rEnd > rowAlignedEnd)
                    throw new AssertionError("stored locals end " + rEnd + " for row " + r + " overflows bytesUsed(r)=" + bytesUsed(r));
                for (int c = 0; c < cols; c++) {
                    int off = slices[r * slRowInts + (c << 1) + SL_OFF];
                    int len = slices[r * slRowInts + (c << 1) + SL_LEN];
                    if (len > 0 && off < lastRowAlignedEnd)
                        throw new AssertionError("locals offset for col " + c + " of row " + r + " is before the assigned start for this row");
                    if (len > 0 && off+len > rowAlignedEnd)
                        throw new AssertionError("locals offset for col " + c + " of row " + r + " overflows assigned locals segment for row");
                    try {
                        get(r, c);
                    } catch (Throwable t) {
                        throw new AssertionError("Failed to get(" + r + ", " + c + "): " + t);
                    }
                    if (!equals(r, c, this, r, c))
                        throw new AssertionError("Equality not reflexive for col " + c + " of row " + r);
                }
                if (!equals(r, this, r))
                    throw new AssertionError("Equality not reflexive for row " + r);
                lastRowAlignedEnd = Math.max(lastRowAlignedEnd, rowAlignedEnd);
            }
        } catch (Throwable t) {
            try {
                System.err.println(dump());
            } catch (Throwable ignored) {}
            throw t;
        }
        return true;
    }

    private void growLocals(byte[] src, int newLen) {
        this.localsSeg = MemorySegment.ofArray(this.locals = copyOf(src, newLen));
    }

    private int slBase(int row, int col) {
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        return row* slRowInts + (col<<1);
    }
    private int slRowBase(int row) {
        if (row < 0 || row >= rows) throw new IndexOutOfBoundsException();
        return row*slRowInts + (cols<<1);
    }


    /** Get a copy of {@code md} that is at least 50% bigger, can contain at least
     *  {@code additionalInts} more integers and is aligned to {@code I_SP_LEN}. */
    private static int[] slGrow(int[] slices, int additionalInts) {
        int ints = slices.length + max(slices.length >> 1, additionalInts); // grow at least 50%
        return Arrays.copyOf(slices, slCeil(ints));
    }

    /** Align ints to {@code I_SP_LEN}. */
    private static int slCeil(int ints) {
        if (ints == 0) return I_SP_LEN; // never return 0
        return ints + ((I_SP_LEN-ints)&I_SP_MASK); // align size to I_SP_LEN
    }

    /** Increase {@code bytes} until it is aligned to {@code B_SP_LEN} */
    static int localsCeil(int bytes) { return  bytes + ((B_SP_LEN-bytes)&B_SP_MASK); }

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
        if (destCol <     0) throw new IndexOutOfBoundsException();
        if (destCol >= cols) throw new IndexOutOfBoundsException();
        // find write location in md and grow if needed
        int slBase = rows*slRowInts + (destCol<<1), dest = offerNextLocals;
        if (slices[slBase+SL_OFF] != 0)
            throw new IllegalStateException("Column already set");
        int len = flaggedLocalLen & LEN_MASK;
        if (len > 0) {
            offerLastCol = destCol;
            int required  = localsCeil(dest+len);
            if (required > locals.length) {
                if (forbidGrow) return -1;
                growLocals(locals, max(required, localsCeil(locals.length+(locals.length>>1))));
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

    /**
     * Erases the side effects of a rejected {@code beginOffer}/{@code offerTerm}/
     * {@code commitOffer} sequence.
     */
    private boolean rollbackOffer() {
        fill(locals, bytesUsed, locals.length, (byte)0);
        offerLastCol = -1;
        offerNextLocals = -1;
        return false;
    }

    /* --- --- --- lifecycle --- --- --- */

    CompressedBatch(int rowsCapacity, int cols, int bytesCapacity) {
        super(0, cols);
        growLocals(EMPTY.u8(), max(B_SP_LEN, bytesCapacity));
        this.slRowInts = (cols+1)<<1;
        this.slices = new int[slCeil(max(1, rowsCapacity)*slRowInts+PUT_SLACK)];
        this.shared = new SegmentRope[max(1, rowsCapacity)*slRowInts+PUT_SLACK];
    }

    private CompressedBatch(CompressedBatch o) {
        super(o.rows, o.cols);
        this.bytesUsed = o.bytesUsed;
        growLocals(o.locals, o.locals.length);
        this.slices = copyOf(o.slices, o.slices.length);
        this.shared = copyOf(o.shared, o.shared.length);
        this.offerNextLocals = o.offerNextLocals;
        this.offerLastCol = o.offerLastCol;
        this.slRowInts = o.slRowInts;
    }

    @Override public Batch<CompressedBatch> copy() { return new CompressedBatch(this); }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public int bytesUsed() { return bytesUsed; }

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
        int h = FNV_BASIS;
        if (cols == 0)
            return h;
        SegmentRope fst, snd, local = new SegmentRope(localsSeg, locals, 0, 0);
        for (int i = row*cols, e = i+cols, slb = row*slRowInts; i < e; ++i, slb += 2) {
            SegmentRope s = shared[i];
            if (s == null) s = EMPTY;
            int len = slices[slb+SL_LEN];
            local.slice(slices[slb+SL_OFF], len&LEN_MASK);
            if ((len & SH_SUFF_MASK) == 0)  {
                fst = s;
                snd = local;
            } else {
                fst = local;
                snd = s;
            }
            h = snd.hashCode(fst.hashCode(h));
        }
        return h;
    }

    @Override public int bytesUsed(int row) {
        return cols == 0 ? 0 : localsCeil(slices[slRowBase(row)+SL_LEN]);
    }

    @Override public boolean equals(int row, CompressedBatch other, int oRow) {
        if (!HAS_UNSAFE)
            return equalsNoUnsafe(row, other, oRow);
        if (row < 0 || row >= rows || oRow < 0 || oRow >= other.rows)
            throw new IndexOutOfBoundsException();
        int cols = this.cols;
        if (cols != other.cols) return false;
        if (cols == 0) return true;
        byte[] locals = this.locals, oLocals = other.locals;
        SegmentRope[] shared = this.shared, oshared = other.shared;
        int[] sl = this.slices, osl = other.slices;
        int slb = row*slRowInts, oslb = oRow*slRowInts;
        int shBase = row*cols, oShBase = oRow*cols;
        for (int c = 0; c < cols; ++c) {
            SegmentRope sh = shared[shBase+c], osh = oshared[oShBase+c];
            if ( sh == null) sh = EMPTY;
            if (osh == null) osh = EMPTY;
            int c2 = c << 1;
            if (compare2_2(sh.utf8,  sh.segment.address()+sh.offset,  sh.len,
                           locals, sl[slb+c2+SL_OFF], sl[slb+c2+SL_LEN]&LEN_MASK,
                           osh.utf8, osh.segment.address()+osh.offset, osh.len,
                           oLocals, osl[oslb+c2+SL_OFF], osl[oslb+c2+SL_LEN]&LEN_MASK) != 0)
                return false;
        }
        return true;
    }


    private boolean equalsNoUnsafe(int row, CompressedBatch other, int oRow) {
        if (row < 0 || row >= rows || oRow < 0 || oRow >= other.rows)
            throw new IndexOutOfBoundsException();
        int cols = this.cols;
        if (cols != other.cols) return false;
        if (cols == 0) return true;
        MemorySegment localsSeg = this.localsSeg, oLocalsSeg = other.localsSeg;
        SegmentRope[] shared = this.shared, oshared = other.shared;
        int[] slices = this.slices, oSlices = other.slices;
        int slBase = row*slRowInts, oSlBase = oRow*slRowInts;
        int shBase = row*cols, oShBase = oRow*cols;
        for (int c = 0; c < cols; ++c) {
            SegmentRope sh = shared[shBase+c], osh = oshared[oShBase+c];
            if ( sh == null) sh = EMPTY;
            if (osh == null) osh = EMPTY;
            int c2 = c << 1;
            if (compare2_2(sh.segment,  sh.offset,  sh.len,  localsSeg,
                    slices[ slBase+c2+SL_OFF],
                    slices[ slBase+c2+SL_LEN]&LEN_MASK,
                    osh.segment, osh.offset, osh.len, oLocalsSeg,
                    oSlices[oSlBase+c2+SL_OFF],
                    oSlices[oSlBase+c2+SL_LEN]&LEN_MASK) != 0)
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
        var local = new SegmentRope(localsSeg, locals, slices[i2+SL_OFF], len);
        dest.set(sh, local, suffix);
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

    @Override public @NonNull SegmentRope shared(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row < 0 || row >= rows || col < 0 || col >= cols)
            throw new IndexOutOfBoundsException();
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
        return RopeSupport.reverseSkip(locals, off, off+localLen, UNTIL_DQ)-off;
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
        SegmentRope local = new SegmentRope(localsSeg, locals, off, len);
        return new Term(sh, local, false).asDatatypeSuff();
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
        int i = RopeSupport.reverseSkip(locals, off, end, UNTIL_DQ);
        if      (i+1 == end)
            return Term.XSD_STRING;
        else if (locals[i+1] == '@')
            return Term.RDF_LANGSTRING;
        else if (end-i > 5 /*"^^<x>*/)
            return Term.splitAndWrap(new SegmentRope(localsSeg, locals, i+3, end-(i+3)));
        else
            throw new InvalidTermException(this, i, "Unexpected literal suffix");
    }

    @Override
    public void writeSparql(ByteSink<?> dest, int row, int col, PrefixAssigner prefixAssigner) {
        int base = slBase(row, col), len = slices[base + SL_LEN];
        boolean suffix = len < 0;
        len &= LEN_MASK;
        var sh = shared[row*cols + col];
        if (len != 0 || sh != null)
            Term.toSparql(dest, prefixAssigner, sh, localsSeg, slices[base+SL_OFF], len, suffix);
    }

    @Override public void writeNT(ByteSink<?> dest, int row, int col) {
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

    @Override public void write(ByteSink<?> dest, int row, int col, int begin, int end) {
        int base = slBase(row, col);
        MemorySegment fst = localsSeg, snd = EMPTY.segment;
        long fstOff = slices[base+SL_OFF], sndOff = 0;
        int fstLen  = slices[base+SL_LEN], sndLen = 0;
        var sh = shared[row*cols + col];
        if (sh != null) {
            if (fstLen < 0) { snd = sh.segment; sndOff = sh.offset; sndLen = sh.len; }
            else            { snd = fst;        sndOff = fstOff;    sndLen = fstLen&LEN_MASK;
                              fst = sh.segment; fstOff = sh.offset; fstLen = sh.len; }
        }
        fstLen &= LEN_MASK;
        if (begin < 0 || end > (fstLen+sndLen))
            throw new IndexOutOfBoundsException();
        if (fstLen + sndLen == 0) return;

        if (begin < fstLen)
            dest.append(fst, fstOff+begin, Math.min(fstLen, end)-begin);
        if (end > fstLen) {
            begin = Math.max(0, begin-fstLen);
            dest.append(snd, sndOff+begin, Math.max(0, (end-fstLen)-begin));
        }
    }

    @Override public int hash(int row, int col) {
        int slBase = slBase(row, col), fstLen, sndLen = slices[slBase+SL_LEN];
        long fstOff, sndOff = slices[slBase+SL_OFF];
        SegmentRope sh = shared[row*cols + col];
        if (Term.isNumericDatatype(sh)) {
            Term tmp = new Term();
            if (getView(row, col, tmp))
                return tmp.hashCode();
            return 0;
        } else if (sh == null) {
            return SegmentRope.hashCode(FNV_BASIS, localsSeg, sndOff, sndLen&LEN_MASK);
        } else {
            MemorySegment fst = sh.segment, snd = localsSeg;
            if (sndLen < 0) {
                fstOff = sndOff; fstLen = sndLen & LEN_MASK;
                sndOff = sh.offset; sndLen = sh.len;
                MemorySegment tmp = fst;
                fst = snd;
                snd = tmp;
            } else {
                fstOff = sh.offset; fstLen = sh.len;
            }
            int h = SegmentRope.hashCode(Rope.FNV_BASIS, fst, fstOff, fstLen);
            return SegmentRope.hashCode(h, snd, sndOff, sndLen);
        }
    }

    @Override public boolean equals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        int base = slBase(row, col), fstLen = 0, sndLen = slices[base+SL_LEN];
        boolean suffix = (sndLen & SH_SUFF_MASK) != 0;
        sndLen &= LEN_MASK;
        SegmentRope sh = shared[row*cols + col];
        // handle nulls
        if (other == null)
            return sh == null && sndLen == 0;
        if (sh == null && sndLen == 0)
            return false;
        // handle numeric
        if (Term.isNumericDatatype(sh))
            return other.asNumber() != null && other.equals(get(row, col));
        else if (other.asNumber() != null)
            return Term.isNumericDatatype(sh) && other.equals(get(row, col));
        // compare as string
        MemorySegment fst = EMPTY.segment, snd = localsSeg;
        long fstOff = 0, sndOff = slices[base+SL_OFF];
        if (sh != null) {
            if (suffix) {
                fst = localsSeg;  fstOff = sndOff;    fstLen = sndLen;
                snd = sh.segment; sndOff = sh.offset; sndLen = sh.len;
            } else {
                fst = sh.segment; fstOff = sh.offset; fstLen = sh.len;
            }
        }
        SegmentRope oFst = other.first(), oSnd = other.second();
        return compare2_2( fst,  fstOff,  fstLen,  snd,  sndOff,  sndLen,
                          oFst.segment, oFst.offset, oFst.len,
                          oSnd.segment, oSnd.offset, oSnd.len) == 0;
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col,
                          CompressedBatch other, int oRow, int oCol) {
        int b = slBase(row, col), ob = other.slBase(oRow, oCol);
        SegmentRope sh = shared[row*cols+col], osh = other.shared[oRow*other.cols + oCol];
        // handle numeric
        if (Term.isNumericDatatype(sh)) //noinspection DataFlowIssue
            return Term.isNumericDatatype(osh) && get(row, col).equals(other.get(oRow, oCol));
        if (Term.isNumericDatatype(osh)) //noinspection DataFlowIssue
            return Term.isNumericDatatype(sh) && get(row, col).equals(other.get(oRow, oCol));

        int  fstLen = 0, sndLen = slices[b+SL_LEN], oFstLen = 0, oSndLen = other.slices[ob+SL_LEN];
        long fstOff = 0, sndOff = slices[b+SL_OFF], oFstOff = 0, oSndOff = other.slices[ob+SL_OFF];
        boolean suff = sndLen < 0, oSuff = oSndLen < 0;
        sndLen  &= LEN_MASK;
        oSndLen &= LEN_MASK;
        // handle nullity
        if (sndLen == 0 && (sh == null || sh.len == 0))
            return oSndLen == 0 && (osh == null || osh.len == 0);
        if (oSndLen == 0 && (osh == null || osh.len == 0))
            return false;

        //compare as strings
        MemorySegment  fst = EMPTY.segment,  snd = localsSeg;
        MemorySegment oFst = EMPTY.segment, oSnd = other.localsSeg;
        if (sh != null) {
            if (suff) {
                fst = snd;        fstOff = sndOff;    fstLen = sndLen;
                snd = sh.segment; sndOff = sh.offset; sndLen = sh.len;
            } else {
                fst = sh.segment; fstOff = sh.offset; fstLen = sh.len;
            }
        }
        if (osh != null) {
            if (oSuff) {
                oFst = oSnd;        oFstOff = oSndOff;    oFstLen = oSndLen;
                oSnd = osh.segment; oSndOff = osh.offset; oSndLen = osh.len;
            } else {
                oFst = osh.segment; oFstOff = osh.offset; oFstLen = osh.len;
            }
        }
        return compare2_2(fst, fstOff, fstLen, snd, sndOff, sndLen,
                          oFst, oFstOff, oFstLen, oSnd, oSndOff, oSndLen) == 0;
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void reserve(int additionalRows, int additionalBytes) {
        int required = (rows + additionalRows) * cols;
        if (required > shared.length)
            shared = copyOf(shared, Math.max(shared.length+(shared.length>>1), required));
        required = (rows+additionalRows) * slRowInts + PUT_SLACK;
        if (required > slices.length)
            slices = slGrow(slices, Math.max(slices.length+ (slices.length>>1), required));
        required = bytesUsed()+additionalBytes;
        if (required > locals.length)
            growLocals(locals, localsCeil(required));
    }

    @Override public void clear() {
        rows            = 0;
        bytesUsed       = 0;
        offerNextLocals = -1;
        offerLastCol    = -1;
        slices[SL_OFF]  = 0;
        slices[SL_LEN]  = 0;
    }

    @Override public void clear(int newColumns) {
        clear();
        cols            = newColumns;
        slRowInts       = (newColumns+1) << 1;
        int required = slCeil(newColumns<<2);
        if (slices.length < required) {
            slices = new int[required];
        } else { // set off, len to make bytesUsed() return 0
            slices[slRowInts-2+SL_OFF] = 0;
            slices[slRowInts-2+SL_LEN] = 0;
        }
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
        if (dest < 0) return rollbackOffer();
        local.copy(0, local.len, locals, dest);
        return true;
    }

    @Override public boolean offerTerm(int destCol, CompressedBatch other, int oRow, int oCol) {
        int oBase = other.slBase(oRow, oCol);
        int[] oSl = other.slices;
        int fLen = oSl[oBase + SL_LEN];
        int dest = allocTerm(true, destCol, other.shared[oRow*other.cols+oCol], fLen);
        if (dest < 0) return rollbackOffer();
        arraycopy(other.locals, oSl[oBase+SL_OFF], locals, dest, fLen&LEN_MASK);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, MemorySegment local, long localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return rollbackOffer();
        MemorySegment.copy(local, JAVA_BYTE, localOff, locals, dest, localLen);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, byte[] local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return rollbackOffer();
        arraycopy(local, localOff, locals, dest, localLen);
        return true;
    }

    @Override
    public boolean offerTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(true, col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        if (dest < 0) return rollbackOffer();
        local.copy(localOff, localOff+localLen, locals, dest);
        return true;
    }

    @Override public boolean commitOffer() {
        if (offerNextLocals < 0) throw new IllegalStateException();
        int base = (rows+1) * slRowInts - 2;
        slices[base+SL_OFF] = bytesUsed;
        slices[base+SL_LEN] = offerNextLocals - bytesUsed;
        bytesUsed = localsCeil(offerNextLocals);
        ++rows;
        offerNextLocals = -1;
        offerLastCol = -1;
        assert validate() : "corrupted";
        return true;
    }

    @Override public boolean offerRow(CompressedBatch other, int row) {
        if ((rows+1)*slRowInts               > slices.length) return false;
        if (other.bytesUsed(row)+bytesUsed() > locals.length) return false;
        putRow(other, row);
        return true;
    }

    @Override public boolean offer(CompressedBatch o) {
        if (rows+o.rows*slRowInts     > slices.length) return false;
        if (bytesUsed()+o.bytesUsed() > locals.length) return false;
        put(o);
        return true;
    }

    @Override public void beginPut() {
        offerLastCol = -1;
        offerNextLocals = bytesUsed;

        int required = (rows+1) * slRowInts;
        if (required > slices.length)  // grow capacity by 50% + 2 so that md fits at least 2 rows
            slices = slGrow(slices, slRowInts+PUT_SLACK);
        fill(slices, required-slRowInts, required, 0);

        required = (rows+1)*cols;
        if (required > shared.length)
            shared = copyOf(shared, Math.max(shared.length + (shared.length >> 1), required));
        fill(shared, required-cols, required, null);
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
    public void putTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(false, col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, locals, dest);
    }

    @Override public void commitPut() { commitOffer(); }


    /** Mask true for SL_OFF and false for SL_LEN */
    private static final VectorMask<Integer> PUT_MASK = fromLong(I_SP, 0x5555555555555555L);
    static {
        if (SL_OFF != 0) throw new AssertionError("PUT_MASK misaligned with SL_OFF");
    }

    public void put(CompressedBatch o) {
        // handle special cases
        int oRows = o.rows, cols = this.cols;
        if (oRows == 0) return;
        if (slRowInts != o.slRowInts || o.cols != cols)
            throw new IllegalArgumentException("cols != o.cols");

        int dst = slRowInts*rows, slLen = oRows*slRowInts;
        int lDst = bytesUsed, lLen = o.bytesUsed;

        int required = slCeil(dst + slLen + PUT_SLACK);
        if (required > slices.length)
            slices = slGrow(slices, required);
        if ((required >>= 1) > shared.length)
            shared = copyOf(shared, Math.max(shared.length + (shared.length>>1), required));
        if ((required=localsCeil(bytesUsed+lLen)) > locals.length)
            growLocals(locals, required);

        //vectorized copy of slices, adding lDst to each SL_OFF in slices
        int[] sl = this.slices, osl = o.slices;
        int src = 0;
        if (slLen >= I_SP_LEN) {
            IntVector delta = IntVector.zero(I_SP).blend(lDst, PUT_MASK);
            for (; src < slLen && src+I_SP_LEN < osl.length; src += I_SP_LEN, dst += I_SP_LEN)
                fromArray(I_SP, osl, src).add(delta).intoArray(sl, dst);
        }
        // copy/offset leftovers. arraycopy()+for is faster than a fused copy/offset loop
        if ((slLen -= src) > 0) {
            arraycopy(osl, src, sl, dst, slLen);
            for (int end = dst+slLen; dst < end; dst += 2)
                sl[dst] += lDst;
        }

        //copy shared
        arraycopy(o.shared, 0, shared, rows*cols, oRows*cols);

        //copy locals. arraycopy() is faster than vectorized copy
        arraycopy(o.locals, 0, locals, lDst, lLen);

        // update batch-level data
        bytesUsed += lLen;
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
        if (row > o.rows) throw new IndexOutOfBoundsException("row > o.rows");
        int[] osl = o.slices;
        int oBase = row*o.slRowInts, lLen = o.bytesUsed(row), lSrc = osl[oBase+(cols<<1) + SL_OFF];
        reserve(1, lLen);
        int base = rows*slRowInts, lDst = bytesUsed;
        int[] sl = this.slices;
        for (int e = base+(cols<<1); base < e; base += 2, oBase += 2) {
            sl[base+SL_OFF] = osl[oBase+SL_OFF] - lSrc + lDst;
            sl[base+SL_LEN] = osl[oBase+SL_LEN];
        }
        arraycopy(o.shared, row*cols, shared, rows*cols, cols);
        arraycopy(o.locals, lSrc, locals, lDst, lLen);
        int slRowBase = (rows+1)*slRowInts - 2;
        slices[slRowBase+SL_OFF] = bytesUsed;
        slices[slRowBase+SL_LEN] = lLen;
        bytesUsed += lLen;
        ++rows;
        assert validate() : "corrupted";
    }

    @Override public @This <O extends Batch<O>> CompressedBatch putConverting(O other) {
        if (other instanceof CompressedBatch cb) {
            put(cb);
            return this;
        }
        int cols = this.cols, rows = other.rows;
        if (other.cols != cols) throw new IllegalArgumentException();
        reserve(rows, other.bytesUsed());
        TwoSegmentRope t = new TwoSegmentRope();
        for (int r = 0; r < rows; r++) {
            beginPut();
            for (int c = 0; c < cols; c++) {
                if (other.getRopeView(r, c, t)) {
                    byte fst = t.get(0);
                    SegmentRope sh = switch (fst) {
                        case '"' -> SHARED_ROPES.internDatatypeOf(t, 0, t.len);
                        case '<' -> SHARED_ROPES.  internPrefixOf(t, 0, t.len);
                        case '_' -> EMPTY;
                        default -> throw new IllegalArgumentException("Not an RDF term: "+t);
                    };
                    int localLen = t.len-sh.len, localOff = fst == '<' ? sh.len : 0;
                    int dest = allocTerm(false, c, sh,
                                         localLen|(fst == '"'? SH_SUFF_MASK : 0));
                    t.copy(localOff, localOff+localLen, locals, dest);
                }
            }
            commitPut();
        }
        return this;
    }

    /* --- --- --- operation objects --- --- --- */

    private static final VarHandle REC_TMD, REC_SH;
    @SuppressWarnings("unused") // access through REC_TMD
    private static int[] recTMd;
    @SuppressWarnings("unused") // access through REC_TMD
    private static SegmentRope[] recSh;
    static {
        try {
            REC_TMD = lookup().findStaticVarHandle(CompressedBatch.class,"recTMd",
                                                   int[].class);
            REC_SH = lookup().findStaticVarHandle(CompressedBatch.class,"recSh",
                                                  SegmentRope[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Get a possibly recycled {@code int[]} with {@code length >= required}.
     * @param required required size for the {@code int[]}
     * @return a {@code int[]} that may not be zero-filled.
     */
    private static int[] getTSl(int required) {
        required = slCeil(required);
        int[] a = (int[]) REC_TMD.getAcquire();
        if (a != null) {
            if (a.length < required || !REC_TMD.compareAndSet(a, null))
                a = null;
        }
        return a != null ? a : new int[required];
    }

    /** Analogous to {@link #getTSl(int)}. */
    private static SegmentRope[] getTSh(int required) {
        SegmentRope[] a = (SegmentRope[]) REC_SH.getAcquire();
        if (a != null) {
            if (a.length < required || !REC_SH.compareAndSet(a, null))
                a = null;
        }
        return a != null ? a : new SegmentRope[required];
    }

    /**
     * If the recycled and not yet taken {@code int[]} is null or smaller than {@code tmd},
     * atomically replaces it with {@code tmd}. The caller of this method ALWAYS looses
     * ownership of {@code tmd}, even if it was not stored.
     *
     * @param tmd a {@code int[] to recycle}
     */
    private static void recycleTSl(int[] tmd) {
        int[] a = (int[]) REC_TMD.getAcquire();
        if (a == null || tmd.length > a.length)
            REC_TMD.setRelease(tmd);
    }

    /** Analogous to {@link #recycleTSl(int[])}. */
    private static void recycleTSh(SegmentRope[] sh) {
        SegmentRope[] a = (SegmentRope[]) REC_SH.getAcquire();
        if (a == null || sh.length > a.length)
            REC_SH.compareAndExchangeRelease(a, sh);
    }

    static final class Merger extends BatchMerger<CompressedBatch> {
        private int @Nullable [] tsl;
        private SegmentRope @Nullable [] tsh;

        public Merger(BatchType<CompressedBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
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
            int tSlRequired = rows*((columns.length+1)<<1)+PUT_SLACK;
            int tShRequired = rows*columns.length;

            //project/compact md
            int[] bsl = b.slices, tsl = this.tsl, otmd = null;
            if (tsl == null || tsl.length < tSlRequired) {
                otmd = tsl;
                tsl = getTSl(tSlRequired);
            }
            SegmentRope[] bsh = b.shared, tsh = this.tsh, otsh = null;
            if (tsh == null || tsh.length < tShRequired) {
                otsh = tsh;
                tsh = getTSh(tShRequired);
            }
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

            // replace md, recycle bsl and omd
            b.slices = tsl;
            b.shared = tsh;
            this.tsl = bsl;
            this.tsh = bsh;
            if (otmd != null) recycleTSl(otmd);
            if (otsh != null) recycleTSh(otsh);
            b.cols = columns.length;
            // md now is packed (before b.mdRowInts could be >cols)
            b.slRowInts = (columns.length+1)<<1;
            assert b.validate() : "corrupted by projection";
            return b;
        }
    }

    static final class Filter extends BatchFilter<CompressedBatch> {
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
            assert projector == null || projector.outVars.equals(outVars);
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
            if (rows == -1 && survivors == 0) {
                batchType.recycle(in);
                return null;
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
            int tslRowInts = (cols+1)<<1, islRowInts = in.slRowInts;
            int tslRequired = rows * tslRowInts+PUT_SLACK, slOut = 0, shOut = 0;
            int tshRequired = rows*cols;
            int[] otmd = null, tsl = this.tsl, isl = in.slices;
            if (tsl == null || tsl.length < tslRequired) {
                otmd = tsl;
                tsl = getTSl(tslRequired);
            }
            SegmentRope[] otsh = null, tsh = this.tsh, ish = in.shared;
            if (tsh == null || tsh.length < tshRequired) {
                otsh = tsh;
                tsh = getTSh(tshRequired);
            }

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

            if (rows == -1 && slOut == 0) { //TERMINATED
                batchType.recycle(in);
                recycleTSl(tsl);
                recycleTSh(tsh);
                return null;
            }
            //update metadata. start with division so it runs while we do other stuff
            in.rows = rows = slOut/tslRowInts;
            in.slices = tsl;  // replace metadata
            in.shared = tsh;
            this.tsl = isl;   // use original isl on next call
            this.tsh = ish;
            if (otmd != null) // offer our old tsl to other instances
                recycleTSl(otmd);
            if (otsh != null)
                recycleTSh(otsh);
            in.slRowInts = tslRowInts;
            if (columns != null)
                in.cols = columns.length;
            // update bytesUsed and localsTailClear
            if (rows == 0) {
                in.bytesUsed = 0;
            } else {
                int base = rows * tslRowInts - 2;
                in.bytesUsed = localsCeil(tsl[base+SL_OFF] + tsl[base+SL_LEN]);
            }
            assert in.validate() : "corrupted by projection";
            return in;
        }
    }

}
