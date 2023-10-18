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
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.isNumericDatatype;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.HAS_UNSAFE;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.fill;

public class CompressedBatch extends Batch<CompressedBatch> {
    static final int SH_SUFF_MASK = 0x80000000;
    static final int     LEN_MASK = 0x7fffffff;
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;

    /**
     * For the term at row {@code r} and column {@code c}:
     * <ul>
     *     <li>index {@code r*slRowInts + (c<<1) + SL_OFF} store the offset into {@code locals} where the local
     *         segment of the term starts.</li>
     *     <li>index {@code r*slRowInts + (c<<1) + SL_LEN} stores the length of the local segment of the
     *         term in bits [0,31) and whether the shared segment comes before (0) or after (1)
     *         at bit 31 (see {@code SH_SUFFIX_MASK})</li>
     * </ul>
     */
    private int[] slices;

    /**
     * Array with the shared segments of all terms in this batch. The segment for term at
     * {@code (row, col)} is stored at index {@code row*cols + col}.
     */
    private SegmentRope[] shared;

    /**
     * Equivalent to {@code shared.length}. This field exists to reduce cache misses from
     * checking {@code shared.length}, which requires de-referencing the pointer.
     */
    private int termsCapacity;

    /** Storage for local parts of terms. */
    private byte[] locals;
    /** {@code MemorySegment.ofArray(locals)} */
    private MemorySegment localsSeg;

    /**
     * The total number of bytes in {@code locals} being currently used by the rows of this batch.
     */
    private int localsLen;
    /**
     * The index of the first byte in {@code locals} that is effectively used.
     */
    private int localsBegin;

    /** {@code -1} if not in a {@link #beginPut()}. Else this is the index
     * into {@code locals} where local bytes for the next column shall be written to. */
    private int offerNextLocals = -1;

    /* --- --- --- helpers --- --- --- */

    @SuppressWarnings("unused") String dump() {
        var sb = new StringBuilder();
        sb.append(String.format("""
                CompressedBatch{
                  rows=%d, cols=%d,
                  offerNextLocals=%d
                """,
                rows, cols, offerNextLocals));
        int dumpRows = offerNextLocals == -1 ? rows : rows+1, slw = cols<<1;
        for (int r = 0; r < dumpRows; r++) {
            sb.append("  row=").append(r).append('\n');
            for (int c = 0; c < cols; c++) {
                int off        = slices[r*slw+(c<<1)+SL_OFF];
                int flaggedLen = slices[r*slw+(c<<1)+SL_LEN];
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

    public boolean validate() {
        if (!SELF_VALIDATE)
            return true;
        if (termsCapacity != shared.length)
            return false; // termCapacity out of sync
        if (rows*cols > termsCapacity)
            return false; // not enough capacity
        if (termsCapacity<<1 > slices.length)
            return false;
        int slw = (cols<<1);
        for (int r = 0; r < rows; r++) {
            for (int c = 0, i = r*slw; c < cols; c++, i += 2) {
                int off = slices[i+SL_OFF], fLen = slices[i+SL_LEN], end = off+fLen&LEN_MASK;
                if (off < 0 || (fLen&LEN_MASK) < 0 || end > locals.length)
                    return false; // out-of-bounds off or len
                if (end > off) {
                    if ((fLen&SH_SUFF_MASK) != 0 && locals[off] != '"')
                        return false; // non-literal with shared suffix
                    // check if term intersects with other term in same row
                    for (int p = 0, pi = r*slw; p < c; p++, pi+=2) {
                        int pOff = slices[pi+SL_OFF], pEnd = pOff+slices[pi+SL_LEN] & LEN_MASK;
                        if (pEnd == pOff) continue;
                        if ((pOff >= off && pOff < end) || (off >= pOff && off < pEnd))
                            return false; // terms local segments overlap
                    }
                }
            }
        }
        return super.validate();
    }

    private int slBase(int row, int col) {
        requireUnpooled();
        int cols2 = cols<<1, col2 = col<<1;
        if ((row|col) < 0 || row >= rows || col2 > cols2)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row, col));
        return row*cols2+col2;
    }

    /**
     * Tries to set a term at (rows, offerCol) and if succeeds increments {@code offerCol}.
     *
     * @param shared          A shared suffix/prefix of the term to be kept by reference
     * @param flaggedLocalLen length (in bytes) of the term local part, possibly {@code |}'ed
     *                        with {@code SH_SUFFIX_MASK}
     * @return the offset into {@code this.locals} where {@code flaggedLocalLen&LEN_MASK} bytes
     * MUST be copied after this method return, or {@code -1} if {@code forbidGrow}
     * and there was not enough space in {@code this.locals}
     */
    private int allocTerm(int destCol, SegmentRope shared,
                          int flaggedLocalLen) {
        if (offerNextLocals < 0) throw new IllegalStateException();
        int rows = this.rows, cols = this.cols;
        if (destCol < 0 || destCol >= cols)
            throw new IndexOutOfBoundsException("destCol not in [0, cols)");
        // find write location in md and grow if needed
        int slBase = (rows*cols+destCol)<<1, dest = offerNextLocals;
        if (slices[slBase+SL_OFF] != 0)
            throw new IllegalStateException("Column already set");
        int len = flaggedLocalLen & LEN_MASK;
        if (len > 0) {
            int required  = dest+len;
            if (required > locals.length)
                growLocals(dest, required);
            offerNextLocals = required;
        } else if (shared != null && shared.len > 0) {
            throw new IllegalArgumentException("Empty local with non-empty shared");
        }
        this.shared[rows*cols + destCol] = shared != null && shared.len == 0 ? null : shared;
        slices[slBase+SL_OFF] = dest;
        slices[slBase+SL_LEN] = flaggedLocalLen;
        return dest;
    }

    /* --- --- --- lifecycle --- --- --- */

    CompressedBatch(int terms, int cols) {
        super(0, cols);
        this.locals        = EMPTY.utf8;
        this.localsSeg     = EMPTY.segment;
        this.shared        = segmentRopesAtLeast(terms);
        this.termsCapacity = shared.length;
        this.slices        = intsAtLeast(termsCapacity<<1);
        BatchEvent.Created.record(this);
    }


    void markGarbage() {
        if (MARK_POOLED && (int)P.getAndSetRelease(this, P_GARBAGE) != P_POOLED)
            throw new IllegalStateException("non-pooled marked garbage");
        INT     .offer(slices, slices.length);
        SEG_ROPE.offer(shared, termsCapacity);
        BYTE    .offer(locals, locals.length);
        slices        = EMPTY_INT;
        shared        = EMPTY_SEG_ROPE;
        termsCapacity = 0;
        locals        = EMPTY.utf8;
        localsSeg     = EMPTY.segment;
        BatchEvent.Garbage.record(this);
    }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public CompressedBatchType type() { return COMPRESSED; }

    @Override public CompressedBatch copy(@Nullable CompressedBatch dst) {
        int cols = this.cols, rows = this.rows;
        dst = COMPRESSED.emptyForTerms(dst, rows > 0 ? rows*cols : cols, cols);
        dst.doAppend(this, rows, localsBegin, localsLen);
        return dst;
    }

    @Override public CompressedBatch withCapacity(int addRows) {
        int rows = this.rows, terms = (rows+addRows)*cols;
        if (terms <= termsCapacity)
            return this;
        return grown(terms, cols, localsLen, null, null);
    }

    public int localsFreeCapacity() { return locals.length-(localsBegin+localsLen); }

    @Override public int      localBytesUsed() { return localsLen; }
    @Override public int       termsCapacity() { return termsCapacity; }
    @Override public int  totalBytesCapacity() { return termsCapacity*12 + locals.length; }

    @Override public boolean hasCapacity(int terms, int localBytes) {
        return terms <= termsCapacity && localBytes <= locals.length;
    }

    @Override public int rowsCapacity() {
        return cols == 0 ? Integer.MAX_VALUE : termsCapacity/cols;
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
        if (row >= rows) throw new IndexOutOfBoundsException("row >= rows");
        int sum = 0, slw = cols<<1;
        int[] slices = this.slices;
        for (int i = row*slw+SL_LEN, end = i+slw; i < end; i += 2)
            sum += slices[i]&LEN_MASK;
        return sum;
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
        int slw = cols<<1, slb = row*slw, oslb = oRow*slw;
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
        int slw = cols<<1, slb = row*slw, oslb = oRow*slw;
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

    @Override public CompressedBatch copyRow(int row, @Nullable CompressedBatch offer) {
        offer = COMPRESSED.emptyForTerms(offer, cols, cols);
        offer.putRow(this, row);
        return offer;
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
        SegmentRope sh;
        return (slices[slBase(row, col) + SL_LEN] & LEN_MASK)
                + ((sh = shared[row*cols+col]) == null ? 0 : sh.len);
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

    byte[] copyToBucket(byte[] rowData, SegmentRope[] bucketShared, @NonNegative int dstRow,
                        @NonNegative int srcRow) {
        int cols = this.cols, lBegin = cols<<3;
        arraycopy(shared, srcRow*cols, bucketShared, dstRow*cols, cols);
        int [] slices = this.slices;
        byte[] locals = this.locals;
        rowData = bytesAtLeast(lBegin+localBytesUsed(srcRow), rowData);
        for (int o = 0, lDst = lBegin, i = (srcRow*cols)<<1; o < lBegin; o += 8, i+=2) {
            rowData[o  ] = (byte)(lDst       );
            rowData[o+1] = (byte)(lDst >>>  8);
            rowData[o+2] = (byte)(lDst >>> 16);
            rowData[o+3] = (byte)(lDst >>> 24);
            int len = slices[i+SL_LEN];
            rowData[o+4] = (byte)(len       );
            rowData[o+5] = (byte)(len >>>  8);
            rowData[o+6] = (byte)(len >>> 16);
            rowData[o+7] = (byte)(len >>> 24);
            arraycopy(locals, slices[i+SL_OFF], rowData, lDst, len&=LEN_MASK);
            lDst += len;
        }
        return rowData;
    }

    @Override public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
        return slices[(row*cols+col)<<1+SL_LEN] & LEN_MASK;
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
            begin = max(0, begin-fstLen);
            dest.append(snd, sndU8, sndOff+begin, max(0, (end-fstLen)-begin));
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

    @Override public @Nullable CompressedBatch recycle() { return COMPRESSED.recycle(this); }

    @Override public void clear() {
        rows            =  0;
        localsBegin     =  0;
        localsLen       =  0;
        offerNextLocals = -1;
    }

    @Override public @This CompressedBatch clear(int cols) {
        this.cols            = cols;
        this.rows            =    0;
        this.localsBegin     =    0;
        this.localsLen       =    0;
        this.offerNextLocals =   -1;
        if (termsCapacity < cols)
            growForCols(cols);
        return this;
    }

    private void growForCols(int cols) {
        shared        = segmentRopesAtLeast(cols, shared);
        termsCapacity = shared.length;
        if (slices.length < termsCapacity<<1)
            slices = intsAtLeast(termsCapacity<<1, slices);
        BatchEvent.TermsGrown.record(this);
    }

    protected void swapLocals(CompressedBatch other) {
        var otherLocals    = other.locals;
        var otherLocalsSeg = other.localsSeg;
        other.locals       =  this.locals;
        other.localsSeg    =  this.localsSeg;
        this .locals       = otherLocals;
        this .localsSeg    = otherLocalsSeg;
    }

    @Override public void reserveAddLocals(int addBytes) {
        int req = localsBegin+localsLen+addBytes;
        if (req <= locals.length)
            growLocals(req-addBytes, req);
    }

    private void growLocals(int currentEnd, int reqLen) {
        byte[] bigger = bytesAtLeast(reqLen);
        if (currentEnd > 0)
            arraycopy(locals, localsBegin, bigger, 0, currentEnd-localsBegin);
        BYTE.offer(locals, locals.length);
        localsBegin = 0;
        locals      = bigger;
        localsSeg   = MemorySegment.ofArray(bigger);
        BatchEvent.LocalsGrown.record(this);
    }

    @Override public void abortPut() throws IllegalStateException {
        requireUnpooled();
        if (offerNextLocals < 0)
            return; // not inside an uncommitted offer/put
        fill(locals, localsLen, locals.length, (byte)0);
        offerNextLocals = -1;
    }

    @Override public CompressedBatch beginPut() {
        requireUnpooled();
        int terms = (rows+1)*cols;
        var dst = terms <= termsCapacity ? this
                : grown(terms, cols, localsLen+64, null, null);
        dst.beginPut0();
        return dst;
    }

    private void beginPut0() {
        offerNextLocals = localsBegin+localsLen;
        int begin = rows*cols, end = begin+cols;
        var shared = this.shared;
        var slices = this.slices;
        for (int i = begin                  ; i < end ; i++) shared[i] = null;
        for (int i = begin<<1, end2 = end<<1; i < end2; i++) slices[i] = 0;
    }

    @Override public void putTerm(int col, Term t) {
        SegmentRope shared, local;
        if (t == null) { shared =      EMPTY; local =     EMPTY; }
        else           { shared = t.shared(); local = t.local(); }
        int fLen = local.len | (t != null && t.sharedSuffixed() ? SH_SUFF_MASK : 0);
        int dest = allocTerm(col, shared, fLen);
        local.copy(0, local.len, locals, dest);
    }

    @Override public void putTerm(int destCol, CompressedBatch other, int row, int col) {
        int[] oSl = other.slices;
        int oBase = other.slBase(row, col), fLen = oSl[oBase + SL_LEN];
        int dest = allocTerm(destCol, other.shared[row*other.cols+col], fLen);
        arraycopy(other.locals, oSl[oBase+SL_OFF], locals, dest, fLen&LEN_MASK);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, MemorySegment local, long localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        MemorySegment.copy(local, JAVA_BYTE, localOff, locals, dest, localLen);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, byte[] local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTerm(col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        arraycopy(local, localOff, locals, dest, localLen);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, SegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        int dest = allocTerm(col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, locals, dest);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        int dest = allocTerm(col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, locals, dest);
    }

    @Override public void commitPut() {
        requireUnpooled();
        if (offerNextLocals < 0) throw new IllegalStateException();
        commitPut0();
        assert validate() : "corrupted";
    }

    public void commitPut0() {
        ++rows;
        localsLen       = offerNextLocals-localsBegin;
        offerNextLocals = -1;
    }

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
//        int lDst = localsLen, lLen = o.localsLen;
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


    @Override public CompressedBatch
    put(CompressedBatch o, @Nullable VarHandle rec, @Nullable Object holder) {
        final int oRows = o.rows, cols = this.cols, terms = (rows+oRows)*cols;
        if (cols != o.cols) {
            throw new IllegalArgumentException("cols != other.cols");
        } else if (cols == 0 || oRows == 0) {
            this.rows += oRows;
            return this;
        } else if (MARK_POOLED) {
            this .requireUnpooled();
            o    .requireUnpooled();
        }

        var dst = terms <= termsCapacity ? this
                : grown(terms, cols, localsLen+o.localsLen, rec, holder);
        dst.doAppend(o, oRows, o.localsBegin, o.localsLen);
        assert dst.validate() : "corrupted";
        return dst;
    }

    @Override public CompressedBatch put(CompressedBatch other) {
        return put(other, null, null);
    }

    private CompressedBatch
    grown(int terms, int cols, int localReq, @Nullable VarHandle rec, @Nullable Object holder) {
        var b = COMPRESSED.createForTerms(terms, cols);
        if (b.locals.length < localReq)
            b.growLocals(0, localReq);
        b.doAppend(this, rows, localsBegin, localsLen);
        BatchEvent.TermsGrown.record(b);
        if (rec != null) {
            if (rec.compareAndExchangeRelease(holder, null, markPooled()) == null)
                return b;
            markUnpooledNoTrace();
        }
        COMPRESSED.recycle(this);
        return b;
    }

    private void doAppend(CompressedBatch o, int rowCount, int lSrc, int lLen) {
        int dstTerm = rows*cols, nTerms = rowCount*cols;
        this.rows += rowCount;
        int        [] isl = o.slices, sl = slices;
        SegmentRope[] ish = o.shared, sh = shared;
        int lDst = doAppendLocals(o.locals, lSrc, lLen);
        arraycopy(ish, 0, sh, dstTerm, nTerms);
        arraycopy(isl, 0, sl, dstTerm<<=1, nTerms<<=1);
        for (int i = dstTerm+SL_OFF, e = dstTerm+nTerms, d = lDst-lSrc; i < e; i += 2)
            sl[i] += d;
    }

    private int doAppendLocals(byte[] other, int begin, int len) {
        int lDst = localsBegin+localsLen;
        if (lDst+len > locals.length)
            growLocals(lDst, lDst+len);
        this.localsLen += len;
        arraycopy(other, begin, locals, lDst, len);
        return lDst;
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

    @Override public CompressedBatch putRow(CompressedBatch o, int row) {
        int cols = this.cols, dstTerm = rows*cols, srcTerm = row*cols;
        if (cols != o.cols) {
            throw new IllegalArgumentException("o.cols != cols");
        } else if (cols == 0) {
            this.rows++;
            return this;
        } else if (MARK_POOLED) {
            this.requireUnpooled();
            o   .requireUnpooled();
        }

        int lLen = o.localBytesUsed(row);
        var dst = dstTerm+cols <= termsCapacity ? this
                : grown(dstTerm+cols, cols, localsLen+lLen, null, null);
        int lDst = dst.localsBegin+dst.localsLen;
        if (lDst+lLen > dst.locals.length)
            dst.growLocals(lDst, dst.localsLen+lLen);
        dst.localsLen = lDst+lLen-dst.localsBegin;
        ++dst.rows;
        int [] osl = o.slices, dsl = dst.slices;
        byte[] olo = o.locals, dlo = dst.locals;
        arraycopy(o.shared, srcTerm, dst.shared, dstTerm, cols);
        for (int c = 0, fLen; c < cols; c++) {
            int dPos = (dstTerm+c)<<1, oPos = (srcTerm+c)<<1;
            dsl[dPos+SL_OFF] = lDst;
            dsl[dPos+SL_LEN] = fLen = osl[oPos+SL_LEN];
            arraycopy(olo, osl[oPos+SL_OFF], dlo, lDst, fLen&LEN_MASK);
            lDst += fLen&LEN_MASK;
        }

        assert dst.validate() : "corrupted";
        return dst;
    }

    static { assert Integer.bitCount(SL_OFF) == 0 : "update lastOff/rowOff"; }
    @Override public @This CompressedBatch putConverting(Batch<?> other, VarHandle rec,
                                                         Object holder) {
        int cols = this.cols, oRows = other.rows, terms = (rows+oRows)*cols;
        if (other instanceof CompressedBatch cb) {
            return put(cb, rec, holder);
        } else if (cols != other.cols) {
            throw new IllegalArgumentException();
        } else if (cols == 0 || oRows <= 0) {
            rows += oRows;
            return this;
        } else if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        int localReq = localsLen+max(cols << 4, rows == 0 ? 0 : localsLen/rows);
        CompressedBatch dst;
        if (terms > termsCapacity) {
            dst = grown(terms, cols, localReq, rec, holder);
        } else {
            dst = this;
            if ((localReq += localsBegin) > locals.length)
                dst.growLocals(localsBegin+localsLen, localReq);
        }

        var t = TwoSegmentRope.pooled();
        for (int r = 0; r < oRows; r++)
            dst.putRowConverting(other, r, t, cols);
        t.recycle();
        return dst;
    }

    @Override public CompressedBatch putRowConverting(Batch<?> other, int row) {
        int cols = this.cols, rows = this.rows, terms = (rows+1)*cols;
        if (other instanceof CompressedBatch cb) {
            return putRow(cb, row);
        } else if (cols != other.cols) {
            throw new IllegalArgumentException("cols mismatch");
        } else if (cols == 0) {
            this.rows = rows+1; return this;
        } else if (MARK_POOLED) {
            this .requireUnpooled();
            other.requireUnpooled();
        }

        var t = TwoSegmentRope.pooled();
        var dst = terms <= termsCapacity ? this
                : grown(terms, cols, localsLen+64, null, null);
        dst.putRowConverting(other, row, t, cols);
        t.recycle();
        return dst;
    }

    private void putRowConverting(Batch<?> other, int row, TwoSegmentRope t, int cols) {
        beginPut0();
        for (int c = 0; c < cols; c++) {
            if (other.getRopeView(row, c, t)) {
                byte fst = t.get(0);
                SegmentRope sh = switch (fst) {
                    case '"' -> SHARED_ROPES.internDatatypeOf(t, 0, t.len);
                    case '<' -> SHARED_ROPES.  internPrefixOf(t, 0, t.len);
                    case '_' -> EMPTY;
                    default -> throw new IllegalArgumentException("Not an RDF term: "+ t);
                };
                int localLen = t.len-sh.len, localOff = fst == '<' ? sh.len : 0;
                int dest = allocTerm(c, sh,
                                     localLen|(fst == '"'? SH_SUFF_MASK : 0));
                t.copy(localOff, localOff+localLen, locals, dest);
            }
        }
        commitPut0();
    }

    /* --- --- --- operation objects --- --- --- */

    public static final class Merger extends BatchMerger<CompressedBatch> {
        private static final SegmentRope[] NULL_SHARED = {null};
        private static final int        [] NULL_SLICES = {0, 0};
        private final int[] leftSlices;
        private final SegmentRope[] leftShared;

        public Merger(BatchType<CompressedBatch> batchType, Vars outVars, int[] sources) {
            super(batchType, outVars, sources);
            this.leftShared  = new SegmentRope[sources.length];
            this.leftSlices  = new int        [sources.length<<1];
        }

        @Override
        public CompressedBatch merge(@Nullable CompressedBatch dst, CompressedBatch left, int leftRow, @Nullable CompressedBatch right) {
            int rows = right == null ? 0 : right.rows;
            dst = COMPRESSED.withCapacity(dst, rows, sources.length);
            if (rows == 0)
                return mergeWithMissing(dst, left, leftRow);
            int[] sources = this.sources;
            int lAdj = appendLocalsAndPrepareLeftPointers(dst, left, leftRow, right);
            int d = dst.rows*dst.cols, dEnd = d+rows*sources.length;
            dst.rows += rows;
            int        [] dsl = dst.slices, rsl = right.slices, osl;
            SegmentRope[] dsh = dst.shared, rsh = right.shared, osh;
            int rCols = right.cols;
            for (int rt = 0; d < dEnd; d+=sources.length, rt+=rCols) {
                for (int c = 0, src; c < sources.length; c++) {
                    int dPos = d+c;
                    if ((src=sources[c]) > 0) {
                        osh = leftShared;  osl = leftSlices;  src =    src-1;
                    } else if (src < 0) {
                        osh = rsh;         osl = rsl;         src = rt-src-1;
                    } else {
                        osh = NULL_SHARED; osl = NULL_SLICES;
                    }
                    dsh[dPos    ] = osh[src    ];
                    dsl[dPos<<=1] = osl[src<<=1]+lAdj;
                    dsl[dPos + 1] = osl[src + 1];
                }
            }
            assert dst.validate();
            return dst;
        }

        private CompressedBatch
        mergeWithMissing(CompressedBatch dst, CompressedBatch left, int leftRow) {
            dst.beginPut0();
            for (int c = 0, src; c < sources.length; c++) {
                if ((src = sources[c]) > 0)
                    dst.putTerm(c, left, leftRow, src-1);
            }
            dst.commitPut0();
            return dst;
        }

        private int appendLocalsAndPrepareLeftPointers(CompressedBatch dst, CompressedBatch left, int leftRow,
                                                       @Nullable CompressedBatch right) {
            int lReq = dst.localsLen + left.localBytesUsed(leftRow)
                                     + (right == null ? 0 : right.localsLen);
            if (dst.localsBegin+lReq > dst.locals.length)
                dst.growLocals(dst.localsBegin+dst.localsLen, lReq);
            int lDst = dst.localsBegin+dst.localsLen;
            SegmentRope[] sh = left.shared;
            int        [] sl = left.slices;
            byte       [] dLocals= dst.locals, lLocals = left.locals;
            int base = leftRow*left.cols;
            for (int c = 0, lc, len; c < sources.length; c++) {
                if ((lc=sources[c]-1) < 0) continue;
                int i = base+lc;
                leftShared[lc] = sh[i];
                leftSlices[(lc<<=1)+SL_OFF] = lDst;
                leftSlices[ lc     +SL_LEN] = len = sl[(i<<=1)+SL_LEN];
                arraycopy(lLocals, sl[i+SL_OFF], dLocals, lDst, len&=LEN_MASK);
                lDst += len;
            }
            dst.localsLen = lDst-dst.localsBegin;
            if (right != null) {
                int rBegin = right.localsBegin, lAdj = lDst-rBegin;
                dst.doAppendLocals(right.locals, rBegin, right.localsLen);
                for (int i = SL_OFF; i < leftSlices.length; i+=2) leftSlices[i] -= lAdj;
                return lAdj;
            }
            return 0;
        }

        private void projectInto(CompressedBatch dst, CompressedBatch in, int[] cols, int lAdjust) {
            int inCols = in.cols, inTerms = in.rows*inCols, dZero;
            if (dst == in) {
                dZero = 0;
                dst.cols = cols.length;
            } else {
                dZero = dst.rows*dst.cols;
                dst.rows += in.rows;
            }
            SegmentRope[] dsh = dst.shared, ish = in.shared;
            int        [] dsl = dst.slices, isl = in.slices;

            // project shared
            for (int i=0, d=dZero; i < inTerms; i+=inCols, d+=cols.length) {
                for (int c = 0, src; c < cols.length; c++)
                    dsh[d+c] = (src=cols[c]) < 0 ? null : ish[i+src];
            }

            // project slices
            for (int i = 0, d = dZero; i < inTerms; i+=inCols, d+=cols.length) {
                for (int c = 0, dstPos = d<<1, src; c < cols.length; c++, dstPos += 2) {
                    if ((src = cols[c]) < 0) {
                        dsl[dstPos  ] = 0;
                        dsl[dstPos+1] = 0;
                    } else {
                        int inPos = (i+src)<<1;
                        dsl[dstPos  ] = isl[inPos  ]+lAdjust;
                        dsl[dstPos+1] = isl[inPos+1];
                    }
                }
            }
            assert dst.validate();
        }

        @Override public CompressedBatch project(CompressedBatch dst, CompressedBatch in) {
            int[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            dst = COMPRESSED.withCapacity(dst, in.rows, cols.length);
            int lBegin = in.localsBegin;
            projectInto(dst, in, cols,
                    dst.doAppendLocals(in.locals, lBegin, in.localsLen)-lBegin);
            return dst;
        }

        @Override
        public CompressedBatch projectRow(@Nullable CompressedBatch dst, CompressedBatch in, int row) {
            int[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            dst = COMPRESSED.withCapacity(dst, 1, cols.length);
            dst.beginPut0();
            for (int c = 0, src; c < cols.length; c++) {
                if ((src=cols[c]) >= 0)
                    dst.putTerm(c, in, row, src);
            }
            dst.commitPut0();
            return dst;
        }

        @Override public CompressedBatch projectInPlace(CompressedBatch in) {
            final int[] cols = this.columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            final int terms = in.rows*cols.length;
            if (terms == 0) // early exit allows eliding checks for underflow
                return projectInPlaceEmpty(in);

            CompressedBatch dst;
            if (safeInPlaceProject)
                dst = in;
            else
                in.swapLocals(dst = COMPRESSED.createForTerms(terms, cols.length));
            projectInto(dst, in, cols, 0);
            if (dst != in)
                COMPRESSED.recycle(in);
            return dst;
        }
    }

    public static final class Filter extends BatchFilter<CompressedBatch> {
        private final Merger projector;
        public Filter(BatchType<CompressedBatch> batchType, Vars outVars,
                      Merger projector, RowFilter<CompressedBatch> rowFilter,
                      @Nullable BatchFilter<CompressedBatch> before) {
            super(batchType, outVars, rowFilter, before);
            assert projector == null || projector.vars.equals(outVars);
            this.projector = projector;
        }

        private CompressedBatch filterInto(CompressedBatch dst, CompressedBatch in) {
            RowFilter.Decision decision = DROP;
            int rows = in.rows, cols = in.cols;
            int r = 0, dstTerm, lDelta;
            int        [] isl = in.slices, dsl = dst.slices;
            SegmentRope[] ish = in.shared, dsh = dst.shared;
            if (dst == in) { // avoid no-op arraycopy if first rows are kept
                while (r < rows && (decision = rowFilter.drop(in, r)) == KEEP)
                    ++r;
                dstTerm = r*cols;
                ++r; // do not re-evaluate the non-KEEP row
                lDelta = 0;
            } else {
                dstTerm = dst.rows*cols;
                int lSrc = in.localsBegin;
                lDelta = dst.doAppendLocals(in.locals, lSrc, in.localsLen)-lSrc;
            }
            for (; r < rows && (decision != TERMINATE); r++) {
                int start = r;
                while (r < rows && (decision = rowFilter.drop(in, r)) == KEEP) ++r;
                if (r > start) {
                    int nTerms = (r-start)*cols, srcTerm = start*cols;
                    arraycopy(ish, srcTerm, dsh, dstTerm, nTerms);
                    arraycopy(isl, srcTerm<<1, dsl, dstTerm<<1, nTerms<<1);
                    if (lDelta > 0) {
                        for (int i = (dstTerm<<1)+SL_OFF, e = i+nTerms<<1; i < e; i+=2)
                            dsl[i] += lDelta;
                    }
                    dstTerm += nTerms;
                }
            }
            return endFilter(dst, dstTerm, cols, decision==TERMINATE);
        }

        private CompressedBatch projectingFilterInto(CompressedBatch dst, CompressedBatch in,
                                                     Merger projector) {
            int[] columns = projector.columns;
            assert columns != null;
            RowFilter.Decision decision = DROP;
            int rows = in.rows, inCols = in.cols, dstTerm;
            int        [] isl = in.slices, dsl = dst.slices;
            SegmentRope[] ish = in.shared, dsh = dst.shared;
            int lDelta;
            if (dst == in) {
                lDelta = 0;
                dstTerm = 0;
            } else {
                int lBegin = in.localsBegin;
                lDelta = dst.doAppendLocals(in.locals, lBegin, in.localsLen)-lBegin;
                dstTerm = dst.rows*dst.cols;
            }
            for (int r = 0; r < rows && decision != TERMINATE; r++) {
                if ((decision = rowFilter.drop(in, r)) == KEEP)  {
                    int srcTerm = r*inCols;
                    for (int c = 0, srcCol; c < columns.length; c++) {
                        int dstPos = dstTerm+c;
                        if ((srcCol = columns[c]) < 0) {
                            dsh[dstPos    ] = null;
                            dsl[dstPos<<=1] = 0;
                            dsl[dstPos + 1] = 0;
                        } else {
                            int srcPos      = srcTerm+srcCol;
                            dsh[dstPos    ] = ish[srcPos    ];
                            dsl[dstPos<<=1] = isl[srcPos<<=1] + lDelta;
                            dsl[dstPos + 1] = isl[srcPos + 1];
                        }
                    }
                    dstTerm += columns.length;
                }
            }
            return endFilter(dst, dstTerm, columns.length, decision==TERMINATE);
        }

        @Override public CompressedBatch filter(@Nullable CompressedBatch dst, CompressedBatch in) {
            if (before != null)
                in = before.filter(null, in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(dst, in);
            var projector = this.projector;
            CompressedBatch garbage = null;
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
            dst = COMPRESSED.withCapacity(dst, in.rows, outColumns);
            dst = projector == null ? filterInto(dst, in)
                                    : projectingFilterInto(dst, in, projector);
            if (garbage != null)
                COMPRESSED.recycle(garbage);
            return dst;
        }

        @Override public CompressedBatch filterInPlace(CompressedBatch in) {
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

        private CompressedBatch
        filterInPlaceProjecting(CompressedBatch in, Merger p) {
            CompressedBatch dst, garbage;
            if (p.safeInPlaceProject) {
                garbage = null;
                dst = in;
            } else {
                garbage = in;
                dst = COMPRESSED.createForTerms(in.rows*outColumns, outColumns);
            }
            dst = projectingFilterInto(dst, in, p);
            if (garbage != null)
                COMPRESSED.recycle(garbage);
            return dst;
        }
    }

}
