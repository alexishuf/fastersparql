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

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.isNumericDatatype;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.HAS_UNSAFE;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;

public class CompressedBatch extends Batch<CompressedBatch> {
    static final short SH_SUFF_MASK = (short)0x8000;
    static final short     LEN_MASK = (short)0x7fff;
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;
    private static final byte[]        EMPTY_LOCALS     = EMPTY.u8();
    private static final MemorySegment EMPTY_LOCALS_SEG = EMPTY.segment;

    /**
     * For the term at row {@code r} and column {@code c}:
     * <ul>
     *     <li>index {@code r*cols*2 + (c<<1) + SL_OFF} store the offset into {@code locals} where the local
     *         segment of the term starts.</li>
     *     <li>index {@code r*cols*2 + (c<<1) + SL_LEN} stores the length of the local segment of the
     *         term in bits [0,15) and whether the shared segment comes before (0) or after (1)
     *         at bit 15 (see {@code SH_SUFFIX_MASK})</li>
     * </ul>
     */
    private final short[] slices;

    /**
     * Array with the shared segments of all terms in this batch. The segment for term at
     * {@code (row, col)} is stored at index {@code row*cols + col}.
     */
    private final SegmentRope[] shared;

    /**
     * Equivalent to {@code shared.length}. This field exists to reduce cache misses from
     * checking {@code shared.length}, which requires de-referencing the pointer.
     */
    private final short termsCapacity;

    /**
     * The total number of bytes in {@code locals} being currently used by the rows of this batch.
     */
    private short localsLen;

    /** Storage for local parts of terms. */
    private byte[] locals;
    /** {@code MemorySegment.ofArray(locals)} */
    private MemorySegment localsSeg;

    /** {@code -1} if not in a {@link #beginPut()}. Else this is the index
     * into {@code locals} where local bytes for the next column shall be written to. */
    private short offerNextLocals = -1;

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

    protected boolean validateNode(Validation validation) {
        if (!SELF_VALIDATE || validation == Validation.NONE)
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
                short off = slices[i+SL_OFF], fLen = slices[i+SL_LEN];
                int end = off+fLen&LEN_MASK;
                if ((fLen&LEN_MASK) != 0 && (off < 0 || end > locals.length || end < off))
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
        return super.validateNode(validation);
    }

    private short slBase(int row, int col) {
        requireUnpooled();
        int cols2 = cols<<1, col2 = col<<1;
        if ((row|col) < 0 || row >= rows || col2 > cols2)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row, col));
        return (short)(row*cols2+col2);
    }

    private CompressedBatch createTail() {
        CompressedBatch b = COMPRESSED.create(cols), tail = this.tail;
        if (tail == null)
            throw new UnsupportedOperationException("intermediary batch");
        tail.tail = null;
        tail.next = b;
        this.tail = b;
        return b;
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
    private int allocTermMaybeChangeTail(int destCol, SegmentRope shared, int flaggedLocalLen) {
        var tail = this.tailUnchecked();
        if (tail.offerNextLocals < 0) throw new IllegalStateException();
        short rows = tail.rows, cols = tail.cols;
        if (destCol < 0 || destCol >= cols)
            throw new IndexOutOfBoundsException("destCol not in [0, cols)");
        // find write location in md and grow if needed
        short slBase = (short)((rows*cols+destCol)<<1), dest = tail.offerNextLocals;
        short[] slices = tail.slices;
        if (slices[slBase+SL_OFF] != 0)
            throw new IllegalStateException("Column already set");
        int len = flaggedLocalLen & LEN_MASK;
        if (len > 0) {
            int required  = dest+len;
            if (required > Short.MAX_VALUE)
                return allocTermOnNewTail(destCol, shared, flaggedLocalLen);
            else if (required > tail.locals.length)
                tail.growLocals(dest, required);
            tail.offerNextLocals = (short)required;
        } else if (shared != null && shared.len > 0) {
            throw new IllegalArgumentException("Empty local with non-empty shared");
        }
        tail.shared[rows*cols + destCol] = shared != null && shared.len == 0 ? null : shared;
        slices[slBase+SL_OFF] = dest;
        slices[slBase+SL_LEN] = (short)flaggedLocalLen;
        return dest;
    }

    private int allocTermOnNewTail(int destCol, SegmentRope shared, int flaggedLocalLen) {
        CompressedBatch prev = this.tail, tail = createTail();
        assert prev != null;
        tail.offerNextLocals = (short)(prev.offerNextLocals-prev.localsLen);
        prev.offerNextLocals = -1;

        short cols = prev.cols, begin = (short)(prev.rows*cols);
        short[] prevSlices = prev.slices, slices = tail.slices;
        int localsLen = flaggedLocalLen&LEN_MASK;
        for (int i = begin+SL_LEN, e = begin+(cols<<1); i < e; i += 2)
            localsLen += prevSlices[i]&LEN_MASK;
        if (localsLen > LEN_MASK)
            raiseRowTooWide(prev, tail);
        if (localsLen > tail.locals.length)
            tail.growLocals(0, min(LEN_MASK, localsLen<<1));

        byte[] prevLocals = prev.locals, locals = tail.locals;
        localsLen = 0;
        for (int i = begin, e = begin+(cols<<1); i < e; i+=2) {
            slices[i+SL_OFF] = (short)localsLen;
            short len = prevSlices[i+SL_LEN];
            slices[i+SL_LEN] = len;
            arraycopy(prevLocals, prevSlices[i+SL_OFF], locals, localsLen, len&=LEN_MASK);
            localsLen += len;
        }
        tail.localsLen = (short)localsLen;
        return allocTermMaybeChangeTail(destCol, shared, flaggedLocalLen);
    }

    private void raiseRowTooWide(CompressedBatch prev, CompressedBatch tail) {
        prev.next = tail.recycle();
        prev.tail = prev;
        this.tail = prev;
        throw new IllegalArgumentException("row local segments are too wide");
    }


    /* --- --- --- lifecycle --- --- --- */

    CompressedBatch(short terms, short cols) {
        super((short)0, cols);
        this.locals        = bytesAtLeastUpcycle(terms<<5);
        this.localsSeg     = MemorySegment.ofArray(locals);
        this.shared        = segmentRopesAtLeastUpcycle(Math.max(terms, cols));
        this.termsCapacity = (short)Math.min(Short.MAX_VALUE, shared.length);
        this.slices        = shortsAtLeast(termsCapacity<<1);
        BatchEvent.Created.record(this);
    }


    @Override void markGarbage() {
        if (MARK_POOLED && (int)P.getAndSetRelease(this, P_GARBAGE) != P_POOLED)
            throw new IllegalStateException("non-pooled marked garbage");
        SHORT   .offer(slices, slices.length);
        SEG_ROPE.offer(shared, termsCapacity);
        BYTE    .offer(locals, locals.length);
        //slices        = EMPTY_SHORT;
        //shared        = EMPTY_SEG_ROPE;
        //termsCapacity = 0;
        locals          = EMPTY_LOCALS;
        localsSeg       = EMPTY_LOCALS_SEG;
        BatchEvent.Garbage.record(this);
    }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public CompressedBatchType type() { return COMPRESSED; }

    @Override public CompressedBatch dup() {return dup((int)currentThread().threadId());}
    @Override public CompressedBatch dup(int threadId) {
        CompressedBatch b = COMPRESSED.createForThread(threadId, cols);
        b.copy(this);
        return b;
    }

    @Override public int      localBytesUsed() { return localsLen; }
    @Override public int       termsCapacity() { return termsCapacity; }
    @Override public int  totalBytesCapacity() { return termsCapacity*12 + locals.length; }

    @Override public int avgLocalBytesUsed() {
        int bytes = 0, rows = 0;
        for (var b = this; b != null; b = b.next) {
            rows  += b.rows;
            bytes += b.localsLen;
        }
        return bytes/rows;
    }

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
        short slb = slBase(row, 0), cslb;
        for (int c = 0, cols = this.cols; c < cols; c++) {
            cslb = (short)(slb+(c<<1));
            int fstLen, sndLen = slices[cslb+SL_LEN];
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
                    fst =  locals; fstOff = sndOff; fstLen = sndLen&LEN_MASK;
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
        byte       [] oLocals = other.locals;
        short      [] osl     = other.slices;
        int slw = cols<<1, slb = row*slw, oslb = oRow*slw;
        int shBase = row*cols, oShBase = oRow*cols;
        for (int c = 0, c2, cslb, ocslb; c < cols; ++c) {
            SegmentRope sh = shared[shBase+c], osh = other.shared[oShBase+c];
            cslb = slb + (c2 = c << 1);
            ocslb = oslb + c2;
            if (!termEquals( sh,  localsSeg,  locals, slices[ cslb+SL_OFF], slices[ cslb+SL_LEN],
                            osh, oLocalsSeg, oLocals,    osl[ocslb+SL_OFF],    osl[ocslb+SL_LEN]))
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
        short[] sl = this.slices, osl = other.slices;
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

    @Override public CompressedBatch dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public CompressedBatch dupRow(int row, int threadId) {
        CompressedBatch b = COMPRESSED.createForThread(threadId, cols);
        b.putRow(this, row);
        return b;
    }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        short i2 = slBase(row, col), len = slices[i2+SL_LEN];
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
        short i2 = slBase(row, col), len = slices[i2+SL_LEN];
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
        short i2 = slBase(row, col), len = slices[i2+SL_LEN];
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
        byte[] u8 = copyOfRange(locals, off, off+len);
        tsr.wrapSecond(MemorySegment.ofArray(u8), u8, 0, len);
        if (suffix)
            tsr.flipSegments();
        return tsr;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        short i2 = slBase(row, col), len = slices[i2+SL_LEN];
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
        short slBase = slBase(row, col), localLen = slices[slBase+SL_LEN];
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

    @Override public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
        return slices[(row*cols+col)<<1+SL_LEN] & LEN_MASK;
    }
    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        return slices[slBase(row, col)+SL_LEN] & LEN_MASK;
    }

    @Override public Term.@Nullable Type termType(int row, int col) {
        short slBase = slBase(row, col), len = slices[slBase + SL_LEN];
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
        short slBase = slBase(row, col), len = slices[slBase+SL_LEN];
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
        short base = slBase(row, col), len = slices[base + SL_LEN];
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
        short base = slBase(row, col), len = slices[base+SL_LEN];
        int off = slices[base+SL_OFF];
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
        short base = slBase(row, col);
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
        short slb = slBase(row, col), len = this.slices[slb+SL_LEN];
        SegmentRope sh = shared[row * cols + col];
        if (sh == null)
            sh = EMPTY;
        if (other == null != (sh.len == 0 && (len&LEN_MASK) == 0))
            return false;
        else if (other == null)
            return true;
        SegmentRope ol = other.local();
        return termEquals(sh, localsSeg, locals, this.slices[slb+SL_OFF], len,
                          other.shared(), ol.segment, ol.utf8, ol.offset,
                          ol.len|(other.sharedSuffixed() ? SH_SUFF_MASK : 0));
    }

    public boolean safeEquals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        short slb = slBase(row, col), len = this.slices[slb + SL_LEN];
        SegmentRope sh = shared[row * cols + col];
        if (sh == null)
            sh = EMPTY;
        if (other == null != (sh.len == 0 && (len&LEN_MASK) == 0))
            return false;
        else if (other == null)
            return true;
        SegmentRope ol = other.local();
        return safeTermEquals(sh, localsSeg, locals, this.slices[slb+SL_OFF], len,
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
        short slb = slBase(row, col), oslb = other.slBase(oRow, oCol);
        short[] sl = this.slices, osl = other.slices;
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

    /* --- --- --- bucket --- --- --- */

    byte[] copyToBucket(byte[] rowData, SegmentRope[] bucketShared, @NonNegative int dstRow,
                        @NonNegative int srcRow) {
        short cols = this.cols, lBegin = (short)(cols<<2);
        arraycopy(shared, srcRow*cols, bucketShared, dstRow*cols, cols);
        short[] slices = this.slices;
        rowData = bytesAtLeast(lBegin+localBytesUsed(srcRow), rowData);
        for (short o = 0, lDst = lBegin, i = (short)(srcRow*cols<<1); o < lBegin; o += 4, i+=2) {
            rowData[o  ] = (byte)(lDst       );
            rowData[o+1] = (byte)(lDst >>>  8);
            short len = slices[i+SL_LEN];
            rowData[o+2] = (byte)(len       );
            rowData[o+3] = (byte)(len >>>  8);
            arraycopy(this.locals, slices[i+SL_OFF], rowData, lDst, len&=LEN_MASK);
            lDst += len;
        }
        return rowData;
    }

    void copyFromBucket(byte[] rowData, SegmentRope[] bucketShared, @NonNegative int srcRow) {
        short cols = this.cols, base, lLen, lAdj;
        var tail = tailUnchecked();
        if (cols               ==                  0) { tail.rows++; return; }
        if ((tail.rows+1)*cols >  tail.termsCapacity)   tail = createTail();

        base = (short)((cols-1)<<2);
        lLen = (short)(((rowData[base  ]&0xff) | ((rowData[base+1]&0xff) << 8))
                     + ((rowData[base+2]&0xff) | ((rowData[base+3]&0xff) << 8))&LEN_MASK);
        int lDst = reserveAddLocals0(lLen);
        arraycopy(rowData, base+4, tail.locals, lDst, lLen);
        lAdj = (short)(lDst-(base+4));

        base = (short)(tail.rows*cols);
        arraycopy(bucketShared, srcRow*cols, tail.shared, base, cols);

        short[] slices = tail.slices;
        for (short is=(short)(base<<1), slw = (short)(cols<<1), ib=0; is < slw; is += 2, ib += 4) {
            slices[is  ] = (short)(((rowData[ib  ]&0xff) | ((rowData[ib+1]&0xff) << 8)) + lAdj);
            slices[is+1] = (short)(((rowData[ib+2]&0xff) | ((rowData[ib+3]&0xff) << 8)));
        }
    }

    /* --- --- --- mutators --- --- --- */

    @Override public @Nullable CompressedBatch recycle() { return COMPRESSED.recycle(this); }

    @Override public void clear() {
        rows            =  0;
        localsLen       =  0;
        offerNextLocals = -1;
        tail            = this;
        if (next != null)
            next = COMPRESSED.recycle(next);
    }

    @Override public @This CompressedBatch clear(int cols) {
        if (cols > termsCapacity) {
            CompressedBatch bigger = COMPRESSED.create(cols);
            recycle();
            return bigger;
        }
        this.cols            = (short)cols;
        this.rows            =  0;
        this.localsLen       =  0;
        this.offerNextLocals = -1;
        this.tail            = this;
        if (this.next != null)
            this.next = COMPRESSED.recycle(next);
        return this;
    }

    @Override public void reserveAddLocals(int addBytes) { reserveAddLocals0(addBytes); }

    private int reserveAddLocals0(int addBytes) {
        var tail = this.tail;
        if (tail == null)
            tail = this;
        int reqLen = tail.localsLen+addBytes;
        if (reqLen > Short.MAX_VALUE) {
            if (tail.rows == 0) throw new UnsupportedOperationException("locals too wide");
            tail = createTail();
            reqLen = addBytes;
        }
        if (reqLen > tail.locals.length)
            tail.growLocals(tail.localsLen, reqLen);
        return tail.localsLen;
    }

    private void growLocals(int currentEnd, int reqLen) {
        byte[] bigger = bytesAtLeast(reqLen);
        if (currentEnd > 0)
            arraycopy(locals, 0, bigger, 0, currentEnd);
        if (locals != EMPTY.utf8)
            BYTE.offer(locals, locals.length);
        locals      = bigger;
        localsSeg   = MemorySegment.ofArray(bigger);
        BatchEvent.LocalsGrown.record(this);
    }

    @Override public void abortPut() throws IllegalStateException {
        var tail = tail();
        if (tail.offerNextLocals < 0)
            return; // not inside an uncommitted offer/put
        tail.offerNextLocals = -1;
        if (tail != this && tail.rows == 0)
            dropTail(tail);
    }

    @Override public void beginPut() {
        var tail = this.tail;
        if (tail == null) throw new UnsupportedOperationException("intermediary batch");
        if ((tail.rows+1)*tail.cols > tail.termsCapacity)
            tail = createTail();
        tail.beginPut0();
    }

    private void beginPut0() {
        offerNextLocals = localsLen;
        short begin = (short)(rows*cols), end = (short)(begin+cols);
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
        int dest = allocTermMaybeChangeTail(col, shared, fLen);
        local.copy(0, local.len, tailUnchecked().locals, dest);
    }

    @Override public void putTerm(int destCol, CompressedBatch other, int row, int col) {
        short[] oSl = other.slices;
        short oBase = other.slBase(row, col), fLen = oSl[oBase+SL_LEN];
        int dest = allocTermMaybeChangeTail(destCol, other.shared[row*other.cols+col], fLen);
        arraycopy(other.locals, oSl[oBase+SL_OFF], tailUnchecked().locals, dest, fLen&LEN_MASK);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, MemorySegment local, long localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTermMaybeChangeTail(col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        MemorySegment.copy(local, JAVA_BYTE, localOff, tailUnchecked().locals, dest, localLen);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, byte[] local, int localOff, int localLen, boolean sharedSuffix) {
        int dest = allocTermMaybeChangeTail(col, shared,
                             localLen | (sharedSuffix ? SH_SUFF_MASK : 0));
        arraycopy(local, localOff, tailUnchecked().locals, dest, localLen);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, SegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        int dest = allocTermMaybeChangeTail(col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, tailUnchecked().locals, dest);
    }

    @Override
    public void putTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        int dest = allocTermMaybeChangeTail(col, shared,
                             localLen|(sharedSuffix ? SH_SUFF_MASK : 0));
        local.copy(localOff, localOff+localLen, tailUnchecked().locals, dest);
    }

    @Override public void commitPut() {
        var tail = this.tail;
        if (tail == null) throw new UnsupportedOperationException("intermediary batch");
        if (tail.offerNextLocals < 0) throw new IllegalStateException();

        tail.commitPut0();
        assert tail.validate() : "corrupted";
    }

    public void commitPut0() {
        ++rows;
        localsLen       = offerNextLocals;
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

    @Override public void copy(CompressedBatch o) {
        short cols = this.cols;
        if (o.cols != cols) {
            throw new IllegalArgumentException("cols mismatch");
        } else if (cols == 0) {
            addRowsToZeroColumns(o.totalRows());
        } else {
            CompressedBatch dst = tail();
            for (; o != null; o = o.next) {
                o.requireUnpooled();
                short lDst = dst.localsLen;
                short tCap = (short) (dst.termsCapacity - dst.rows * cols);
                if (o.rows * cols > tCap || lDst + o.localsLen >= LEN_MASK)
                    dst = coldCopyNode(o);
                else
                    dst.doAppend(o, o.rows, o.localsLen);
            }
            assert validate() : "corrupted";
        }
    }

    private CompressedBatch coldCopyNode(CompressedBatch o) {
        for (int r = 0, oRows = o.rows; r < oRows; r++)
            putRow(o, r);
        return tailUnchecked();
    }

    @Override public void append(CompressedBatch other) {
        short cols = this.cols;
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (rows == 0)
            other = copyFirstNodeToEmpty(other);
        CompressedBatch dst = tail(), src = other, prev = null;
        for (; src != null; src = (prev = src).next) {
            src.requireUnpooled();
            short lDst = dst.localsLen, tCap = (short)(dst.termsCapacity - dst.rows*cols);
            if (src.rows*cols > tCap || lDst+src.localsLen >= Short.MAX_VALUE)
                break;
            dst.doAppend(src, src.rows, src.localsLen);
        }
        if (src != null)
            other = appendRemainder(other, prev, src);
        COMPRESSED.recycle(other);
        assert validate() : "corrupted";
    }

    private void doAppend(CompressedBatch o, short rowCount, int lLen) {
        short dstTerm = (short)(rows*cols), nTerms = (short)(rowCount*cols);
        this.rows += rowCount;
        short[] slices = this.slices;
        short lDst = this.localsLen;
        int lReq = lDst+lLen;
        if (lReq > Short.MAX_VALUE)
            throw new IllegalArgumentException("locals.length will overflow");
        if (lReq > locals.length)
            growLocals(lDst, lReq);
        this.localsLen += (short)lLen;
        arraycopy(o.locals, 0, locals, lDst,        lLen);
        arraycopy(o.shared, 0, shared, dstTerm,     nTerms);
        arraycopy(o.slices, 0, slices, dstTerm<<=1, nTerms<<=1);
        for (int i = dstTerm+SL_OFF, e = dstTerm+nTerms; i < e; i += 2)
            slices[i] += lDst;
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
        short cols = this.cols;
        if (cols != o.cols) {
            throw new IllegalArgumentException("o.cols != cols");
        } else if (cols == 0) {
            this.rows++; return;
        }
        o.requireUnpooled();

        CompressedBatch dst = tail();
        short dstTerm = (short) (dst.rows * cols), lLen = (short) o.localBytesUsed(row);
        int lReq = dst.localsLen + lLen;
        if (dstTerm+cols > dst.termsCapacity || lReq > Short.MAX_VALUE) {
            dst = createTail();
            lReq = lLen;
            dstTerm = 0;
        }
        if (lReq > dst.locals.length)
            dst.growLocals(lReq-lLen, lReq);
        short lDst = dst.localsLen, srcTerm = (short) (row * cols);
        dst.localsLen = (short) (lDst + lLen);
        ++dst.rows;
        short[] osl = o.slices, dsl = dst.slices;
        byte[] olo = o.locals, dlo = dst.locals;
        arraycopy(o.shared, srcTerm, dst.shared, dstTerm, cols);
        for (int c = 0; c < cols; c++) {
            short dPos = (short) ((dstTerm + c) << 1), oPos = (short) ((srcTerm + c) << 1), fLen;
            dsl[dPos+SL_OFF] = lDst;
            dsl[dPos+SL_LEN] = fLen = osl[oPos+SL_LEN];
            arraycopy(olo, osl[oPos+SL_OFF], dlo, lDst, fLen&=LEN_MASK);
            lDst += fLen;
        }
        assert dst.validate() : "corrupted";
    }

    static { assert Integer.bitCount(SL_OFF) == 0 : "update lastOff/rowOff"; }
    @Override public void putConverting(Batch<?> other) {
        short cols = this.cols;
        if (other instanceof CompressedBatch cb) {
            copy(cb); return;
        } else if (cols != other.cols)
            throw new IllegalArgumentException();
        other.requireUnpooled();

        TwoSegmentRope t = TwoSegmentRope.pooled();
        for (; other != null; other = other.next) {
            for (short r = 0, oRows = other.rows; r < oRows; r++)
                putRowConverting(t, other, r, cols);
        }
        t.recycle();
    }

    @Override public void putRowConverting(Batch<?> other, int row) {
        short cols = this.cols;
        if (other instanceof CompressedBatch cb) {
            putRow(cb, row); return;
        } else if (cols != other.cols) {
            throw new IllegalArgumentException("cols mismatch");
        } else if (row >= other.rows) {
            throw new IndexOutOfBoundsException("row >= other.rows");
        }
        other.requireUnpooled();

        var t = TwoSegmentRope.pooled();
        putRowConverting(t, other, (short)row, cols);
        t.recycle();
    }

    private void putRowConverting(TwoSegmentRope t, Batch<?> other, short row, short cols) {
        beginPut();
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
                int dest = allocTermMaybeChangeTail(c, sh,
                        localLen|(fst == '"'? SH_SUFF_MASK : 0));
                t.copy(localOff, localOff+localLen, tailUnchecked().locals, dest);
            }
        }
        tailUnchecked().commitPut0();
    }

    /* --- --- --- operation objects --- --- --- */

    @SuppressWarnings("UnnecessaryLocalVariable")
    public static final class Merger extends BatchMerger<CompressedBatch> {
        private short[] leftSlices;
        private SegmentRope[] leftShared;
        private final short outColumns;

        public Merger(BatchType<CompressedBatch> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            this.leftShared = segmentRopesAtLeast(sources.length);
            this.leftSlices = shortsAtLeast(sources.length<<1);
            this.outColumns = (short)sources.length;
        }

        @Override protected void doRelease() {
            leftSlices = SHORT   .offer(leftSlices, leftSlices.length);
            leftShared = SEG_ROPE.offer(leftShared, leftShared.length);
            super.doRelease();
        }

        private CompressedBatch setupDst(CompressedBatch offer, boolean inplace) {
            int cols = outColumns;
            if (offer != null) {
                if (offer.rows == 0 || inplace)
                    offer.cols = (short)cols;
                else if (offer.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return offer;
            }
            return COMPRESSED.create(cols);
        }

        private CompressedBatch createTail(CompressedBatch root) {
            return root.setTail(COMPRESSED.create(outColumns));
        }

        private CompressedBatch mergeWithMissing(CompressedBatch dst, CompressedBatch left,
                                                 int leftRow) {
            dst.beginPut();
            for (int c = 0, src; c < sources.length; c++) {
                if ((src = sources[c]) > 0)
                    dst.putTerm(c, left, leftRow, src-1);
            }
            dst.commitPut0();
            return dst;
        }

        private short leftLocalsRequired(short[] sources, CompressedBatch left, int leftRow) {
            short slb = (short)((leftRow*left.cols<<1)+SL_LEN), sum = 0;
            for (int c = 0, s; c < sources.length; c++)
                if ((s=sources[c]) > 0) sum += (short)(left.slices[(slb+((s-1)<<1))]&LEN_MASK);
            return sum;
        }

        private short rightLocalsReq(short[] sources, short[] slices, short cols, short row,
                                     short lLocalsReq) {
            short sum = lLocalsReq, slb = (short)(row*(cols<<1)+SL_LEN);
            for (int c = 0, s; c < sources.length; c++)
                if ((s=sources[c]) < 0) sum += (short)(slices[slb-(s<<1)-2]&LEN_MASK);
            return sum;
        }

        @Override
        public CompressedBatch merge(@Nullable CompressedBatch dst, CompressedBatch left, int leftRow, @Nullable CompressedBatch right) {
            dst = setupDst(dst, false);
            short [] sources = this.sources;
            if (sources.length == 0)
                return mergeThin(dst, right);
            if (right == null || right.rows == 0)
                return mergeWithMissing(dst, left, leftRow);
            short      [] lsl = leftSlices;
            SegmentRope[] lsh = leftShared;
            short llr = leftLocalsRequired(sources, left, leftRow);
            short rc = right.cols, dc = dst.cols, lBase = (short)(left.cols*leftRow);
            CompressedBatch tail = dst.tailUnchecked();
            short dCap = tail.termsCapacity, dPos = (short)(tail.rows*tail.cols);
            short lDst = tail.localsLen, rlr;
            boolean hasLeftLocals = false;
            for (; right != null; right = right.next) {
                short[] rsl = right.slices;
                byte [] rlo = right.locals;
                for (short r = 0, rRows = right.rows, rBase=0; r < rRows; r++, rBase+=rc) {
                    rlr = rightLocalsReq(sources, rsl, rc, r, llr);
                    int lReq = lDst + (hasLeftLocals ? 0 : llr) + rlr;
                    if (dPos+dc > dCap || lReq > LEN_MASK) {
                        tail.rows = (short)(dPos/dc);
                        dCap = (tail = createTail(dst)).termsCapacity;
                        dPos = lDst = 0;
                        lReq = llr+rlr;
                        hasLeftLocals = false;
                    }
                    if (lReq > tail.locals.length)
                        tail.growLocals(lDst, lReq);
                    if (!hasLeftLocals) {
                        lDst = appendLeftLocals(tail, sources, left, lBase);
                        hasLeftLocals = true;
                    }
                    short      [] tsl = tail.slices;
                    SegmentRope[] tsh = tail.shared;
                    byte       [] tlo = tail.locals;
                    for (short c = 0, s, i; c < sources.length; c++, dPos++) {
                        short d2 = (short)(dPos<<1), len;
                        if ((s=sources[c]) == 0)  {
                            tsh[dPos]      = null;
                            tsl[d2+SL_LEN] = 0;
                        } else if (s > 0) {
                            tsh[dPos]      = lsh[i=(short)(s-1)];
                            tsl[d2+SL_OFF] = lsl[(i<<=1)+SL_OFF];
                            tsl[d2+SL_LEN] = lsl[ i     +SL_LEN];
                        } else  {
                            tsh[dPos]      = right.shared[i=(short)(rBase-s-1)];
                            tsl[d2+SL_OFF] = lDst;
                            tsl[d2+SL_LEN] = len = rsl[(i<<=1)+SL_LEN];
                            arraycopy(rlo, rsl[i+SL_OFF], tlo, lDst, len&=LEN_MASK);
                            lDst += len;
                        }
                    }
                    tail.localsLen = lDst;
                }
            }
            tail.rows = (short)(dPos/dc);
            assert dst.validate();
            return dst;
        }

        private short appendLeftLocals(CompressedBatch tail, short[] sources,
                                       CompressedBatch left, short lBase) {
            SegmentRope[] ish = left.shared, dsh = leftShared;
            short      [] isl = left.slices, dsl = leftSlices;
            byte       [] ilo = left.locals, dlo = tail.locals;
            short lDst = tail.localsLen, len, i;
            for (short s : sources) {
                if (s > 0) {
                    i = (short)(lBase+(--s));
                    dsh[s]              = ish[i];
                    dsl[(s<<=1)+SL_OFF] = lDst;
                    dsl[s+SL_LEN]       = len = isl[(i<<=1)+SL_LEN];
                    arraycopy(ilo, isl[i+SL_OFF], dlo, lDst, len&=LEN_MASK);
                    lDst += len;
                }
            }
            tail.localsLen = lDst;
            return lDst;
        }

        @Override public CompressedBatch project(CompressedBatch dst, CompressedBatch in) {
            short[] cols = this.columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            boolean inplace = dst == in;
            short ic = in.cols;
            dst = setupDst(dst, inplace);
            if (cols.length == 0)
                return mergeThin(dst, in);
            CompressedBatch tail = inplace ? in : dst.tailUnchecked();
            for (; in != null; in = in.next) {
                @SuppressWarnings("UnnecessaryLocalVariable")
                SegmentRope[] ish = in.shared, dsh;
                short      [] isl = in.slices, dsl;
                for (short ir=0, iRows=in.rows, dZero, nr, lDst=0; ir < iRows; ir+=nr) {
                    if (inplace) {
                        dZero          = 0;
                        (tail=in).rows = nr = iRows;
                        tail.cols      = (short)cols.length;
                    } else {
                        dZero = (short)(tail.rows*tail.cols);
                        int lReq = (lDst=tail.localsLen)+in.localsLen;
                        if (dZero+tail.cols > tail.termsCapacity || lReq > LEN_MASK) {
                            tail = createTail(dst);
                            lDst = dZero = 0;
                            lReq = in.localsLen;
                        }
                        nr = (short)min((tail.termsCapacity-dZero)/cols.length, iRows-ir);
                        tail.rows += nr;
                        if (lReq > tail.locals.length)
                            tail.growLocals(lDst, lReq);
                        arraycopy(in.locals, 0, tail.locals, lDst, in.localsLen);
                        tail.localsLen = (short)lReq;
                    }

                    // project shared
                    dsh = tail.shared;
                    dsl = tail.slices;
                    short i = (short)(ir*ic), ie = (short)((ir+nr)*ic);
                    for (short d = dZero; i < ie; i += ic, d+=(short)cols.length) {
                        for (int c = 0, s; c < cols.length; c++)
                            dsh[d+c] = (s=cols[c]) < 0 ? null : ish[i+s];
                    }

                    //project slices
                    i = (short)(ir*ic);
                    for (short d2=(short)(dZero<<1), i2, s; i < ie; i+=ic) {
                        for (int c=0; c < cols.length; c++, d2+=2) {
                            if ((s=cols[c]) < 0) {
                                dsl[d2  ] = 0;
                                dsl[d2+1] = 0;
                            } else {
                                dsl[d2  ] = (short)( isl[i2=(short)((i+s)<<1)] + lDst );
                                dsl[d2+1] = isl[i2+1];
                            }
                        }
                    }
                }
            }
            assert dst.validate();
            return dst;
        }

        @Override public CompressedBatch projectInPlace(CompressedBatch in) {
            if (in == null || in.rows == 0 || outColumns == 0)
                return projectInPlaceEmpty(in);
            var dst = project(safeInPlaceProject ? in : null, in);
            if (dst != in) COMPRESSED.recycle(in);
            return dst;
        }

        @Override public CompressedBatch processInPlace(CompressedBatch b) {
            return projectInPlace(b);
        }

        @Override public @Nullable CompressedBatch onBatch(CompressedBatch batch) {
            if (batch == null) return null;
            onBatchPrologue(batch);
            return onBatchEpilogue(projectInPlace(batch));
        }

        @Override
        public CompressedBatch projectRow(@Nullable CompressedBatch dst, CompressedBatch in, int row) {
            short[] cols = columns;
            if (cols == null)
                throw new UnsupportedOperationException("not a projecting merger");
            CompressedBatch tail = (dst = setupDst(dst, false)).tailUnchecked();

            // ensure tail has capacity for terms and locals
            short[] isl = in.slices;
            short ic = in.cols, lLen = 0;
            short slb = (short)((row*ic)<<1), sle = (short)(slb+(ic<<1));
            short lDst = tail.localsLen;
            short d = (short)(tail.rows*tail.cols);
            for (short i = (short)(slb+SL_LEN); i < sle; i++)
                lLen += (short)(isl[i]&LEN_MASK);
            if (d+cols.length > tail.termsCapacity || lDst+lLen > LEN_MASK) {
                tail = createTail(dst);
                lDst = d = 0;
            }
            if (lDst+lLen > tail.locals.length)
                tail.growLocals(lDst, lDst+lLen);

            //project slices and copy locals
            short[] dsl = tail.slices;
            byte[] ilo = in.locals, dlo = tail.locals;
            for (short c = 0, d2 = (short)(d<<1), s, len; c < cols.length; c++) {
                dsl[d2+SL_OFF] = lDst;
                dsl[d2+SL_LEN] = len = (s=(short)(cols[c]<<1)) < 0 ? 0 : isl[slb+s+SL_LEN];
                arraycopy(ilo, isl[slb+s+SL_OFF], dlo, lDst, len&=LEN_MASK);
                lDst += len;
            }

            // project shared
            @SuppressWarnings("UnnecessaryLocalVariable")
            SegmentRope[] ish = in.shared, dsh = tail.shared;
            for (int c = 0, s, i = slb>>1; c < cols.length; c++)
                dsh[d] = (s=cols[c]) < 0 ? null : ish[i+s];

            assert dst.validate();
            return dst;
        }

        @Override
        public CompressedBatch mergeRow(@Nullable CompressedBatch dst,
                                        CompressedBatch left, int leftRow,
                                        CompressedBatch right, int rightRow) {
            var tail = (dst = setupDst(dst, false)).tailUnchecked();
            if (dst.cols > 0) {
                short d = (short)(tail.rows*tail.cols), s;
                short l = (short)(leftRow*left.cols), r = (short)(rightRow*right.cols);
                // reserve locals/terms capacity
                int lEnd = tail.localsLen +  left.localBytesUsed(leftRow)
                                          + right.localBytesUsed(rightRow);
                if (lEnd > LEN_MASK || d+tail.cols > tail.termsCapacity) {
                    lEnd -= tail.localsLen;
                    tail = createTail(dst);
                    d = 0;
                }
                if (lEnd > LEN_MASK)
                    throw new UnsupportedOperationException("locals exceed LEN_MASK");
                if (lEnd > tail.locals.length)
                    tail.growLocals(tail.localsLen, tail.localsLen+lEnd);
                lEnd = tail.localsLen;

                SegmentRope[] dsh = tail.shared, lsh = left.shared, rsh = right.shared;
                short      [] dsl = tail.slices, lsl = left.slices, rsl = right.slices, isl;
                byte       [] dlo = tail.locals, llo = left.locals, rlo = right.locals, ilo;
                short i, o = d, len;
                for (int c = 0; c < sources.length; o = (short)(d+(++c))) {
                    if ((s=sources[c]) == 0) {
                        dsh[o]     = null;
                        dsl[o<<=1] = 0;
                        dsl[o]     = 0;
                    } else {
                        if (s > 0) {
                            dsh[o] = lsh[i=(short)(l+s-1)];
                            isl    = lsl;
                            ilo    = llo;
                        } else {
                            dsh[o] = rsh[i=(short)(r-s-1)];
                            isl    = rsl;
                            ilo    = rlo;
                        }
                        dsl[(o<<=1)+SL_OFF] = (short)lEnd;
                        dsl[ o     +SL_LEN] = len = isl[(i<<=1)+SL_LEN];
                        arraycopy(ilo, isl[i+SL_OFF], dlo, lEnd, len&=LEN_MASK);
                        lEnd += len;
                    }
                }
            }
            tail.rows++;
            assert tail.validate();
            return dst;
        }
    }

    public static final class Filter extends BatchFilter<CompressedBatch> {
        private final Merger projector;
        private final Filter before;
        public Filter(BatchType<CompressedBatch> batchType, Vars outVars,
                      Merger projector, RowFilter<CompressedBatch> rowFilter,
                      @Nullable BatchFilter<CompressedBatch> before) {
            super(batchType, outVars, rowFilter, before);
            assert projector == null || projector.vars.equals(outVars);
            this.projector = projector;
            this.before = (Filter)before;
        }


        @Override public CompressedBatch processInPlace(CompressedBatch b) {
            return filterInPlace(b);
        }
        @Override public @Nullable CompressedBatch onBatch(CompressedBatch batch) {
            if (batch == null) return null;
            onBatchPrologue(batch);
            return onBatchEpilogue(filterInPlace(batch));
        }

        @Override public CompressedBatch filterInPlace(CompressedBatch in) {
            if (before != null)
                in = before.filterInPlace(in);
            if (in == null || (in.rows*outColumns) == 0)
                return filterEmpty(in, in, in);
            var p = this.projector;
            if (p != null && rowFilter.targetsProjection()) {
                in = p.projectInPlace(in);
                p = null;
            }
            CompressedBatch b = in, prev = in;
            short cols = in.cols, rows;
            var decision = DROP;
            while (b != null) {
                rows = b.rows;
                int d = 0;
                SegmentRope[] shared = b.shared;
                short      [] slices = b.slices;
                decision = DROP;
                for (short r=0, start; r < rows && decision != TERMINATE; r++) {
                    start = r;
                    while (r < rows && (decision=rowFilter.drop(b, r)) == KEEP) ++r;
                    if (r > start) {
                        int n = (r-start)*cols, srcPos = start*cols;
                        arraycopy(shared, srcPos, shared, d, n);
                        arraycopy(slices, srcPos<<1, slices, d<<1, n<<1);
                        d += (short)n;
                    }
                }
                b.rows = (short)(d/cols);
                if (d == 0 && b != in)  // remove b from linked list
                    b = filterInPlaceSkipEmpty(b, prev);
                if (decision == TERMINATE) {
                    cancelUpstream();
                    if (b.next  != null) b.next = COMPRESSED.recycle(b.next);
                    if (in.rows ==    0) in     = COMPRESSED.recycle(in);
                }
                b = (prev = b).next;
            }
            in = filterInPlaceEpilogue(in, prev);
            if (p != null && in != null && in.rows > 0)
                in = p.projectInPlace(in);
            return in;
        }
    }

}
