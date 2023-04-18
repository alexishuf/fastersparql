package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.Rope.UNTIL_DQ;
import static com.github.alexishuf.fastersparql.model.rope.RopeSupport.rangesEqual;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;
import static jdk.incubator.vector.ByteVector.fromArray;
import static jdk.incubator.vector.IntVector.SPECIES_256;
import static jdk.incubator.vector.IntVector.fromArray;
import static jdk.incubator.vector.VectorOperators.XOR;

public class CompressedBatch extends Batch<CompressedBatch> {
    /** Number of {@code int}s per metadata column for a single row. */
    private static final int MD_COL_INTS = 3;
    private static final int MD_FID = 0;
    private static final int MD_OFF = 1;
    private static final int MD_LEN = 2;

    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_SP_LEN = B_SP.length();
    private static final int B_SP_MASK = B_SP_LEN-1;
    private static final VectorSpecies<Integer> I_SP = IntVector.SPECIES_PREFERRED;
    private static final int I_SP_LEN = I_SP.length();
    private static final int PUT_SLACK = I_SP_LEN == 8 ? 24 : 0;
    private static final int I_SP_MASK = I_SP_LEN-1;

    /** Storage for local parts of terms. */
    private byte[] locals;

    /**
     * Array with {@code flaggedId}s, local offset into {@code locals} and local length
     * for each column of each row.
     *
     * <p>The following formulas allow finding the index in {@code md} for the each
     * "field" of  a particular column {@code c} of a row {@code r}:</p>>
     *
     * <ol>
     *     <li>{@code flaggedId(r, c) = md[r*mdRowInts + c*MD_COL_INTS + 0]}</li>
     *     <li>{@code localLen(r, c} = md[r*modRowInts + c*MD_COL_INTS + 1]</li>
     *     <li>{@code localOff(r, c} = md[r*modRowInts + c*MD_COL_INTS + 2]</li>
     * </ol>
     *
     * {@code localLen} and {@code localOff} overlap in the above list because when
     * {@code mdColInts == 2} localOff and localLen are stored as a single
     * {@code int word = localOff << 16 | localLen}.
     */
    private int[] md;

    /** Aligned index in {@code locals} where locals of a new row shall be placed. */
    private int bytesUsed;
    /** Number of {@code int}s per metadata row */
    private int mdRowInts;
    /** {@code -1} if not in a {@link #beginOffer()}/{@link #beginPut()}. Else this is the index
     * into {@code locals} where local bytes for the next column shall be written to. */
    private int offerNextLocals = -1;
    /** {@code col} in last {@code offerTerm}/{@code putTerm} call in the current
     *  {@link #beginOffer()}/{@link #beginPut()}, else {@code -1}. */
    private int offerLastCol = -1;
    /** Whether hash() and equals() can be vectorized */
    private boolean vectorSafe;
    /** Whether the range {@code [bytesUsed(), locals.length)} is zero-filled. */
    private boolean localsTailClear;


    /* --- --- --- helpers --- --- --- */

    boolean corrupted() {
        if (cols == 0) return false;
        if (rows < 0 || cols < 0)
            return true;
        int lastRowAlignedEnd = 0;
        for (int r = 0; r < rows; r++) {
            int rOff = rowLocalOff(mdBase(r, 0));
            int rEnd = rowLocalEnd(mdBase(r, 0));
            if (rEnd > rOff && rOff < lastRowAlignedEnd)
                return true; // put/offer broke alignment
            if ((bytesUsed(r) & B_SP_MASK) != 0)
                return true; // unaligned bytesUsed
            int rowAlignedEnd = lastRowAlignedEnd + bytesUsed(r);
            if (rEnd > rowAlignedEnd)
                return true; // bytesUsed() is too low
            if (rEnd > rOff) {
                for (int i = rEnd; i < rowAlignedEnd; i++)
                    if (locals[i] != 0)
                        return true; // non-zero right padding
            }
            for (int c = 0; c < cols; c++) {
                int off = md[r * mdRowInts + c * MD_COL_INTS + MD_OFF];
                int len = md[r * mdRowInts + c * MD_COL_INTS + MD_LEN];
                if (len > 0 && off < lastRowAlignedEnd)
                    return true; // offset in another row
                if (len > 0 && off >= rowAlignedEnd)
                    return true; // offset in next row
                try {get(r, c);} catch (Throwable t) {return true;}
                if (!equals(r, c, this, r, c))
                    return true;
            }
            if (!equals(r, this, r))
                return true;
            lastRowAlignedEnd = Math.max(lastRowAlignedEnd, rowAlignedEnd);
        }
        if (localsTailClear) {
            for (int i = lastRowAlignedEnd; i < locals.length; i++) 
                if (locals[i] != 0) return true;
        }
        return false;
    }

    private int mdBase(int row, int col) {
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        return row*mdRowInts + col* MD_COL_INTS;
    }

    private int rowLocalOff(int mdBase) {
        if (cols == 0) return 0;
        if (vectorSafe) {
            for (; mdBase >= 0; mdBase -= mdRowInts) {
                int i = mdBase+MD_LEN, end = mdBase+MD_COL_INTS*cols;
                for (; i < end; i+=MD_COL_INTS)
                    if (md[i] > 0) return md[i+(MD_OFF-MD_LEN)];
            }
        } else {
            for (; mdBase >= 0; mdBase -= mdRowInts) {
                int min = Integer.MAX_VALUE, i = mdBase+MD_LEN, end = mdBase+MD_COL_INTS*cols;
                for (; i < end; i += MD_COL_INTS)
                    if (md[i] > 0) min = Math.min(min, md[i+(MD_OFF-MD_LEN)]);
                if (min != Integer.MAX_VALUE) return min;
            }
        }
        return 0;
    }

    private int rowLocalEnd(int mdBase) {
        if (cols == 0) return 0;
        if (vectorSafe) {
            int i = mdBase+MD_COL_INTS*(cols-1)+MD_LEN;
            for (int len; i >= mdBase; i-=MD_COL_INTS)
                if ((len=md[i]) > 0) return len + md[i+(MD_OFF-MD_LEN)];
        } else {
            int max = 0, i = mdBase+MD_LEN, end = mdBase+MD_COL_INTS*cols;
            for (int len; i < end; i += MD_COL_INTS)
                if ((len=md[i]) > 0) max = Math.max(max, len+md[i+(MD_OFF-MD_LEN)]);
            if (max > 0) return max;
        }
        return rowLocalOff(mdBase);
    }

    /** Get a copy of {@code md} that is at least 50% bigger, can contain at least
     *  {@code additionalInts} more integers and is aligned to {@code I_SP_LEN}. */
    private static int[] mdGrow(int[] md, int additionalInts) {
        int ints = md.length + max(md.length >> 1, additionalInts); // grow at least 50%
        return Arrays.copyOf(md, mdCeil(ints));
    }

    /** Align ints to {@code I_SP_LEN}. */
    private static int mdCeil(int ints) {
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
     * @param flaggedId the {@link Term#flaggedDictId} to write
     * @param local the {@link Term#local} or something equivalent
     * @param localOff start of the term's local part in {@code local}
     * @param localLen length (in bytes) of the term local part.
     * @return true iff the term was written.
     */
    private boolean setTerm(boolean forbidGrow, int destCol, int flaggedId,
                            byte @Nullable [] local, @Nullable Rope localRope,
                            int localOff, int localLen) {
        if (offerNextLocals < 0) throw new IllegalStateException();
        if (destCol < 0 || destCol >= cols) throw new IndexOutOfBoundsException();
        // find write location in md and grow if needed
        int mdBase = rows*mdRowInts + destCol * MD_COL_INTS, dest = offerNextLocals;
        if (md[mdBase+MD_OFF] != 0)
            throw new IllegalStateException("Column already set");
        if (localLen > 0) {
            if (offerLastCol > destCol)
                vectorSafe = false; // out-of-order locals breaks vectorized row equals/hash
            offerLastCol = destCol;
            int required  = localsCeil(dest+localLen);
            if (required > locals.length) {
                if (forbidGrow) return false;
                locals = copyOf(locals, max(required, localsCeil(locals.length+(locals.length>>1))));
            }
            if (local != null)
                arraycopy(local, localOff, locals, dest, localLen);
            else if (localRope != null)
                localRope.copy(localOff, localOff+localLen, locals, dest);
            offerNextLocals = dest+localLen;
        }
        md[mdBase+MD_FID] = flaggedId;
        md[mdBase+MD_OFF] = dest;
        md[mdBase+MD_LEN] = localLen;
        return true;
    }

    /**
     * Erases the side effects of a rejected {@code beginOffer}/{@code offerTerm}/
     * {@code commitOffer} sequence.
     */
    private void rollbackOffer() {
        fill(locals, bytesUsed, offerNextLocals, (byte)0);
        offerLastCol = -1;
        offerNextLocals = -1;
    }

    /* --- --- --- lifecycle --- --- --- */

    CompressedBatch(int rowsCapacity, int cols, int bytesCapacity) {
        super(0, cols);
        this.locals = new byte[max(B_SP_LEN, bytesCapacity)];
        this.mdRowInts = MD_COL_INTS* max(1, cols);
        this.md = new int[mdCeil(max(1, rowsCapacity)*mdRowInts+PUT_SLACK)];
        this.vectorSafe = true;
        this.localsTailClear = true;
    }

    private CompressedBatch(CompressedBatch o) {
        super(o.rows, o.cols);
        this.bytesUsed = o.bytesUsed;
        this.locals = copyOf(o.locals, o.locals.length);
        this.md = copyOf(o.md, o.md.length);
        this.offerNextLocals = o.offerNextLocals;
        this.offerLastCol = o.offerLastCol;
        this.mdRowInts = o.mdRowInts;
        this.vectorSafe = o.vectorSafe;
        this.localsTailClear = o.localsTailClear;
    }

    @Override public Batch<CompressedBatch> copy() { return new CompressedBatch(this); }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public int bytesUsed() { return bytesUsed; }

    @Override public int rowsCapacity() {
        return md.length/mdRowInts;
    }

    @Override public boolean hasCapacity(int rowsCapacity, int bytesCapacity) {
        return md.length/mdRowInts >= rowsCapacity && locals.length >= bytesCapacity;
    }

    @Override public boolean hasMoreCapacity(CompressedBatch other) {
        if (locals.length <= other.locals.length) return false;
        return md.length/mdRowInts > other.md.length/other.mdRowInts;
    }

    /* --- --- --- row-level accessors --- --- --- */

    @Override public int hash(int row) {
        if (cols == 0)
            return 0;
        int h = 0;
        int mdb = mdBase(row, 0), mde = mdb + cols*MD_COL_INTS;
        for (int i = mdb; i < mde; i += MD_COL_INTS) h ^= md[i];
        if (vectorSafe) {
            // there is no gap between locals and (ve-i) % B_SP_LEN == 0
            int ve = localsCeil(rowLocalEnd(mdb));
            for (int i = rowLocalOff(mdb); i < ve; i += B_SP.length())
                h ^= fromArray(B_SP, locals, i).reinterpretAsInts().reduceLanes(XOR);
        } else {
            // due to possible gaps, we must simulate locals were sequentially laid out,
            // without gaps. to achieve this bit must be carried over accross local segments
            int bit = 0;
            for (int i = mdb+ MD_OFF; i < mde; i+= MD_COL_INTS) {
                for (int bb = md[i], be = bb+md[i+(MD_LEN-MD_OFF)]; bb < be; bit = (bit+8) & 31)
                    h ^= locals[bb++] << bit;
            }
        }
        return h;
    }

    @Override public int bytesUsed(int row) {
        if (cols == 0) return 0;
        int base = mdBase(row, 0), begin = rowLocalOff(base);
        return localsCeil(rowLocalEnd(base)-begin);
    }

    @Override public boolean equals(int row, CompressedBatch other, int oRow) {
        if (!vectorSafe || !other.vectorSafe) return super.equals(row, other, oRow);
        if (cols != other.cols) return false;
        if (cols == 0) return true;

        int base = mdBase(row, 0), oBase = other.mdBase(oRow, 0);
        int  off =       rowLocalOff(base),   end =       rowLocalEnd(base);
        int oOff = other.rowLocalOff(oBase), oEnd = other.rowLocalEnd(oBase);
        if (end-off != oEnd-oOff) return false;
        byte[] oLocals = other.locals;
        for (int e = localsCeil(end); off < e; off += B_SP.length(), oOff += B_SP.length()) {
            if (!fromArray(B_SP, locals, off).eq(fromArray(B_SP, oLocals, oOff)).allTrue())
                return false;
        }
        return true;
    }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public @Nullable Term get(@NonNegative int row, @NonNegative int col) {
        int base = mdBase(row, col), fId = md[base];
        return Term.make(fId, locals, md[base+MD_OFF], md[base+MD_LEN]);
    }

    @Override public int flaggedId(@NonNegative int row, @NonNegative int col) {
        return md[mdBase(row, col)];
    }

    @Override public int lexEnd(@NonNegative int row, @NonNegative int col) {
        int base = mdBase(row, col), fId = md[base], localLen = md[base+MD_LEN];
        if (fId > 0 || localLen == 0) return 0;
        if (fId < 0) return localLen;
        int off = md[base+MD_OFF];
        if (locals[off] != '"') return 0;
        return RopeSupport.reverseSkip(locals, off, off+localLen, UNTIL_DQ)-off;
    }

    @Override public int len(@NonNegative int row, @NonNegative int col) {
        int base = mdBase(row,  col), fId = md[base];
        return (fId == 0 ? 0 : RopeDict.get(fId&0x7fffffff).len) + md[base+MD_LEN];
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        return md[mdBase(row, col)+MD_LEN];
    }

    @Override public Term.@Nullable Type termType(int row, int col) {
        int base = mdBase(row, col);
        int fId = md[base];
        if      (fId                       <  0) return Term.Type.LIT;
        else if (fId                       >  0) return Term.Type.IRI;
        else if (md[base+MD_LEN] == 0) return null;
        return switch (locals[md[base+MD_OFF]]) {
            case '"'      -> Term.Type.LIT;
            case '_'      -> Term.Type.BLANK;
            case '<'      -> Term.Type.IRI;
            case '?', '$' -> Term.Type.VAR;
            default       -> throw new IllegalStateException();
        };
    }

    @Override public @NonNegative int asDatatypeId(int row, int col) {
        int base = mdBase(row, col), fId = md[base];
        if (fId < 0)
            return 0; // not an IRI
        int off = md[base+MD_OFF], len = md[base+MD_LEN];
        if (fId == RopeDict.P_XSD) {
            for (int i = 0; i < Term.FREQ_XSD_DT.length; i++) {
                byte[] candidate = Term.FREQ_XSD_DT[i].local;
                if (candidate.length == len && rangesEqual(locals, off, candidate, 0, len))
                    return Term.FREQ_XSD_DT_ID[i];
            }
        } else if (fId == RopeDict.P_RDF) {
            if (len == 5) { //HTML or JSON
                if (rangesEqual(locals, off, Term.RDF_HTML.local, 0, len))
                    return RopeDict.DT_langString;
                if (rangesEqual(locals, off, Term.RDF_JSON.local, 0, len))
                    return RopeDict.DT_XMLLiteral;
            } else if (len == 11) { // langString or XMLLiteral
                if (rangesEqual(locals, off, Term.RDF_LANGSTRING.local, 0, len))
                    return RopeDict.DT_langString;
                if (rangesEqual(locals, off, Term.RDF_XMLLITERAL.local, 0, len))
                    return RopeDict.DT_XMLLiteral;
            }
        } else if (len == 0) {
            return 0; //null Term
        }
        return requireNonNull(get(row, col)).asDatatypeId();
    }

    @Override public @Nullable Term datatypeTerm(int row, int col) {
        int base = mdBase(row, col);
        int fId = md[base];
        if (fId > 0) {
            return null; // IRI
        } else if (fId < 0) {
            var suffix = RopeDict.get(fId & 0x7fffffff);
            return Term.valueOf(suffix, 3/*"^^*/, suffix.len);
        }
        int off = md[base+MD_OFF], end = off+md[base+MD_LEN];
        if (end == off || locals[off] != '"')
            return null; // not a literal
        int i = RopeSupport.reverseSkip(locals, off, end, UNTIL_DQ);
        if      (i+1 == end) return Term.XSD_STRING;
        else if (locals[i+1] == '@') return Term.RDF_LANGSTRING;
        throw new InvalidTermException(this, i, "Unexpected literal suffix");
    }

    @Override
    public void writeSparql(ByteSink<?> dest, int row, int col, PrefixAssigner prefixAssigner) {
        int base = mdBase(row, col);
        Term.toSparql(dest, prefixAssigner, md[base], locals, md[base+MD_OFF],
                      md[base+MD_LEN]);
    }

    @Override public void writeNT(ByteSink<?> dest, int row, int col) {
        int base = mdBase(row, col), fId = md[base];
        if (fId > 0) dest.append(RopeDict.get(fId));
        dest.append(locals, md[base+MD_OFF], md[base+MD_LEN]);
        if (fId < 0) dest.append(RopeDict.get(fId&0x7fffffff));
    }

    @Override public void write(ByteSink<?> dest, int row, int col, int begin, int end) {
        if (begin < 0 || end < 0) throw new IndexOutOfBoundsException();
        int base = mdBase(row, col), fId = md[base];
        if (fId > 0) {
            ByteRope prefix = RopeDict.get(fId);
            int len = prefix.len;
            if (begin < len) {
                dest.append(prefix, begin, Math.min(end, len));
                begin = 0;
            } else {
                begin -= len;
            }
            end -= len;
            if (begin < end) {
                if (end > md[base+MD_LEN]) throw new IndexOutOfBoundsException(end+len);
                dest.append(locals, md[base+MD_OFF], end-begin);
            }
        } else {
            int len = md[base+MD_LEN];
            if (begin < len) {
                int off = md[base + MD_OFF];
                dest.append(locals, off+begin, Math.min(len, end)-begin);
                begin = 0;
            } else { begin -= len; }
            end -= len;
            if (begin < end) {
                if (fId == 0) throw new IndexOutOfBoundsException();
                ByteRope suffix = RopeDict.get(fId & 0x7fffffff);
                if (end > suffix.len) throw new IndexOutOfBoundsException();
                dest.append(suffix, begin, end);
            }
        }
    }

    @Override public int hash(int row, int col) {
        int b = mdBase(row, col);
        int h = md[b];
        if (Term.isNumericDatatype(h))
            return super.hash(row, col);
        return 31*h + RopeSupport.hash(locals, md[b+MD_OFF], md[b+MD_LEN]);
    }

    @Override public boolean equals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        int base = mdBase(row, col), fId = md[base], len = md[base+MD_LEN];
        if (other == null)
            return fId == 0 && len == 0;
        if (Term.isNumericDatatype(fId))
            return other.asNumber() != null && other.equals(get(row, col));
        else if (other.asNumber() != null || fId != other.flaggedDictId)
            return false;
        //same fId, compare ranges
        int off = md[base+MD_OFF];
        byte[] oLocal = other.local;
        return len == oLocal.length && rangesEqual(locals, off, oLocal, 0, oLocal.length);
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col,
                          CompressedBatch other, int oRow, int oCol) {
        int base = mdBase(row, col), oBase = other.mdBase(oRow, oCol);
        int fId = md[base], oFId = other.md[oBase];
        if (Term.isNumericDatatype(fId))
            return Term.isNumericDatatype(oFId) && super.equals(row, col, other, oRow, oCol);
        if (Term.isNumericDatatype(oFId) || fId != oFId)
            return false;

        int len = md[base+MD_LEN];
        if (len != other.md[oBase+MD_LEN]) return false; //len mismatch
        return rangesEqual(locals, md[base+MD_OFF],
                           other.locals, other.md[oBase+MD_OFF], len);
    }

    /* --- --- --- mutators --- --- --- */

    @Override public void reserve(int additionalRows, int additionalBytes) {
        if ((rows+additionalRows)*mdRowInts > md.length)
            md = mdGrow(md, additionalRows*mdRowInts + PUT_SLACK);
        int required = bytesUsed()+additionalBytes;
        if (required > locals.length)
            locals = copyOf(locals, localsCeil(required));
    }

    @Override public void clear() {
        rows            = 0;
        bytesUsed       = 0;
        offerNextLocals = -1;
        offerLastCol    = -1;
        md[MD_OFF]      = 0;
        md[MD_LEN]      = 0;
        vectorSafe      = true;
        localsTailClear = false;
    }

    @Override public void clear(int newColumns) {
        clear();
        cols            = newColumns;
        mdRowInts       = newColumns == 0 ? MD_COL_INTS : newColumns*MD_COL_INTS;
        int required = mdCeil(newColumns<<2);
        if (md.length < required) {
            md = new int[required];
        } else { // set off, len to make bytesUsed() return 0
            md[MD_OFF]      = 0;
            md[MD_LEN]      = 0;
        }
    }

    @Override public boolean beginOffer() {
        if (rowsCapacity() <= rows) return false;
        beginPut();
        return true;
    }

    @Override public boolean offerTerm(int col, Term t) {
        int fId = t == null ? 0 : t.flaggedDictId;
        byte[] local = t == null ? ByteRope.EMPTY.utf8 : t.local;
        return setTerm(true, col, fId, local, null, 0, local.length);
    }

    @Override public boolean offerTerm(int destCol, CompressedBatch other, int oRow, int oCol) {
        int oBase = other.mdBase(oRow, oCol);
        int[] omd = other.md;
        boolean ok = setTerm(true, destCol, omd[oBase], other.locals, null,
                             omd[oBase+MD_OFF], omd[oBase+MD_LEN]);
        if (!ok) rollbackOffer();
        return ok;
    }

    @Override public boolean offerTerm(int col, TermParser parser) {
        return setTerm(true, col, parser.flaggedId(), null, parser.localBuf(),
                       parser.localBegin(), parser.localEnd-parser.localBegin);
    }

    @Override
    public boolean offerTerm(int col, int flaggedId, Rope localRope, int localOff, int localLen) {
        return setTerm(true, col, flaggedId, null, localRope, localOff, localLen);
    }

    @Override public boolean commitOffer() {
        if (offerNextLocals < 0) throw new IllegalStateException();
        ++rows;
        bytesUsed = localsCeil(offerNextLocals);
        offerNextLocals = -1;
        offerLastCol = -1;
        assert !corrupted() : "corrupted";
        return true;
    }

    @Override public boolean offerRow(CompressedBatch other, int row) {
        if (mdCeil((rows+1)*mdRowInts)       > md.length    ) return false;
        if (other.bytesUsed(row)+bytesUsed() > locals.length) return false;
        putRow(other, row);
        return true;
    }

    @Override public boolean offer(CompressedBatch o) {
        if (rows+o.rows*mdRowInts     > md.length    ) return false;
        if (bytesUsed()+o.bytesUsed() > locals.length) return false;
        put(o);
        return true;
    }

    @Override public void beginPut() {
        offerLastCol = -1;
        offerNextLocals = bytesUsed;
        int base = rows * mdRowInts, required = base + mdRowInts;
        if (required > md.length)  // grow capacity by 50% + 2 so that md fits at least 2 rows
            md = mdGrow(md, mdRowInts);
        fill(md, base, required, 0);
        if (!localsTailClear) {
            Arrays.fill(locals, base, locals.length, (byte)0);
            localsTailClear = true;
        }
    }

    @Override public void putTerm(int col, Term t) {
        int fId = t == null ? 0 : t.flaggedDictId;
        byte[] local = t == null ? ByteRope.EMPTY.utf8 : t.local;
        setTerm(false, col, fId, local, null, 0, local.length);
    }

    @Override public void putTerm(int destCol, CompressedBatch other, int row, int col) {
        int base = other.mdBase(row, col);
        int[] omd = other.md;
        setTerm(false, destCol, other.flaggedId(row, col), other.locals, null,
                omd[base+MD_OFF], omd[base+MD_LEN]);
    }

    @Override public void putTerm(int col, TermParser parser) {
        setTerm(false, col, parser.flaggedId(), null, parser.localBuf(),
                parser.localBegin(), parser.localEnd-parser.localBegin);
    }

    @Override
    public void putTerm(int col, int flaggedId, Rope localRope, int localOff, int localEnd) {
        setTerm(false, col, flaggedId, null, localRope, localOff, localEnd);
    }

    @Override public void commitPut() { commitOffer(); }


    // FOLFOLFOLFOLFOLFOLFOLFOL
    // _1__1__1                 // MASK0
    //         __1__1__         // MASK1
    //                 1__1__1_ // MASK2
    private static final VectorMask<Integer> PUT_MASK0 =
            VectorMask.fromValues(SPECIES_256, false, true, false, false, true, false, false, true);
    private static final VectorMask<Integer> PUT_MASK1 =
            VectorMask.fromValues(SPECIES_256, false, false, true, false, false, true, false, false);
    private static final VectorMask<Integer> PUT_MASK2 =
            VectorMask.fromValues(SPECIES_256, true, false, false, true, false, false, true, false);

    public void put(CompressedBatch o) {
        // handle special cases
        int oRows = o.rows;
        if (mdRowInts != o.mdRowInts || o.cols != cols || oRows == 0 || I_SP_LEN != 8) { scalarPut(o); return; }

        int dst = mdRowInts*rows, mdLen = oRows*mdRowInts;
        int lDst = bytesUsed, lLen = o.bytesUsed;

        int required = mdCeil(dst + mdLen + 24);
        if (required > md.length)
            md = mdGrow(md, required);
        if ((required=localsCeil(bytesUsed+lLen)) > locals.length) {
            localsTailClear = true;
            locals = copyOf(locals, required);
        }

        // vectorized copy of md, shifting MD_OFF entries by lDst
        // Since MD_COL_INTS=3, the lanes containing offsets change for every vector, this change
        // is cyclical, thus there are only 3 possible lane assignments:
        // _1__1__1 (MASK0), __1__1__ (MASK1) and 1__1__1_ (MASK2)
        int[] md = this.md, omd = o.md;
        int src = 0;
        if (mdLen >= 24) {
            IntVector delta0 = IntVector.zero(I_SP).blend(lDst, PUT_MASK0);
            IntVector delta1 = IntVector.zero(I_SP).blend(lDst, PUT_MASK1);
            IntVector delta2 = IntVector.zero(I_SP).blend(lDst, PUT_MASK2);
            // md has a slack of 24 ints, but omd may not have this slack
            for (; src < mdLen &&  src+24 < omd.length; src += 24, dst += 24) {
                fromArray(I_SP, omd, src   ).add(delta0).intoArray(md, dst);
                fromArray(I_SP, omd, src+ 8).add(delta1).intoArray(md, dst+ 8);
                fromArray(I_SP, omd, src+16).add(delta2).intoArray(md, dst+16);
            }
        }
        // copy/offset leftovers. arraycopy()+for is faster than a fused copy/offset loop
        if ((mdLen -=src) > 0) {
            arraycopy(omd, src, md, dst, mdLen);
            for (int i = dst + MD_OFF, e = dst + mdLen; i < e; i += 3)
                md[i] += lDst;
        }

        //copy locals. arraycopy() is faster than vectorized copy
        arraycopy(o.locals, 0, locals, lDst, lLen);

        // update batch-level data
        vectorSafe &= o.vectorSafe; // copied any out-of-order locals and gaps from o.locals
        bytesUsed += lLen;
        rows += oRows;
        assert !corrupted() : "corrupted";
    }

    private void scalarPut(CompressedBatch o) {
        if (o.cols != cols)   throw new IllegalArgumentException("cols != o.cols");
        int localsLen = o.bytesUsed(), localsDest = bytesUsed();
        reserve(o.rows, localsLen);

        //copy md, offsetting MD_OFF by localsDest
        int[] md = this.md, omd = o.md;
        int rInts = this.mdRowInts, orInts = o.mdRowInts;
        int src = 0, dst = rows*rInts, eob = o.rows*orInts, width = cols*MD_COL_INTS;
        for (; src < eob; src += orInts-width, dst += rInts-width) {
            for (int eoc = src+width; src < eoc; src += MD_COL_INTS, dst+=MD_COL_INTS) {
                md[dst+MD_FID] = omd[src+MD_FID];
                md[dst+MD_OFF] = omd[src+MD_OFF] + localsDest;
                md[dst+MD_LEN] = omd[src+MD_LEN];
            }
        }
        // copy locals
        arraycopy(o.locals, 0, locals, localsDest, localsLen);

        //update batch-level metadata
        vectorSafe &= o.vectorSafe; // out-of-order columns and gaps were copied
        rows += o.rows;
        assert !corrupted() : "corrupted";
    }

    @Override public void putRow(CompressedBatch o, int row) {
        if (!o.vectorSafe || row >= o.rows || cols != o.cols) { super.putRow(o, row); return; }
        int base = rows * mdRowInts, oBase = row*o.mdRowInts;
        int lLen = o.bytesUsed(row), lSrc = o.rowLocalOff(oBase), lDst = bytesUsed;
        reserve(1, lLen);
        int[] md = this.md, omd = o.md;
        for (int e = base+cols*MD_COL_INTS; base < e; base+=MD_COL_INTS, oBase+=MD_COL_INTS) {
            md[base+MD_FID] = omd[oBase+MD_FID];
            md[base+MD_OFF] = omd[oBase+MD_OFF] - lSrc + lDst;
            md[base+MD_LEN] = omd[oBase+MD_LEN];
        }
        arraycopy(o.locals, lSrc, locals, lDst, lLen);
        if (lDst+lLen == locals.length && !localsTailClear)
            localsTailClear = true;
        bytesUsed += lLen;
        ++rows;
        assert !corrupted() : "corrupted";
    }

    /* --- --- --- operation objects --- --- --- */

    private static final VarHandle REC_TMD;
    @SuppressWarnings("unused") // access through REC_TMD
    private static int[] recTMd;
    static {
        try {
            REC_TMD = lookup().findStaticVarHandle(CompressedBatch.class,"recTMd",
                                                   int[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Get a possibly recycled {@code int[]} with {@code length >= required}.
     * @param required required size for the {@code int[]}
     * @return a {@code int[]} that may not be zero-filled.
     */
    private static int[] getTMd(int required) {
        required = mdCeil(required);
        int[] a = (int[]) REC_TMD.getAcquire();
        if (a != null) {
            if (a.length < required || !REC_TMD.compareAndSet(a, null))
                a = null;
        }
        return a != null ? a : new int[required];
    }

    /**
     * If the recycled and not yet taken {@code int[]} is null or smaller than {@code tmd},
     * atomically replaces it with {@code tmd}. The caller of this method ALWAYS looses
     * ownership of {@code tmd}, even if it was not stored.
     *
     * @param tmd a {@code int[] to recycle}
     */
    private static void recycleTMd(int[] tmd) {
        int[] a = (int[]) REC_TMD.getAcquire();
        if (a == null || tmd.length > a.length)
            REC_TMD.setRelease(tmd);
    }

    static final class Merger extends BatchMerger<CompressedBatch> {
        private int @Nullable [] tmd;

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
            int rows = b.rows, bMdWidth = b.mdRowInts, tMdRequired = rows*columns.length*3;

            //project/compact md
            int[] bmd = b.md, tmd = this.tmd, otmd = null;
            if (tmd == null || tmd.length < tMdRequired) {
                otmd = tmd;
                tmd = getTMd(tMdRequired);
            }
            for (int r = 0, out = 0, base = 0; r < rows; r++, base += bMdWidth) {
                for (int src : columns) {
                    if (src < 0) {
                        tmd[out  ] = 0;
                        tmd[out+1] = 0;
                        tmd[out+2] = 0;
                    } else {
                        src = base + 3*src;
                        tmd[out  ] = bmd[src  ];
                        tmd[out+1] = bmd[src+1];
                        tmd[out+2] = bmd[src+2];
                    }
                    out += 3;
                }
            }

            // replace md, recycle bmd and omd
            b.md = tmd;
            this.tmd = bmd;
            if (otmd != null) recycleTMd(otmd);
            b.cols = columns.length;
            // md now is packed (before b.mdRowInts could be >cols)
            b.mdRowInts = 3* columns.length;
            // any projection breaks hash/equals vector-safety as there is unexpected data
            // between or after local strings.
            b.vectorSafe = false;
            return b;
        }
    }

    static final class Filter extends BatchFilter<CompressedBatch> {
        private int @Nullable [] tmd;

        public Filter(BatchType<CompressedBatch> batchType, BatchMerger<CompressedBatch> projector,
                      RowFilter<CompressedBatch> rowFilter) {
            super(batchType, projector, rowFilter);
        }

        private CompressedBatch filterInPlaceEmpty(CompressedBatch in, int cols) {
            int rows = in.rows, survivors = 0;
            for (int r = 0; r < rows; r++) {
                if (!rowFilter.drop(in, r)) survivors++;
            }
            in.clear(cols);
            in.rows = survivors;
            return in;
        }

        @Override public CompressedBatch filterInPlace(CompressedBatch in,
                                                       BatchMerger<CompressedBatch> projector) {
            int @Nullable[] columns = projector == null ? null : projector.columns;
            int rows = in.rows, cols = columns == null ? in.cols : columns.length;
            if (cols == 0 || rows == 0)
                return filterInPlaceEmpty(in, cols);
            int tmdRowInts = MD_COL_INTS * cols, imdRowInts = in.mdRowInts;
            int tmdRequired = rows * tmdRowInts, out = 0;
            int[] otmd = null, tmd = this.tmd, imd = in.md;
            if (tmd == null || tmd.length < tmdRequired) {
                otmd = tmd;
                tmd = getTMd(tmdRequired);
            }

            if (columns != null && rowFilter.targetsProjection()) {
                in = projector.projectInPlace(in);
                columns = null;
            }
            if (columns == null) {
                for (int r = 0; r < rows; r++) {
                    if (rowFilter.drop(in, r)) continue;
                    arraycopy(imd, r*imdRowInts, tmd, out, tmdRowInts);
                    out += tmdRowInts;
                }
            } else {
                for (int r = 0; r < rows; r++) {
                    if (rowFilter.drop(in, r)) continue;
                    int base = r*imdRowInts;
                    for (int src : columns) {
                        if (src < 0) {
                            tmd[out  ] = 0;
                            tmd[out+1] = 0;
                            tmd[out+2] = 0;
                        } else {
                            int colBase = base + src * MD_COL_INTS;
                            tmd[out  ] = imd[colBase  ];
                            tmd[out+1] = imd[colBase+1];
                            tmd[out+2] = imd[colBase+2];
                        }
                        out += 3;
                    }
                }
            }

            int oldBytesUsed =  in.bytesUsed();
            in.md = tmd;      // replace metadata
            this.tmd = imd;   // use original imd on next call
            if (otmd != null) // offer our old tmd to other instances
                REC_TMD.weakCompareAndSetRelease(null, otmd);
            in.mdRowInts = tmdRowInts;
            if (columns != null) {
                in.vectorSafe = false;
                in.cols = columns.length;
            }
            // update rows, bytesUsed and localsTailClear
            in.rows = rows = out/tmdRowInts;
            for (int base = (rows-1)*tmdRowInts; base >= 0; base -= tmdRowInts) {
                int b = in.rowLocalOff(base), e = in.rowLocalEnd(base);
                if (e > b) {
                    in.bytesUsed = localsCeil(e);
                    break;
                }
            }
            if (in.bytesUsed != oldBytesUsed)
                in.localsTailClear = false;
            return in;
        }
    }

}
