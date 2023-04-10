package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.model.rope.RopeSupport;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.ByteVector;
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
import static java.util.Objects.requireNonNull;
import static jdk.incubator.vector.ByteVector.fromArray;
import static jdk.incubator.vector.VectorOperators.XOR;

public class CompressedBatch extends Batch<CompressedBatch> {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_SP_LEN = B_SP.length();
    private static final int B_SP_MASK = B_SP_LEN-1;

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

    /** Index of last row with values on locals */
    private int lastLocalsRow;
    /** Index of last column of last row with values on locals */
    private int lastLocalsCol;
    /** Number of {@code int}s per metadata row */
    private int mdRowInts;
    /** Where column where {@link Batch#offerTerm(Term)} will place the next term */
    private int offerCol;
    /** Whether hash() and equals() can be vectorized */
    private boolean vectorSafe;
    /** Whether the range {@code [bytesUsed(), locals.length)} is zero-filled. */
    private boolean localsTailClear;
    /** Number of {@code int}s per metadata column for a single row. */
    private static final int MD_COL_INTS = 3;
    private static final int MD_FID = 0;
    private static final int MD_OFF = 1;
    private static final int MD_LEN = 2;


    /* --- --- --- helpers --- --- --- */

    private int mdBase(int row, int col) {
        if (row < 0 || col < 0 || row >= rows || col >= cols) throw new IndexOutOfBoundsException();
        return row*mdRowInts + col* MD_COL_INTS;
    }

    private int rowLocalOff(int mdBase) {
        int len = 0, end = mdBase + mdRowInts;
        int lenIdx = mdBase + MD_LEN;
        while (lenIdx < end && (len = md[lenIdx]) == 0) lenIdx += MD_COL_INTS;
        return len == 0 ? 0 : md[lenIdx+(MD_OFF-MD_LEN)];
    }

    static int vecCeil(int value) {
        int floor = value &~B_SP_MASK;
        return floor == value ? floor : floor + B_SP_LEN;
    }
    private int rowLocalEnd(int mdBase) {
        int len = 0, i = mdBase + mdRowInts - (MD_COL_INTS - MD_LEN);
        while (i > mdBase && (len = md[i]) == 0) i -= MD_COL_INTS;
        return len + (len == 0 ? 0 : md[i+(MD_OFF-MD_LEN)]);
    }

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
    private boolean setTerm(boolean forbidGrow, int flaggedId, byte[] local,
                            int localOff, int localLen) {
        if (offerCol >= cols) throw new IllegalStateException();
        // find write location in md and grow if needed
        int mdBase = rows * mdRowInts + offerCol * MD_COL_INTS, dest = 0;
        if (localLen > 0) {
            if (lastLocalsRow < rows) {
                dest = bytesUsed(); // returns B_SP_LEN-aligned value
            } else {
                int lastBase = lastLocalsRow * mdRowInts + lastLocalsCol * MD_COL_INTS;
                dest = md[lastBase+MD_OFF] + md[lastBase+MD_LEN];
            }
            int end  = vecCeil(dest+localLen);
            if (end > locals.length) {
                if (forbidGrow) return false;
                if (!localsTailClear) {
                    Arrays.fill(locals, dest, locals.length, (byte)0);
                    localsTailClear = true;
                }
                locals = copyOf(locals, max(end, vecCeil(locals.length+(locals.length>>1))));

            }
            arraycopy(local, localOff, locals, dest, localLen);
            lastLocalsRow = rows;
            lastLocalsCol = offerCol;
        }
        md[mdBase+MD_FID] = flaggedId;
        md[mdBase+MD_OFF] = dest;
        md[mdBase+MD_LEN] = localLen;
        ++offerCol;
        return true;
    }

    /**
     * Erases the side effects of a rejected {@code beginOffer}/{@code offerTerm}/
     * {@code commitOffer} sequence.
     */
    private void rollbackOffer() {
        int b = lastLocalsRow * mdRowInts + lastLocalsCol * MD_COL_INTS;
        Arrays.fill(locals, findLastLocal(lastLocalsRow),
                md[b+ MD_OFF]+md[b+ MD_LEN], (byte)0);
        b = rows*mdRowInts;
        Arrays.fill(md, b, b+mdRowInts, 0);
        offerCol = Integer.MAX_VALUE;
    }

    /**
     * Sets {@code lastLocalRow} and {@code lastLocalCol} and returns the end index of the
     * local for such term in {@code locals}.
     *
     * @param lastLocalsRow start the search from this row. The search direction is
     *                      downwards (towards zero).
     * @return the end index (off+len) of the local bytes that term at {@code lastLocalRow},
     *         {@code lastLocalCol} stores at values or zero if this batch is deemed empty.
     */
    private int findLastLocal(int lastLocalsRow) {
        if (rows > 0 && lastLocalsRow > 0) {
            int cols = this.cols, r = lastLocalsRow;
            int mdi = r * mdRowInts + (cols-1)*MD_COL_INTS + MD_LEN;
            int mdGap = mdRowInts - cols*MD_COL_INTS;
            for (int c, len = 0; r >= 0; r--, mdi -= mdGap) {
                for (c = cols - 1; c >= 0 && (len = md[mdi]) == 0; mdi -= MD_COL_INTS)
                    --c;
                if (len > 0) {
                    this.lastLocalsRow = r;
                    this.lastLocalsCol = c;
                    return len + md[mdi + (MD_OFF-MD_LEN)];
                }
            }
        }
        this.lastLocalsRow = this.lastLocalsCol = 0;
        return 0;
    }

    /* --- --- --- lifecycle --- --- --- */

    CompressedBatch(int rowsCapacity, int cols, int bytesCapacity) {
        super(0, cols);
        this.locals = new byte[max(B_SP_LEN, bytesCapacity)];
        this.mdRowInts = MD_COL_INTS* max(1, cols);
        int mdCapacity = max(1, rowsCapacity)*mdRowInts;
        this.md = new int[mdCapacity];
        this.lastLocalsRow = 0;
        this.lastLocalsCol = 0;
        this.vectorSafe = true;
        this.localsTailClear = true;
        this.offerCol = Integer.MAX_VALUE;
    }

    private CompressedBatch(CompressedBatch o) {
        super(o.rows, o.cols);
        this.locals = copyOf(o.locals, o.locals.length);
        this.md = copyOf(o.md, o.md.length);
        this.lastLocalsRow = o.lastLocalsRow;
        this.lastLocalsCol = o.lastLocalsCol;
        this.mdRowInts = o.mdRowInts;
        this.vectorSafe = o.vectorSafe;
        this.localsTailClear = o.localsTailClear;
        this.offerCol = o.offerCol;
    }

    @Override public Batch<CompressedBatch> copy() { return new CompressedBatch(this); }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public int bytesUsed() {
        int base = lastLocalsRow*mdRowInts + lastLocalsCol* MD_COL_INTS;
        int unaligned = md[base+ MD_OFF] + md[base+ MD_LEN];
        return vecCeil(unaligned);
    }

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
            int ve = vecCeil(rowLocalEnd(mdb));
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
        return rowLocalEnd(base)-begin;
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
        for (int e = vecCeil(end); off < e; off += B_SP.length(), oOff += B_SP.length()) {
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
        int deficit = additionalRows - (md.length/mdRowInts - rows);
        if (deficit > 0)
            md = copyOf(md, md.length + max(deficit*mdRowInts, md.length>>1));
        if ((deficit = additionalBytes - (locals.length - bytesUsed())) > 0)
            locals = copyOf(locals, locals.length + max(deficit, locals.length>>1));
    }

    @Override public void clear() {
        rows            = 0;
        lastLocalsRow   = 0;
        lastLocalsCol   = 0;
        md[MD_OFF]      = 0;
        md[MD_LEN]      = 0;
        offerCol        = Integer.MAX_VALUE;
        vectorSafe      = true;
        localsTailClear = false;
    }

    @Override public void clear(int newColumns) {
        rows            = 0;
        lastLocalsRow   = 0;
        lastLocalsCol   = 0;
        offerCol        = Integer.MAX_VALUE;
        vectorSafe      = true;
        localsTailClear = false;
        cols            = newColumns;
        mdRowInts       = newColumns == 0 ? MD_COL_INTS : newColumns*MD_COL_INTS;
        int twice       = mdRowInts<<1;
        if (md.length < twice) {
            md = new int[twice];
        } else {
            md[MD_OFF]      = 0;
            md[MD_LEN]      = 0;
        }
    }

    @Override public boolean beginOffer() {
        if (rowsCapacity() <= rows) return false;
        offerCol = 0;
        return true;
    }

    @Override public boolean offerTerm(Term t) {
        int fId = t == null ? 0 : t.flaggedDictId;
        byte[] local = t == null ? ByteRope.EMPTY.utf8 : t.local;
        return setTerm(true, fId, local, 0, local.length);
    }

    @Override public boolean offerTerm(CompressedBatch other, int oRow, int oCol) {
        int oBase = other.mdBase(oRow, oCol);
        int[] omd = other.md;
        boolean ok = setTerm(true, omd[oBase], other.locals,
                             omd[oBase+MD_OFF], omd[oBase+MD_LEN]);
        if (!ok) rollbackOffer();
        return ok;
    }

    @Override public boolean commitOffer() {
        if (offerCol != cols) throw new IllegalStateException();
        int i = lastLocalsRow * mdRowInts + lastLocalsCol * MD_COL_INTS;
        i = md[i+MD_OFF] + md[i+MD_LEN];
        if (!localsTailClear)
            Arrays.fill(locals, i, vecCeil(i), (byte) 0);
        ++rows;
        offerCol = Integer.MAX_VALUE;
        return true;
    }

    @Override public boolean offerRow(CompressedBatch other, int row) {
        if (other.cols != cols) throw new IllegalArgumentException();
        int omdBase = other.mdBase(row, 0), mdDest = rows*mdRowInts;
        if (mdDest+mdRowInts > md.length) return false;

        int oOff = other.rowLocalOff(omdBase), alignedRowLen = vecCeil(other.rowLocalOff(omdBase))-oOff;
        int off = bytesUsed();
        if (off + alignedRowLen < locals.length) return false;

        arraycopy(other.md, omdBase, md, mdDest, cols*MD_COL_INTS);
        arraycopy(other.locals, oOff, locals, off, alignedRowLen);
        return true;
    }

    @Override public void beginPut() {
        offerCol = 0;
        if ((rows+1)*mdRowInts > md.length)
            md = copyOf(md, md.length + mdRowInts*(2|(rowsCapacity()>>1)));
    }

    @Override public void putTerm(Term t) {
        int fId = t == null ? 0 : t.flaggedDictId;
        byte[] local = t == null ? ByteRope.EMPTY.utf8 : t.local;
        setTerm(false, fId, local, 0, local.length);
    }

    @Override public void putTerm(CompressedBatch other, int row, int col) {
        int base = other.mdBase(row, col);
        int[] omd = other.md;
        setTerm(false, other.flaggedId(row, col), other.locals,
                omd[base+MD_OFF], omd[base+MD_LEN]);
    }

    @Override public void commitPut() { commitOffer(); }

    @Override public boolean offer(CompressedBatch o) {
        if (o.cols != cols) throw new IllegalArgumentException();
        int mdRowInts = this.mdRowInts, mdDest = rows * mdRowInts;
        if (mdDest + rows*mdRowInts > md.length) return false;

        int bytesDest = bytesUsed(), oBytes = o.bytesUsed();
        if (bytesDest+oBytes > locals.length) return false;

        int[] md = this.md, omd = o.md;
        int omdRowInts = o.mdRowInts, omdEnd = o.rows * omdRowInts;
        if (mdRowInts == omdRowInts) {
            arraycopy(omd, 0, md, mdDest, omdEnd);
        } else {
            int rowLen = cols*MD_COL_INTS;
            for (int in = 0, out = 0; in < omdEnd; in += omdRowInts, out += rowLen)
                arraycopy(omd, in, md, out, rowLen);
        }
        arraycopy(o.locals, bytesDest, locals, 0, oBytes);
        rows += o.rows;
        return true;
    }

    @Override public void put(CompressedBatch o) {
        int mdRowInts = this.mdRowInts, mdDest = rows*mdRowInts, oRows = o.rows;
        int localsLen = o.bytesUsed(), localsDest = bytesUsed();
        reserve(oRows, localsLen);

        int[] md = this.md, omd = o.md;
        int omdRowInts = o.mdRowInts, omdEnd = oRows*omdRowInts;
        if (mdRowInts == omdRowInts) {
            arraycopy(omd, 0, md, mdDest, omdEnd);
        } else {
            int rowLen = cols*MD_COL_INTS;
            for (int in = 0; in < omdEnd; in += omdRowInts, mdDest += mdRowInts)
                arraycopy(omd, in, md, mdDest, rowLen);
        }
        arraycopy(o.locals, 0, locals, localsDest, localsLen);
        rows += oRows;
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
            //update last (row,col) with local part
            b.findLastLocal(b.lastLocalsRow);
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
            // update rows, lastLocalsRow, lastLocalsCol and localsTailClear
            in.rows = rows = out/tmdRowInts;
            if (in.findLastLocal(rows-1) != oldBytesUsed)
                in.localsTailClear = false;
            return in;
        }
    }

}
