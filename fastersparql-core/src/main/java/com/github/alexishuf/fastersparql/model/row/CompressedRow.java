package com.github.alexishuf.fastersparql.model.row;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.RopeSupport.rangesEqual;
import static java.lang.System.arraycopy;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.IntVector.fromMemorySegment;
import static jdk.incubator.vector.VectorOperators.XOR;

/**
 * Represents rows as a plain byte[] with a custom semi-compressed binary format.
 * Users MUST use {@link CompressedRow#get(byte[], int)} to fetch a term of the row.
 *
 * <p>A row ({@code ROW} with N terms is encoded in the following grammar:</p>
 *
 * <pre>
 *     row(COLS) = int32_le(HASH) int16_le(COLS) offsets(COLS) int32_le(END) ids(COLS) strings(COLS)
 *     offsets(COLS) = int32_le(OFFSET[0]) ... int32_le(OFFSET[COLS-1])
 *     ids(COLS) = int32_le(FLAGGED_ID[0]) ... int32_le(FLAGGED_ID[COLS-1])
 *     strings(COLS) = utf8(STRING[0]) ... utf8(STRING[COLS-1])
 * </pre>
 *
 * <p>Where:</p>
 *
 * <ul>
 *     <li>{@code HASH} is computed by {@link CompressedRow#hash(byte[])}.</li>
 *     <li>{@code COLUMNS} is the number of terms in this row</li>
 *     <li>{@code FLAGGED_ID[i]} is {@link Term#flaggedDictId} for the i-th term</li>
 *     <li>{@code STRING[i]} is {@link Term#local} for the i-th term.</li>
 *     <li>{@code END} is the whole row bytes length or the idnex of the first byte after the
 *         last string ({@code STRING[COLS-1]})</li>
 * </ul>
 */
public class CompressedRow extends RowType<byte[]> {
    public static final byte[] EMPTY = {
            /* 0 */ 8, 0, 0, 0, //hash
            /* 4 */ 0, 0, //cols
            /* 6 */ 8, 0 // offset[*]
    };
    private static final VectorSpecies<Integer> I_SP = IntVector.SPECIES_PREFERRED;
    private static final int HASH_BEGIN          =             0;
    private static final int COLS_BEGIN          =  HASH_BEGIN+4;
    private static final int OFFSETS_BEGIN       = COLS_BEGIN+2;
    private static final int OFFSET_BYTES        = 2;
    private static final int ID_BYTES            = 4;

    public static final CompressedRow INSTANCE = new CompressedRow();

    protected CompressedRow() { super(byte[].class, Term.class); }

    /* --- --- --- RowType methods --- --- --- */

    @Override public Builder builder(int columns) { return new Builder(columns); }

    @Override public byte[] empty() { return EMPTY; }

    @Override public int columns(byte[] row) {
        return row == null ? 0 : (row[COLS_BEGIN]&0xff) | ((row[COLS_BEGIN+1]&0xff) << 8);
    }

    @Override public @Nullable Term get(byte @Nullable [] row, int column) {
        if (row == null) return null;
        int n = columns(row);
        if (column < 0 || column >= n) throw new IndexOutOfBoundsException(column);
        int off = readOffset(row, column);
        int len = readOffset(row, column + 1) - off;
        int id = readId(row, n, column);
        return id == 0 && len == 0 ? null : Term.make(id, row, off, len);
    }

    @Override
    public void writeSparql(ByteRope dest, byte[] row, int column, PrefixAssigner prefixAssigner) {
        int nCols = columns(row);
        if (column < 0 || column >= nCols) throw new IndexOutOfBoundsException(column);
        int offset = readOffset(row, column);
        Term.toSparql(dest, prefixAssigner, readId(row, nCols, column),
                      row, offset, readOffset(row, column+1)-offset);
    }

    @Override public Merger projector(Vars out, Vars in) {
        return new Merger(mergerSources(out, in, Vars.EMPTY), out);
    }

    @Override public Merger merger(Vars out, Vars leftVars, Vars rightVars) {
        return new Merger(mergerSources(out, leftVars, rightVars), out);
    }

    @Override public boolean equalsSameVars(byte[] l, byte[] r) {
        if (l == r) return true;
        if (l == null || r == null) return false;

        int lCols = columns(l), rCols = columns(r);
        if (lCols != rCols)
            return false;
        int lEnd = readOffset(l, lCols), rEnd = readOffset(r, rCols);
        return lEnd == rEnd
                && rangesEqual(l, OFFSETS_BEGIN, r, OFFSETS_BEGIN, lEnd-OFFSETS_BEGIN);
    }

    @Override public int hash(byte[] r) {
        if (r == null) return 0;
        int h =  (r[HASH_BEGIN  ]&0xff)        | ((r[HASH_BEGIN+1]&0xff) <<  8)
                 | ((r[HASH_BEGIN+2]&0xff) << 16) | ((r[HASH_BEGIN+3]&0xff) << 24);
        if (h == 0) {
            int cols = columns(r), i = 0, len = readOffset(r, cols), base = idsBegin(cols);
            MemorySegment ms = MemorySegment.ofArray(r);
            for (int e = I_SP.loopBound((len-base)>>2); i < e ; i += I_SP.length())
                h ^= fromMemorySegment(I_SP, ms, base+4L*i, LITTLE_ENDIAN).reduceLanes(XOR);
            if (i == 0) {
                h = (cols << 8) | len;
                for (int bit = 0, b = base+ID_BYTES*cols; b < len;  ++b, bit = (bit + 8)&31)
                    h ^= r[b] << bit;
            }
            r[HASH_BEGIN  ] = (byte)  h       ;
            r[HASH_BEGIN+1] = (byte) (h >>  8);
            r[HASH_BEGIN+2] = (byte) (h >> 16);
            r[HASH_BEGIN+3] = (byte) (h >> 24);
        }
        return h;
    }

    @Override public String toString() { return "COMPRESSED"; }

    /* --- --- --- Inner classes --- --- --- */

    public static class Builder implements RowType.Builder<byte[]> {
        private final Object[] data;

        public Builder(int columns) { data = new Object[2*columns]; }

        @Override public boolean isEmpty() {
            int i = 0;
            while (i < data.length && data[i] == null) i += 2;
            return i >= data.length;
        }

        @Override public RowType.@This Builder<byte[]> set(int column, @Nullable Term term) {
            data[column] = term;
            return this;
        }

        @Override public RowType.@This Builder<byte[]> set(int column, byte[] inRow, int inCol) {
            data[column] = inRow;
            data[column+(data.length>>1)] = inCol;
            return this;
        }

        @Override public byte[] build() {
            if (data.length == 0)
                return EMPTY;
            int n = data.length >> 1, idsBegin = idsBegin(n), offset = idsBegin+ID_BYTES*n;
            for (int i = 0; i < n; i++) {
                var o = data[i];
                if (o instanceof Term t) {
                    offset += t.local.length;
                } else if (o instanceof byte[] row) {
                    int col = (int) data[n+i];
                    offset += readOffset(row, col+1)-readOffset(row, col);
                }
            }
            if (offset >= 1<<(OFFSET_BYTES<<3))
                throw new IllegalArgumentException("Offset overflow");
            byte[] row = new byte[offset];
            writeColumns(row, n);
            writeOffset(row, n, offset);
            offset = idsBegin+4*n;
            for (int i = 0, fId, written; i < n; i++) {
                Object o = data[i];
                if (o instanceof Term t) {
                    byte[] local = t.local;
                    arraycopy(local, 0, row, offset, written = local.length);
                    fId = t.flaggedDictId;
                } else if (o instanceof byte[] inRow) {
                    int inCol = (int)data[n+i], begin = readOffset(inRow, inCol);
                    written = readOffset(inRow, inCol+1)-begin;
                    arraycopy(inRow, begin, row, offset, written);
                    fId = readId(inRow, COMPRESSED.columns(inRow), inCol);
                } else {
                    written = 0;
                    fId = 0;
                }
                row[idsBegin++] = (byte) fId;
                row[idsBegin++] = (byte) (fId >>>  8);
                row[idsBegin++] = (byte) (fId >>> 16);
                row[idsBegin++] = (byte) (fId >>> 24);
                writeOffset(row, i, offset);
                offset += written;
            }
            assert INSTANCE.isValid(row);
            return row;
        }
    }

    public final class Merger extends RowType<byte[]>.Merger {
        private byte [] tmp = ByteRope.EMPTY.utf8;
        private final boolean projection, projShuffles;
        private @Nullable Builder builder;

        private Merger(int @Nullable [] sources, Vars outVars) {
            super(outVars, sources);
            if (sources == null) {
                this.projection = true;
                this.projShuffles = false;
            } else {
                if (sources.length >= 1<<16)
                    throw new IllegalArgumentException("Too many columns");
                boolean projection = true, shuffles = false;
                for (int i = 0, max = 0; projection &&  i < sources.length; i++) {
                    int s = sources[i];
                    if (s <= 0) {
                        projection = s == 0;
                    } else if (!shuffles) {
                        if (s <= max) shuffles = true;
                        max = s;
                    }
                }
                this.projection   = projection;
                this.projShuffles = shuffles;
            }
        }

        @Override public String toString() { return "Merger"+outVars+Arrays.toString(sources); }

        @Override public byte[] projectInPlace(byte[] row) {
            if (sources == null || row == null)
                return row;
            if (sources.length == 0)
                return EMPTY;
            if (!projection)
                throw new UnsupportedOperationException("Not a projection Merger");
            int cols = columns(row);
            if (sources.length > cols || projShuffles)
                return merge(row, null); //in-place merge is slow or impossible
            assert isValid(row);

            // save all metadata into tmp
            int ml = stringsBegin(cols);
            arraycopy(row, 0, (tmp.length<ml ? tmp=new byte[ml] : tmp), 0, ml);

            // init row before offsets
            row[HASH_BEGIN+3] = row[HASH_BEGIN+2] = row[HASH_BEGIN+1] = row[HASH_BEGIN] = 0;
            writeColumns(row, sources.length);

            // index of where we will write the next id
            int idsBegin = idsBegin(sources.length), offset = idsBegin+ID_BYTES*sources.length;
            // idsBegin for tmp, but will not move during iteration
            int tmpIdsBegin = idsBegin(cols);
            // write each string
            for (int i = 0; i < sources.length; i++) {
                int s = sources[i];
                writeOffset(row, i, offset);
                if (s == 0) {
                    row[idsBegin++] = row[idsBegin++] = row[idsBegin++] = row[idsBegin++] = 0;
                } else {
                    int sOffset = tmpIdsBegin+ID_BYTES*--s; // where to read the id from
                    row[idsBegin++] = tmp[sOffset++];
                    row[idsBegin++] = tmp[sOffset++];
                    row[idsBegin++] = tmp[sOffset++];
                    row[idsBegin++] = tmp[sOffset  ];
                    // copy the string to offset. Since !projShuffles, sOffset >= offset (i.e.,
                    // the source has not been overwritten).
                    int len = readOffset(tmp, s+1) - (sOffset = readOffset(tmp, s));
                    if (sOffset != offset)
                        arraycopy(row, sOffset, row, offset, len);
                    offset += len;
                }
            }
            writeOffset(row, sources.length, offset); // store end offset
            assert isValid(row);
            return row;
        }

        @Override public byte[] merge(byte @Nullable [] left, byte @Nullable [] right) {
            if (sources == null)
                return left;
            if (sources.length == 0)
                return EMPTY;
            if (builder == null)
                builder = new Builder(sources.length);
            assert isValid(left, right);
            for (int i = 0; i < sources.length; i++) {
                int src = sources[i];
                if (src == 0) continue;
                byte[] side;
                if (src < 0) { side = right; src = -src-1; } else { side = left; src =  src-1; }
                builder.set(i, side, src);
            }
            return builder.build();
//
//            int idsBegin = idsBegin(sources.length), out = idsBegin+ID_BYTES*sources.length;
//            int lCols = columns(left), rCols = columns(right);
//            for (int s : sources) {
//                if (s > 0 && left != null)
//                    out += readOffset( left,  s)-readOffset( left, s-1);
//                else if (s < 0 && right != null)
//                    out += readOffset(right, -s)-readOffset(right, -s-1);
//            }
//            if (out >= 1<<(OFFSET_BYTES<<3))
//                throw new IllegalArgumentException("Offset overflow");
//            byte[] row = new byte[out], side;
//            writeColumns(row, sources.length);
//            writeOffset(row, sources.length, out);
//            out = idsBegin+ID_BYTES*sources.length;
//            for (int i = 0, sideCols; i < sources.length; i++) {
//                int s = sources[i];
//                if (s < 0) { sideCols = rCols; side = right;               s = -s - 1;}
//                else       { sideCols = lCols; side = s > 0 ? left : null; s =  s - 1;}
//                writeOffset(row, i, out);
//                if (side == null) {
//                    row[idsBegin++] = row[idsBegin++] = row[idsBegin++] = row[idsBegin++] = 0;
//                } else {
//                    int offset = readOffset(side, s), len = readOffset(side, s+1)-offset;
//                    copyId(side, sideCols, s, row, sources.length, i);
//                    idsBegin += 4;
//                    arraycopy(side, offset, row, out, len);
//                    out += len;
//                }
//            }
//            assert isValid(row);
//            return row;
        }
    }

    /* --- --- --- implementation helpers --- --- --- */

    private static void writeColumns(byte[] row, int col) {
        row[COLS_BEGIN  ] = (byte)col;
        row[COLS_BEGIN+1] = (byte)(col>>8);
    }

    private static int readOffset(byte[] row, int col) {
        int i = OFFSETS_BEGIN+OFFSET_BYTES*col;
        return  (row[i++]&0xff)        | ((row[i  ]&0xff) <<  8);
//           | ((row[i++]&0xff) << 16) | ((row[i  ]&0xff) << 24);
    }
    private static void writeOffset(byte[] row, int col, int offset) {
        int i = OFFSETS_BEGIN+OFFSET_BYTES*col;
        row[i++] = (byte) offset;
        row[i  ] = (byte)(offset>>8);
//        row[i++] = (byte)(offset>>16);
//        row[i  ] = (byte)(offset>>24);
    }

    private static int stringsBegin(int columns) {
        return OFFSETS_BEGIN+OFFSET_BYTES+(OFFSET_BYTES+ID_BYTES)*columns;
    }
    private static int idsBegin(int columns) {
        return OFFSETS_BEGIN+OFFSET_BYTES*(columns+1);
    }

    private static int readId(byte[] row, int columns, int col) {
        int i = OFFSETS_BEGIN+OFFSET_BYTES*(columns+1)+ID_BYTES*col;
        return  (row[i++]&0xff)        | ((row[i++]&0xff) <<  8)
             | ((row[i++]&0xff) << 16) | ((row[i  ]&0xff) << 24);
    }

    boolean isValid(byte[] row) {
        int cols = columns(row);
        if (cols == 0 && row != EMPTY)
            return false; // not using EMPTY for empty row wastes memory
        if (readOffset(row, cols) > row.length)
            return false; // end offset > row.length
        for (int i = 0; i < cols; i++) {
            int off = readOffset(row, i), next = readOffset(row, i+1);
            if (off < 0 || next-off < 0)
                return false; //negative offset or size
        }
        if (readOffset(row, 0) != stringsBegin(cols))
            return false; // there is gap between metadata and first string
        for (int i = 0; i < cols; i++) {
            int fId = readId(row, cols, i);
            var rope = RopeDict.getTolerant(fId);
            if (fId < 0)
                return rope.has(0, ByteRope.DT_MID_LT); // suffixes must start with "^^<
            else if (fId > 0)
                return rope.get(0) == '<'; // prefixes must start with <
        }
        return true;
    }

    boolean isValid(byte[] left, byte[] right) {
        return     (  left != null || right != null  )
                && (  left == null || isValid(left ) )
                && ( right == null || isValid(right) );
    }
}
