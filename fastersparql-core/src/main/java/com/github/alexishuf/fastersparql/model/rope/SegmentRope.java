package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U8_BASE;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Integer.rotateLeft;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static jdk.incubator.vector.ByteVector.fromMemorySegment;

@SuppressWarnings("resource")
public abstract class SegmentRope extends PlainRope {
    protected static final boolean DEBUG = SegmentRope.class.desiredAssertionStatus();

    public static final byte[]        EMPTY_UTF8    = Bytes.EMPTY.arr;
    public static final MemorySegment EMPTY_SEGMENT = Bytes.EMPTY.segment;

    public long offset;
    public byte @Nullable [] utf8;
    public MemorySegment segment;

    public SegmentRope() {
        super(0);
        this.offset  = 0;
        this.utf8    = EMPTY_UTF8;
        this.segment = EMPTY_SEGMENT;
    }

    protected static void checkRange(long offset, int len, long cap) {
        if (offset < 0 || len < 0 ||  offset+len > cap) {
            String msg = len < 0 ? "negative len" : "range [offset,offset+len) is out of bounds";
            throw new IndexOutOfBoundsException(msg);
        }
    }

    protected static int asIntLen(long len) {
        if (len > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Rope len cannot be above Integer.MAX_LEN");
        return (int)len;
    }

    protected static byte @Nullable[] arrayOf(MemorySegment segment) {
        return segment.isNative() ? null : (byte[])segment.heapBase().orElse(null);
    }
    protected static void checkSameArray(MemorySegment segment, byte[] utf8) {
        if (utf8 != segment.heapBase().orElse(null))
            throw new IllegalArgumentException("given byte[] is not segment.heapBase()");
    }

    protected SegmentRope(byte[] utf8) {
        this(MemorySegment.ofArray(utf8), utf8, 0, utf8.length);
    }

    protected SegmentRope(MemorySegment segment, byte @Nullable[] utf8, long offset, int len) {
        super(len);
        checkRange(offset, len, segment.byteSize());
        this.offset  = offset;
        this.segment = segment;
        if (utf8 == null) {
            this.utf8 = arrayOf(segment);
        } else {
            if (DEBUG)
                checkSameArray(segment, utf8);
            this.utf8 = utf8;
        }
    }

    protected int rangeLen(int begin, int end) {
        int rLen = end - begin, len = this.len;
        String msg;
        if (rLen < 0) msg = "Range with end < begin";
        else if (begin < 0) msg = "Negative begin";
        else if (end > len) msg = "Range overflows Rope end";
        else return rLen;
        throw new IndexOutOfBoundsException(msg);
    }

    public MemorySegment     segment()            { return segment; }
    public long               offset()            { return offset; }
    public byte @Nullable [] backingArray()       { return utf8; }
    @Override public int     backingArrayOffset() { return (int)(segment.address() + offset); }

    public ByteBuffer asBuffer() {
        if (utf8 != null)
            return ByteBuffer.wrap(utf8, backingArrayOffset(), len);
        long end = offset + len;
        if (end >= Integer.MAX_VALUE)
            return segment.asSlice(offset, len).asByteBuffer();
        return segment.asByteBuffer().position((int) offset).limit((int) end);
    }

    @Override public byte get(int i) {
        if (i < 0 || i >= len) throw new IndexOutOfBoundsException(i);
        if (U == null)
            return segment.get(JAVA_BYTE, offset+i);
        return U.getByte(utf8, segment.address()+offset+i+(utf8 == null ? 0 : U8_BASE));
    }

    @Override public byte[] copy(int begin, int end, byte[] dest, int offset) {
        int rLen = rangeLen(begin, end);
        if (U == null)
            return copySafe(begin, rLen, dest, offset);
        U.copyMemory(utf8, segment.address()+this.offset+begin+(utf8==null ? 0 : U8_BASE),
                     dest, U8_BASE+offset, rLen);
        return dest;
    }

    private byte[] copySafe(int begin, int rLen, byte[] dest, int offset) {
        MemorySegment.copy(segment, JAVA_BYTE, this.offset + begin, dest,
                offset, rLen);
        return dest;
    }

    @Override public int write(OutputStream out) throws IOException {
        if (utf8 != null) {
            out.write(utf8, backingArrayOffset(), len);
            return len;
        }
        return super.write(out);
    }

    @Override public SegmentRope sub(int begin, int end) {
        int rLen = rangeLen(begin, end);
        if (rLen == len) return this;
        return new SegmentRopeView().wrap(this, begin, rLen);
    }

    static long safeSkipUntil(MemorySegment segment, long i, long e, char c0) {
        int rLen = (int)(e-i);
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> c0Vec = LowLevelHelper.B_SP.broadcast(c0);
            for (long ve = i + LowLevelHelper.B_SP.loopBound(rLen); i < ve; i += LowLevelHelper.B_LEN) {
                int lane = fromMemorySegment(LowLevelHelper.B_SP, segment, i, LITTLE_ENDIAN).eq(c0Vec).firstTrue();
                if (lane < LowLevelHelper.B_LEN) return (int) (i + lane);
            }
        }
        while (i < e && segment.get(JAVA_BYTE, i) != c0) ++i;
        return i;
    }
    static long skipUntil(MemorySegment segment, byte[] utf8, long i, long e, char c0) {
        if (U == null)
            return safeSkipUntil(segment, i, e, c0);
        int rLen = (int)(e-i);
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> c0Vec = LowLevelHelper.B_SP.broadcast(c0);
            for (long ve = i + LowLevelHelper.B_SP.loopBound(rLen); i < ve; i += LowLevelHelper.B_LEN) {
                int lane = fromMemorySegment(LowLevelHelper.B_SP, segment, i, LITTLE_ENDIAN).eq(c0Vec).firstTrue();
                if (lane < LowLevelHelper.B_LEN) return (int) (i + lane);
            }
        }
        long address = segment.address() + (utf8 == null ? 0 : U8_BASE);
        i += address;
        e += address;
        while (i < e && U.getByte(utf8, i) != c0) ++i;
        return i-address;

    }

    @Override public int skipUntil(int begin, int end, char c0) {
        rangeLen(begin, end);
        return (int) (skipUntil(segment, utf8, begin+offset, end+offset, c0)-offset);
    }

    static long safeSkipUntil(MemorySegment segment, long i, long e, char c0, char c1) {
        int rLen = (int)(e-i);
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> c0Vec = LowLevelHelper.B_SP.broadcast(c0);
            Vector<Byte> c1Vec = LowLevelHelper.B_SP.broadcast(c1);
            for (long ve = i + LowLevelHelper.B_SP.loopBound(rLen); i < ve; i += LowLevelHelper.B_LEN) {
                ByteVector vec = fromMemorySegment(LowLevelHelper.B_SP, segment, i, LITTLE_ENDIAN);
                int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).firstTrue();
                if (lane < LowLevelHelper.B_LEN) return i + lane;
            }
        }
        for (byte c; i < e && (c=segment.get(JAVA_BYTE, i)) != c0 && c != c1;) ++i;
        return i;
    }
    static long skipUntil(MemorySegment segment, byte[] utf8, long i, long e, char c0, char c1) {
        if (U == null)
            return safeSkipUntil(segment, i, e, c0, c1);
        int rLen = (int)(e-i);
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> c0Vec = LowLevelHelper.B_SP.broadcast(c0);
            Vector<Byte> c1Vec = LowLevelHelper.B_SP.broadcast(c1);
            for (long ve = i + LowLevelHelper.B_SP.loopBound(rLen); i < ve; i += LowLevelHelper.B_LEN) {
                ByteVector vec = fromMemorySegment(LowLevelHelper.B_SP, segment, i, LITTLE_ENDIAN);
                int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).firstTrue();
                if (lane < LowLevelHelper.B_LEN) return i + lane;
            }
        }
        long address = segment.address() + (utf8 == null ? 0 : U8_BASE);
        i += address;
        e += address;
        for (byte c; i < e && (c= U.getByte(utf8, i)) != c0 && c != c1;) ++i;
        return i-address;
    }

    @Override public int skipUntil(int begin, int end, char c0, char c1) {
        rangeLen(begin, end);
        return (int)(skipUntil(segment, utf8, offset+begin, offset+end, c0, c1)-offset);
    }

    static long skipUntilLast(MemorySegment segment, long begin, long end, char c0) {
        int rLen = (int)(end-begin);
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> c0Vec = LowLevelHelper.B_SP.broadcast(c0);
            while ((end -= LowLevelHelper.B_LEN) >= begin) {
                int lane = fromMemorySegment(LowLevelHelper.B_SP, segment, end, LITTLE_ENDIAN).eq(c0Vec).lastTrue();
                if (lane >= 0) return end + lane;
            }
            end += LowLevelHelper.B_LEN; // the while above always overdraws from i
        }
        for (end -= 1; end >= begin; --end) {
            if (segment.get(JAVA_BYTE, end) == c0) return end;
        }
        return begin+rLen;
    }

    @Override public int skipUntilLast(int begin, int end, char c0) {
        rangeLen(begin, end);
        return (int)(skipUntilLast(segment, offset+begin, offset+end, c0)-offset);
    }

    static long skipUntilLast(MemorySegment segment, long begin, long end, char c0, char c1) {
        int rLen = (int)(end-begin);
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> c0Vec = LowLevelHelper.B_SP.broadcast(c0);
            Vector<Byte> c1Vec = LowLevelHelper.B_SP.broadcast(c1);
            while ((end -= LowLevelHelper.B_LEN) >= begin) {
                ByteVector vec = fromMemorySegment(LowLevelHelper.B_SP, segment, end, LITTLE_ENDIAN);
                int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).lastTrue();
                if (lane >= 0) return end  + lane;
            }
            end += LowLevelHelper.B_LEN; // the while above always overdraws from end
        }
        end -= 1;
        for (byte c; end >= begin; --end) {
            if ((c=segment.get(JAVA_BYTE, end)) == c0 || c == c1) return end;
        }
        return begin+rLen;
    }

    @Override public int skipUntilLast(int begin, int end, char c0, char c1) {
        rangeLen(begin, end);
        return (int)(skipUntilLast(segment, offset+begin, offset+end, c0, c1)-offset);
    }

    boolean isEscapedPhys(long begin, long i) {
        long not = i - 1;
        while (not >= begin && segment.get(JAVA_BYTE, not) == '\\') --not;
        return ((i - not) & 1) == 0;
    }

    @Override public boolean isEscaped(int i) { return isEscapedPhys(0, i+offset); }
    @Override public boolean isEscaped(int begin, int i) {
        return isEscapedPhys(begin+offset, i+offset);
    }

    @Override public int skipUntilUnescaped(int begin, int end, char c) {
        var segment = this.segment;
        int rLen = rangeLen(begin, end);
        long i = begin + offset;
        if (LowLevelHelper.ENABLE_VEC && rLen >= LowLevelHelper.B_LEN) {
            Vector<Byte> cVec = LowLevelHelper.B_SP.broadcast(c);
            for (long ve = i + LowLevelHelper.B_SP.loopBound(rLen); i < ve; i += LowLevelHelper.B_LEN) {
                long found = fromMemorySegment(LowLevelHelper.B_SP, segment, i, LITTLE_ENDIAN).eq(cVec).toLong();
                for (int lane = 0; (lane+=numberOfTrailingZeros(found>>>lane)) < 64; ++lane)
                    if (!isEscapedPhys(begin, i+lane)) return (int) (i - offset) + lane;
            }
        }
        for (long e = end+offset; i < e; ++i) {
            byte v = segment.get(JAVA_BYTE, i);
            if (v == c) break;
            if (v == '\\' && ++i == e) --i;
        }
        return (int)(i-offset);
    }

    static long skipUnsafe(byte[] base, long begin, long end, int[] alphabet) {
        boolean stopOnNonAscii = (alphabet[3] & 0x80000000) == 0;
        for (; begin < end; ++begin) {
            byte c = U.getByte(base, begin);
            if (c >= 0) { // c is ASCII
                if ((alphabet[c >> 5] & (1 << c)) == 0)
                    break; // c is not in alphabet
            } else if (stopOnNonAscii) {
                break; // non-ASCII  not allowed by alphabet
            }
        }
        return begin;
    }

    static long skipSafe(MemorySegment segment, long begin, long end, int[] alphabet) {
        boolean stopOnNonAscii = (alphabet[3] & 0x80000000) == 0;
        for (; begin < end; ++begin) {
            byte c = segment.get(JAVA_BYTE, begin);
            if (c >= 0) { // c is ASCII
                if ((alphabet[c >> 5] & (1 << c)) == 0)
                    break; // c is not in alphabet
            } else if (stopOnNonAscii) {
                break; // non-ASCII  not allowed by alphabet
            }
        }
        return begin;
    }

    @Override public int skip(int begin, int end, int[] alphabet) {
        rangeLen(begin, end);
        long offset = this.offset, i;
        if (U == null) {
            i = skipSafe(segment, begin+offset, end+offset, alphabet);
        } else {
            offset += segment.address();
            byte[] u8 = utf8;
            if (u8 != null)
                offset += U8_BASE;
            i = skipUnsafe(u8, begin+offset, end+offset, alphabet);
        }
        return (int)(i-offset);
    }

    @Override public int skipWS(int begin, int end) {
        long i = begin+offset, e = i + rangeLen(begin, end); // checks bounds
        for (byte c; i < e && (c = segment.get(JAVA_BYTE, i)) <= ' ' && c >= 0; ) ++i;
        return (int) (i - offset);
    }

    @Override public int reverseSkipUntil(int begin, int end, char c) {
        long physBegin = begin+offset, i = physBegin+rangeLen(begin, end)-1; // checks bounds
        while (i >= physBegin && segment.get(JAVA_BYTE, i) != c) --i;
        return (int) (Math.max(physBegin, i) - offset);
    }

    private boolean hasSafe(int position, byte[] seq) {
        long i = position + offset;
        for (byte c : seq) {
            if (c != segment.get(JAVA_BYTE, i++)) return false;
        }
        return true;
    }

    @Override public boolean has(int position, byte[] seq) {
        if (position < 0) throw new IndexOutOfBoundsException();
        if (position + seq.length > len) return false;
        if (U == null)
            return hasSafe(position, seq);
        return compare1_1(utf8, segment.address()+offset+position, seq.length,
                          seq, 0, seq.length) == 0;
    }

    public static boolean has(MemorySegment left, long pos, MemorySegment right, long begin, int rLen) {
        long end = begin+rLen;
//        if (left != right)
//            return mismatch(left, pos, pos+ rLen, right, begin, end) < 0;
        // else: manually compare due to JDK-8306866

        if (LowLevelHelper.ENABLE_VEC && rLen > LowLevelHelper.B_LEN) {
            for (long ve = pos+ LowLevelHelper.B_SP.loopBound(rLen); pos < ve; pos += LowLevelHelper.B_LEN, begin += LowLevelHelper.B_LEN) {
                ByteVector l = fromMemorySegment(LowLevelHelper.B_SP,  left, pos,   LITTLE_ENDIAN);
                ByteVector r = fromMemorySegment(LowLevelHelper.B_SP, right, begin, LITTLE_ENDIAN);
                if (!l.eq(r).allTrue()) return false;
            }
            rLen = (int)(end-begin);
        }

        if (U == null) {
            end = begin + rLen;
            for (; begin < end && left.get(JAVA_BYTE, pos) == right.get(JAVA_BYTE, begin); ++begin)
                ++pos;
        } else {
            Object lBase =  left.isNative() ? null :  left.heapBase().orElse(null);
            Object rBase = right.isNative() ? null : right.heapBase().orElse(null);
            pos += left.address() + (lBase == null ? 0 : U8_BASE);
            begin += right.address() + (rBase == null ? 0 : U8_BASE);
            end = begin + rLen;
            for (; begin < end && U.getByte(lBase, pos) == U.getByte(rBase, begin); ++begin)
                ++pos;
        }
        return begin == end;

//        return compareTo(left, pos, rLen, right, begin, rLen) == 0;

//        while (begin < end) {
//            if (left.get(JAVA_BYTE, pos++) != right.get(JAVA_BYTE, begin++)) return false;
//        }
//        return true;
    }
    public boolean has(int pos, SegmentRope rope, int begin, int end) {
        int rLen = end - begin;
        if (pos < 0 || rLen < 0) throw new IndexOutOfBoundsException();
        if (pos + rLen > len) return false;

        long lOff = offset + pos, rOff = rope.offset + begin;
        if (U == null)
            return compare1_1(segment, lOff, rLen, rope.segment, rOff, rLen) == 0;
        return compare1_1(utf8, segment.address()+lOff, rLen,
                          rope.utf8, rope.segment.address()+rOff, rLen) == 0;
    }

    public boolean hasSafe(int pos, Rope rope, int begin, int end) {
        int rLen = end - begin;
        if (pos < 0 || rLen < 0) throw new IndexOutOfBoundsException();
        if (pos + rLen > len) return false;

        if (rope instanceof SegmentRope s) {
            return compare1_1(segment, offset+pos, rLen,
                    s.segment, s.offset+begin, rLen) == 0;
        } else {
            MemorySegment fst, snd;
            long fstOff, sndOff;
            int fstLen;
            if (rope instanceof TwoSegmentRope t) {
                fst = t.fst; fstOff = t.fstOff; fstLen = t.fstLen;
                snd = t.snd; sndOff = t.sndOff;
            } else if (rope instanceof Term t) {
                SegmentRope fr = t.first(), sr = t.second();
                fst = fr.segment; fstOff = fr.offset; fstLen = fr.len;
                snd = sr.segment; sndOff = sr.offset;
            } else {
                if (rope == null) throw new NullPointerException("rope");
                throw new UnsupportedOperationException("Unsupported Rope type");
            }
            fstOff += begin;
            sndOff += Math.max(0, begin-fstLen);
            fstLen = Math.min(fstLen, end)-begin;
            return compare1_2(segment, pos, rLen, fst, fstOff, fstLen,
                    snd, sndOff, rLen-fstLen) == 0;
        }
    }
    @Override public boolean has(int pos, Rope rope, int begin, int end) {
        if (U == null)
            return hasSafe(pos, rope, begin, end);
        int rLen = end - begin;
        if (pos < 0 || rLen < 0) throw new IndexOutOfBoundsException();
        if (pos + rLen > len) return false;

        if (rope instanceof SegmentRope s) {
            long lOff = this.offset +   segment.address() + pos;
            long rOff =    s.offset + s.segment.address() + begin;
            return compare1_1(utf8, lOff, rLen, s.utf8, rOff, rLen) == 0;
        } else {
            byte[] fst, snd;
            long fstOff, sndOff;
            int fstLen;
            if (rope instanceof TwoSegmentRope t) {
                fst = t.fstU8; fstOff = t.fst.address()+t.fstOff; fstLen = t.fstLen;
                snd = t.sndU8; sndOff = t.snd.address()+t.sndOff;
            } else if (rope instanceof Term t) {
                SegmentRope fr = t.first(), sr = t.second();
                fst = fr.utf8; fstOff = fr.segment.address()+fr.offset; fstLen = fr.len;
                snd = sr.utf8; sndOff = sr.segment.address()+sr.offset;
            } else {
                if (rope == null) throw new NullPointerException("rope");
                throw new UnsupportedOperationException("Unsupported Rope type");
            }
            fstOff += begin;
            sndOff += Math.max(0, begin-fstLen);
            fstLen = Math.min(fstLen, end)-begin;
            return compare1_2(utf8, segment.address()+offset+pos, rLen, fst, fstOff, fstLen,
                              snd, sndOff, rLen-fstLen) == 0;
        }
    }

    public boolean equals(SegmentRope o) {
        int len = this.len;
        if (o == null || len != o.len) return false;
        if (o == this || (o.utf8==utf8 && o.segment==segment && o.offset==offset)) return true;
        return has(0, o, 0, len);
    }


    @Override public boolean equals(Object o) {
        if (o instanceof SegmentRope r) {
            int rLen = r.len;
            if (rLen != len) return false;
            if (r.utf8 == utf8 && r.segment == segment && r.offset == offset) return true;
            return has(0, r, 0, rLen);
        }
        return super.equals(o);
    }

    @Override public boolean hasAnyCase(int position, byte[] up,
                                        int upOffset, int upLen) {
        if (position < 0) throw new IndexOutOfBoundsException(position);
        if (position+upLen > len)
            return false;
        var segment = this.segment;
        long phys = position+offset;
        for (int i = upOffset, upEnd = upOffset+upLen; i < upEnd; i++) {
            byte actual = segment.get(JAVA_BYTE, phys++), ex = up[i];
            if (actual != ex && ((actual < 'a' || actual > 'z') || actual-32 != ex))
                return false;
        }
        return true;
    }

    @Override public int fastHash(int begin, int end) {
        int h, nFst = Math.min(4, end-begin), nSnd = Math.min(12, end-(begin+4));
        long phys = offset+begin;
        if (U != null) {
            byte[] base = utf8;
            phys += segment.address();
            h = hashCode(FNV_BASIS, base, phys, nFst);
            h = hashCode(h, base, phys+(end-nSnd)-begin, nSnd);
        } else {
            h = hashCode(FNV_BASIS, segment, phys, nFst);
            h = hashCode(h, segment, offset+(end-nSnd), nSnd);
        }
        return h;
    }

    public static int hashCode(int h, byte[] base, long offset, int len) {
        if (base != null) offset += U8_BASE;
        for (int i = 0; i < len; i++)
            h = FNV_PRIME * (h ^ (0xff& U.getByte(base, offset+i)));
        return h;
    }

    public static int hashCode(int h, MemorySegment seg, long off, int len) {
//        if (U != null) {
//            if (len == 0) return h;
//            byte[] base = (byte[])seg.array().orElse(null);
//            return hashCode(h, base, off+seg.address() + (base == null ? 0 : U8_BASE), len);
//        }
        for (int i = 0; i < len; i++)
            h = FNV_PRIME * (h ^ (0xff&seg.get(JAVA_BYTE, off+i)));
        return h;
    }

    @Override public int hashCode() {
        if (U == null)
            return hashCode(FNV_BASIS, segment, offset, len);
        else
            return hashCode(FNV_BASIS, utf8, segment.address()+offset, len);
    }


    @Override public void appendTo(StringBuilder sb, int begin, int end) {
        try (var d = RopeDecoder.create()) {
            d.write(sb, segment, offset+begin, end-begin);
        }
    }

    @Override public @NonNull String toString() {
        if (len == 0) return "";
        if (utf8 != null)
            return new String(utf8, backingArrayOffset(), len, UTF_8);
        return super.toString();
    }

    @Override public String toString(int begin, int end) {
        if (utf8 != null && end-begin > 0)
            return new String(utf8, backingArrayOffset()+begin, end-begin, UTF_8);
        return super.toString(begin, end);
    }

    public static int compare2_2(byte[] lBase, long lOff, int lLen,
                                 byte[] lSnd, long lSndOff, int lSndLen,
                                 byte[] rBase, long rOff, int rLen,
                                 byte[] rSnd, long rSndOff, int rSndLen) {
        boolean lFst = true, rFst = true;
        if (lBase == rBase && lLen == rLen && lOff == rOff)
            lLen = rLen = 0; // do not compare bytes of same reference
        // move to second segment if first is empty
        if (lLen <= 0) { lBase = lSnd; lOff = lSndOff; lLen = lSndLen; lFst = false; }
        if (rLen <= 0) { rBase = rSnd; rOff = rSndOff; rLen = rSndLen; rFst = false; }

        for (int common, diff; true; ) {
            if ((common = Math.min(lLen, rLen)) <= 0)
                return lLen - rLen; // one side exhausted, largest wins
            if ((diff = compare1_1(lBase, lOff, common, rBase, rOff, common)) != 0)
                return diff; // found a mismatching byte
            // advance ranges
            lOff += common; lLen -= common;
            rOff += common; rLen -= common;
            if (lLen == 0 && lFst) { lBase = lSnd; lOff = lSndOff; lLen = lSndLen; lFst = false; }
            if (rLen == 0 && rFst) { rBase = rSnd; rOff = rSndOff; rLen = rSndLen; rFst = false; }
        }
    }

    public static int compare2_2(MemorySegment lSeg, long lOff, int lLen,
                                 MemorySegment lSnd, long lSndOff, int lSndLen,
                                 MemorySegment rSeg, long rOff, int rLen,
                                 MemorySegment rSnd, long rSndOff, int rSndLen) {
//        if (U != null) {
//            // unwrap MemorySegments for unsafe access
//            byte[] lBase = (byte[])lSeg.array().orElse(null);
//            byte[] rBase = (byte[])rSeg.array().orElse(null);
//            byte[] lSndBase = (byte[])lSnd.array().orElse(null);
//            byte[] rSndBase = (byte[])rSnd.array().orElse(null);
//            lOff    += lSeg.address();
//            rOff    += rSeg.address();
//            lSndOff += lSnd.address();
//            rSndOff += rSnd.address();
//
//            boolean lFst = true, rFst = true;
//            // move to second segment if first is empty
//            if (lLen <= 0) { lBase = lSndBase; lOff = lSndOff; lLen = lSndLen; lFst = false; }
//            if (rLen <= 0) { rBase = rSndBase; rOff = rSndOff; rLen = rSndLen; rFst = false; }
//
//            for (int common, diff; true; ) {
//                if ((common = Math.min(lLen, rLen)) <= 0)
//                    return lLen - rLen; // one side exhausted, largest wins
//                if ((diff = compare1_1(lBase, lOff, common, rBase, rOff, common)) != 0)
//                    return diff; // found a mismatching byte
//                // advance ranges
//                lOff += common; lLen -= common;
//                rOff += common; rLen -= common;
//                if (lLen == 0 && lFst) { lBase = lSndBase; lOff = lSndOff; lLen = lSndLen; lFst = false; }
//                if (rLen == 0 && rFst) { rBase = rSndBase; rOff = rSndOff; rLen = rSndLen; rFst = false; }
//            }
//        }

        boolean lFst = true, rFst = true;
        if (lSeg == rSeg && lLen == rLen && lOff == rOff)
            lLen = rLen = 0; // do not compare bytes of same reference
        // move to second segment if first is empty
        if (lLen <= 0) { lSeg = lSnd; lOff = lSndOff; lLen = lSndLen; lFst = false; }
        if (rLen <= 0) { rSeg = rSnd; rOff = rSndOff; rLen = rSndLen; rFst = false; }

        for (int common, diff; true; ) {
            if ((common = Math.min(lLen, rLen)) <= 0)
                return lLen - rLen; // one side exhausted, largest wins
            if ((diff = compare1_1(lSeg, lOff, common, rSeg, rOff, common)) != 0)
                return diff; // found a mismatching byte
            // advance ranges
            lOff += common; lLen -= common;
            rOff += common; rLen -= common;
            if (lLen == 0 && lFst) { lSeg = lSnd; lOff = lSndOff; lLen = lSndLen; lFst = false; }
            if (rLen == 0 && rFst) { rSeg = rSnd; rOff = rSndOff; rLen = rSndLen; rFst = false; }
        }
    }

    public static int compare1_2(MemorySegment lSeg, long lOff, int lLen,
                                 MemorySegment rFst, long rFstOff, int rFstLen,
                                 MemorySegment rSnd, long rSndOff, int rSndLen) {
//        if (U != null) {
//            byte[] lBase = (byte[]) lSeg.array().orElse(null);
//            byte[] rBase = (byte[]) rFst.array().orElse(null);
//            lOff += lSeg.address();
//            rFstOff += rFst.address();
//
//            // compare all we can on the left side with all we can of the right side
//            int common = Math.min(lLen, rFstLen);
//            int diff = compare1_1(lBase, lOff, common, rBase, rFstOff, common);
//            if (diff != 0) return diff;
//            if (rFstLen > common)
//                return -1; // left side exhausted before right side
//
//            // update [lOff, lOff+lLen) range
//            lOff += common;
//            lLen -= common;
//
//            // compare whatever remains on the left side with the second segment of o
//            return compare1_1(lBase, lOff, lLen, (byte[]) rSnd.array().orElse(null),
//                              rSndOff + rSnd.address(), rSndLen);
//        }
        long lOff1 = lOff;
        int lLen1 = lLen;
        // compare all we can on the left side with all we can of the right side
        int common = Math.min(lLen1, rFstLen);
        int diff = compare1_1(lSeg, lOff1, common, rFst, rFstOff, common);
        if (diff != 0) return diff;
        if (rFstLen > common)
            return -1; // left side exhausted before right side

        // update [lOff, lOff+lLen) range
        lOff1 += common;
        lLen1 -= common;

        // compare whatever remains on the left side with the second segment of o
        return compare1_1(lSeg, lOff1, lLen1, rSnd, rSndOff, rSndLen);
    }

    public static int compare1_2(byte[] lBase, long lOff, int lLen,
                                 byte[] rFst, long rFstOff, int rFstLen,
                                 byte[] rSnd, long rSndOff, int rSndLen) {
        if (U == null)
            throw new IllegalStateException("no unsafe");

        // compare all we can on the left side with all we can of the right side
        int common = Math.min(lLen, rFstLen);
        int diff = compare1_1(lBase, lOff, common, rFst, rFstOff, common);
        if (diff != 0) return diff;
        if (rFstLen > common)
            return -1; // left side exhausted before right side

        // update [lOff, lOff+lLen) range
        lOff += common;
        lLen -= common;

        // compare whatever remains on the left side with the second segment of o
        return compare1_1(lBase, lOff, lLen, rSnd, rSndOff, rSndLen);
    }

    public static int compare1_1(byte[] lBase, long lOff, int lLen,
                                 byte[] rBase, long rOff, int rLen) {
        if (lOff == rOff && lBase == rBase && lLen == rLen)
            return 0;
        if (lBase != null)
            lOff += U8_BASE;
        if (rBase != null)
            rOff += U8_BASE;
        long lEnd = lOff + Math.min(lLen, rLen);
        int diff = (int)((lOff|rOff)&7);
        if (diff == 0) {
            for (; lOff+8 < lEnd; lOff += 8, rOff += 8) {
                if (U.getLong(lBase, lOff) != U.getLong(rBase, rOff)) break;
            }
        }
        for (; lOff < lEnd; ++lOff, ++rOff) {
            if ((diff = U.getByte(lBase, lOff) - U.getByte(rBase, rOff)) != 0) return diff;
        }
        return lLen-rLen;
    }

    public static int compare1_1(MemorySegment left, long lOff, int lLen,
                                 MemorySegment right, long rOff, int rLen) {
//        if (U != null) {
//            byte[] lBase = (byte[]) left.array().orElse(null);
//            byte[] rBase = (byte[]) right.array().orElse(null);
//            lOff +=  left.address();
//            rOff += right.address();
//            return compare1_1(lBase, lOff, lLen, rBase, rOff, rLen);
//        }
        //vectorization helps, but is slower than JAVA_INT_UNALIGNED. Using Vector.lane() is
        // too slow. Best vector implementation consisted of left.compare(NE, right).firstTrue()
        // followed by JAVA_BYTE scalar accesses. ByteVector.sub() cannot be used as byte overflows
        // render the comparison invalid.

        // On DictFindBench, reading ints is faster than reading longs. likely because most
        // calls will mismatch in the first 4 bytes for CompositeDict.

        if (lOff == rOff && left == right && lLen == rLen)
            return 0;

        long lEnd = lOff+Math.min(lLen, rLen);
        for (; lOff + 4 < lEnd; lOff += 4, rOff += 4) {
            if (left.get(JAVA_INT_UNALIGNED, lOff) != right.get(JAVA_INT_UNALIGNED, rOff)) break;
        }

        for (int diff; lOff < lEnd; lOff++, rOff++) {
            if ((diff = left.get(JAVA_BYTE, lOff) - right.get(JAVA_BYTE, rOff)) != 0) {
                return diff;
            }
        }
        return lLen - rLen;
    }


    public final int compareTo(SegmentRope o) {
        if (U == null)
            return compare1_1(segment, offset, len, o.segment, o.offset, o.len);
        return compare1_1(utf8, segment.address()+offset, len,
                          o.utf8, o.segment.address()+o.offset, o.len);
    }

    public final int compareTo(TwoSegmentRope o) {
        if (U == null)
            return compare1_2(segment, offset, len,
                              o.fst, o.fstOff, o.fstLen, o.snd, o.sndOff, o.sndLen);
        return compare1_2(  utf8, segment.address()+offset, len,
                          o.fstU8, o.fst.address()+o.fstOff, o.fstLen,
                          o.sndU8, o.snd.address()+o.sndOff, o.sndLen);
    }

    @Override public int compareTo(SegmentRope o, int begin, int end) {
        if (U == null)
            return compare1_1(segment, offset, len, o.segment, o.offset+begin, end-begin);
        return compare1_1(  utf8,   segment.address() +  offset,       len,
                          o.utf8, o.segment.address() +o.offset+begin, end-begin);
    }

    @Override public int compareTo(TwoSegmentRope o, int begin, int end) {
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        return compare1_2(segment, offset, len,
                          o.fst, o.fstOff+begin, Math.min(o.fstLen,end)-begin,
                          o.snd, o.sndOff+Math.max(0, begin-o.fstLen), end-o.fstLen);
    }

    private static int compareNumbersExp(MemorySegment lSeg, long lOff, int lLen,
                                         MemorySegment rSeg, long rOff, int rLen) {
        char[] tmp = new char[Math.max(lLen, rLen)];
        for (int i = 0; i < lLen; i++)
            tmp[i] = (char)lSeg.get(JAVA_BYTE, lOff+i);
        var left = new BigDecimal(tmp, 0, lLen);
        for (int i = 0; i < rLen; i++)
            tmp[i] = (char)rSeg.get(JAVA_BYTE, rOff+i);
        var right = new BigDecimal(tmp, 0, lLen);
        return left.compareTo(right);
    }
    private static final int CMP_NUM_HAS_EXP    = 0x80000000;
    private static final int CMP_NUM_HAS_DOT    = 0x40000000;
    private static final int CMP_NUM_SKIP_MASK  = 0x3fff0000;
    private static final int CMP_NUM_SKIP_ONE   = 0x00010000;
    private static final int CMP_NUM_SKIP_BIT   = numberOfTrailingZeros(CMP_NUM_SKIP_MASK);
    private static final int CMP_NUM_POWER_MASK = 0x0000ffff;
    private static int compareNumbersScan(MemorySegment s, long off, int len) {
        int result = 0;
        boolean skipping = true;
        for (int i = 0; i < len; i++) {
            byte c = s.get(JAVA_BYTE, off+i);
            if (c > '9') {
                return CMP_NUM_HAS_EXP;
            } else if (c == '.') {
                result |= CMP_NUM_HAS_DOT;
                result |= i-((result&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT);
                skipping = false;
            } else if (c > '0') {
                skipping = false;
            } else if (skipping) {
                result += CMP_NUM_SKIP_ONE;
            }
        }
        if ((result&CMP_NUM_HAS_DOT) == 0)
            result |= len-((result&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT);
        return result;
    }
    public static int compareNumbers(MemorySegment lSeg, long lOff, int lLen,
                                     MemorySegment rSeg, long rOff, int rLen) {
        int lScan = compareNumbersScan(lSeg, lOff, lLen);
        int rScan = compareNumbersScan(rSeg, rOff, rLen);
        if (((lScan|rScan)&CMP_NUM_HAS_EXP) != 0)
            return compareNumbersExp(lSeg, lOff, lLen, rSeg, rOff, rLen);
        int diff = (lScan&CMP_NUM_POWER_MASK) - (rScan&CMP_NUM_POWER_MASK);
        if (diff != 0) return diff;

        long lEnd = lOff+lLen, rEnd = rOff+rLen;
        lOff += (lScan&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT;
        rOff += (rScan&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT;
        while (lOff < lEnd && rOff < rEnd) {
            if ((diff=lSeg.get(JAVA_BYTE, lOff++) - rSeg.get(JAVA_BYTE, rOff++)) != 0) return diff;
        }

        int sign = 0;
        if (rOff < rEnd) {
            if ((rScan&CMP_NUM_HAS_DOT) == 0) return -1;
            if ((lScan&CMP_NUM_HAS_DOT) == 0) ++rOff;
            lSeg = rSeg;
            lOff = rOff;
            lEnd = rEnd;
            sign = -1;
        } else if (lOff < lEnd) {
            if ((lScan&CMP_NUM_HAS_DOT) == 0) return 1;
            if ((rScan&CMP_NUM_HAS_DOT) == 0) ++lOff;
        }
        while (lOff < lEnd) {
            if ((diff=lSeg.get(JAVA_BYTE, lOff++) - '0') != 0) return sign*diff;
        }
        return 0;
    }

    private static int compareNumbersScan(byte[] u8, long off, int len) {
        int result = 0;
        boolean skipping = true;
        for (int i = 0; i < len; i++) {
            byte c = U.getByte(u8, off+i);
            if (c > '9') {
                return CMP_NUM_HAS_EXP;
            } else if (c == '.') {
                result |= CMP_NUM_HAS_DOT;
                result |= i-((result&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT);
                skipping = false;
            } else if (c > '0') {
                skipping = false;
            } else if (skipping) {
                result += CMP_NUM_SKIP_ONE;
            }
        }
        if ((result&CMP_NUM_HAS_DOT) == 0)
            result |= len-((result&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT);
        return result;
    }
    private static int compareNumbersExp(byte[] lU8, long lOff, int lLen,
                                         byte[] rU8, long rOff, int rLen) {
        char[] tmp = new char[Math.max(lLen, rLen)];
        for (int i = 0; i < lLen; i++)
            tmp[i] = (char)U.getByte(lU8, lOff+i);
        var left = new BigDecimal(tmp, 0, lLen);
        for (int i = 0; i < rLen; i++)
            tmp[i] = (char)U.getByte(rU8, rOff+i);
        var right = new BigDecimal(tmp, 0, rLen);
        return left.compareTo(right);
    }
    public static int compareNumbers(byte[] lU8, long lOff, int lLen,
                                     byte[] rU8, long rOff, int rLen) {
        if (lU8 != null)
            lOff += U8_BASE;
        if (rU8 != null)
            rOff += U8_BASE;
        int lScan = compareNumbersScan(lU8, lOff, lLen);
        int rScan = compareNumbersScan(rU8, rOff, rLen);
        if (((lScan|rScan)&CMP_NUM_HAS_EXP) != 0)
            return compareNumbersExp(lU8, lOff, lLen, rU8, rOff, rLen);
        int diff = (lScan&CMP_NUM_POWER_MASK) - (rScan&CMP_NUM_POWER_MASK);
        if (diff != 0) return diff;

        long lEnd = lOff+lLen, rEnd = rOff+rLen;
        lOff += (lScan&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT;
        rOff += (rScan&CMP_NUM_SKIP_MASK)>>>CMP_NUM_SKIP_BIT;
        while (lOff < lEnd && rOff < rEnd) {
            if((diff=U.getByte(lU8, lOff++) - U.getByte(rU8, rOff++)) != 0) return diff;
        }

        int sign = 0;
        if (rOff < rEnd) {
            if ((rScan&CMP_NUM_HAS_DOT) == 0) return -1;
            if ((lScan&CMP_NUM_HAS_DOT) == 0) ++rOff;
            lU8  = rU8;
            lOff = rOff;
            lEnd = rEnd;
            sign = -1;
        } else if (lOff < lEnd) {
            if ((lScan&CMP_NUM_HAS_DOT) == 0) return 1;
            if ((rScan&CMP_NUM_HAS_DOT) == 0) ++lOff;
        }
        while (lOff < lEnd) {
            if ((diff=U.getByte(lU8, lOff++) - '0') != 0) return sign*diff;
        }
        return 0;
    }

    public static int compareNumbers(SegmentRope l, int lOff, int lLen,
                                     SegmentRope r, int rOff, int rLen) {
        long alOff = l.offset+lOff, arOff = r.offset+rOff;
        if (U == null)
            return compareNumbers(l.segment, alOff, lLen, r.segment, arOff, rLen);
        return compareNumbers(l.utf8, alOff, lLen, r.utf8, arOff, rLen);
    }
}