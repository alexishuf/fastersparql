package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static jdk.incubator.vector.ByteVector.fromMemorySegment;

public class SegmentRope extends PlainRope {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_LEN = B_SP.length();
    public static final boolean HAS_UNSAFE;
    static final Unsafe U;
    private static final int U8_BASE;

    static {
        Unsafe u = null;
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            u = (Unsafe) field.get(null);
        } catch (Throwable ignored) {
            try {
                Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
                u = c.newInstance();
            } catch (Throwable ignored1) {}
        }
        U = u;
        HAS_UNSAFE = u != null;
        U8_BASE = u == null ? 0 : u.arrayBaseOffset(byte[].class);
    }

    public long offset;
    public byte @Nullable [] utf8;
    public MemorySegment segment;

    public SegmentRope() {
        super(0);
        this.offset = 0;
        this.utf8 = ByteRope.EMPTY_UTF8;
        this.segment = ByteRope.EMPTY_SEGMENT;
    }
    public SegmentRope(ByteBuffer buffer) {
        this(MemorySegment.ofBuffer(buffer), null, 0, buffer.remaining());
    }

    public SegmentRope(MemorySegment segment, byte @Nullable[] utf8, long offset, int len) {
        super(len);
        if (offset < 0 || len < 0)
            throw new IllegalArgumentException("Negative offset/len");
        if (offset+len > segment.byteSize())
            throw new IndexOutOfBoundsException("offset+len > segment.byteSize");
        this.offset = offset;
        if (utf8 == null)
            this.utf8 = segment.isNative() ? null : (byte[]) segment.array().orElse(null);
        else
            this.utf8 = utf8;
        this.segment = segment;
    }

    public SegmentRope(MemorySegment segment, long offset, int len) {
        this(segment, null, offset, len);
    }

    public SegmentRope(byte @NonNull [] utf8, int offset, int len) {
        this(MemorySegment.ofArray(utf8), utf8, offset, len);
    }

    public static SegmentRope of(Rope r) {
        if (r instanceof SegmentRope sr)
            return sr;
        return new SegmentRope(r.toArray(0, r.len), 0, r.len);
    }

    public static SegmentRope of(Object cs) {
        if (cs instanceof SegmentRope sr)
            return sr;
        if (cs instanceof TwoSegmentRope tsr)
            return new SegmentRope(tsr.toArray(0, tsr.len), 0, tsr.len);
        byte[] u8 = cs.toString().getBytes(UTF_8);
        return new SegmentRope(u8, 0, u8.length);
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

    public void wrap(SegmentRope other) {
        this.segment = other.segment;
        this.utf8    = other.utf8;
        this.offset  = other.offset;
        this.len     = other.len;
    }

    public void slice(long offset, int len) {
        this.offset = offset;
        this.len = len;
    }

    public void wrapSegment(MemorySegment segment, byte[] utf8, long offset, int len) {
        this.segment = segment;
        this.utf8    = utf8;
        this.offset  = offset;
        this.len     = len;
    }

    public void wrapBuffer(ByteBuffer buffer) {
        segment = MemorySegment.ofBuffer(buffer);
        utf8 = buffer.isDirect() ? null : (byte[]) segment.array().orElse(null);
        offset = 0;
        long size = segment.byteSize();
        if (size > Integer.MAX_VALUE) throw new IllegalArgumentException("buffer is too big");
        len = (int) size;
    }

    public void wrapEmptyBuffer() {
        segment = ByteRope.EMPTY_SEGMENT;
        utf8 = ByteRope.EMPTY_UTF8;
        offset = 0;
        len = 0;
    }

    @Override public byte get(int i) {
        if (i < 0 || i >= len) throw new IndexOutOfBoundsException(i);
        if (U == null)
            return segment.get(JAVA_BYTE, offset+i);
        return U.getByte(utf8, segment.address()+offset+i+(utf8 == null ? 0 : U8_BASE));
    }

    @Override public byte[] copy(int begin, int end, byte[] dest, int offset) {
        int rLen = rangeLen(begin, end);
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
        return rLen == len ? this : new SegmentRope(segment, utf8, offset + begin, rLen);
    }

    static long skipUntil(MemorySegment segment, long i, long e, char c0) {
        int rLen = (int)(e-i);
        if (rLen >= B_LEN) {
            Vector<Byte> c0Vec = B_SP.broadcast(c0);
            for (long ve = i + B_SP.loopBound(rLen); i < ve; i += B_LEN) {
                int lane = fromMemorySegment(B_SP, segment, i, LITTLE_ENDIAN).eq(c0Vec).firstTrue();
                if (lane < B_LEN) return (int) (i + lane);
            }
        }
        while (i < e && segment.get(JAVA_BYTE, i) != c0) ++i;
        return i;

    }

    @Override public int skipUntil(int begin, int end, char c0) {
        rangeLen(begin, end);
        return (int) (skipUntil(segment, begin+offset, end+offset, c0)-offset);
    }

    static long skipUntil(MemorySegment segment, long i, long e, char c0, char c1) {
        int rLen = (int)(e-i);
        if (rLen >= B_LEN) {
            Vector<Byte> c0Vec = B_SP.broadcast(c0);
            Vector<Byte> c1Vec = B_SP.broadcast(c1);
            for (long ve = i + B_SP.loopBound(rLen); i < ve; i += B_LEN) {
                ByteVector vec = fromMemorySegment(B_SP, segment, i, LITTLE_ENDIAN);
                int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).firstTrue();
                if (lane < B_LEN) return i + lane;
            }
        }
        for (byte c; i < e && (c=segment.get(JAVA_BYTE, i)) != c0 && c != c1;) ++i;
        return i;
    }

    @Override public int skipUntil(int begin, int end, char c0, char c1) {
        rangeLen(begin, end);
        return (int)(skipUntil(segment, offset+begin, offset+end, c0, c1)-offset);
    }

    static long skipUntilLast(MemorySegment segment, long begin, long end, char c0) {
        int rLen = (int)(end-begin);
        if (rLen >= B_LEN) {
            Vector<Byte> c0Vec = B_SP.broadcast(c0);
            while ((end -= B_LEN) >= begin) {
                int lane = fromMemorySegment(B_SP, segment, end, LITTLE_ENDIAN).eq(c0Vec).lastTrue();
                if (lane >= 0) return end + lane;
            }
            end += B_LEN; // the while above always overdraws from i
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
        if (rLen >= B_LEN) {
            Vector<Byte> c0Vec = B_SP.broadcast(c0);
            Vector<Byte> c1Vec = B_SP.broadcast(c1);
            while ((end -= B_LEN) >= begin) {
                ByteVector vec = fromMemorySegment(B_SP, segment, end, LITTLE_ENDIAN);
                int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).lastTrue();
                if (lane >= 0) return end  + lane;
            }
            end += B_LEN; // the while above always overdraws from end
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
        if (rLen >= B_LEN) {
            Vector<Byte> cVec = B_SP.broadcast(c);
            for (long ve = i + B_SP.loopBound(rLen); i < ve; i += B_LEN) {
                long found = fromMemorySegment(B_SP, segment, i, LITTLE_ENDIAN).eq(cVec).toLong();
                for (int lane = 0; (lane+=numberOfTrailingZeros(found>>>lane)) < 64; ++lane)
                    if (!isEscapedPhys(begin, i+lane)) return (int) (i - offset) + lane;
            }
        }
        for (long e = end+offset; i < e; ++i) {
            byte v = segment.get(JAVA_BYTE, i);
            if (v == '\\') ++i; // skip next byte
            else if (v == c) break;
        }
        return (int) (i-offset);
    }

    static long skip(MemorySegment segment, long begin, long end, int[] alphabet) {
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
        return (int)(skip(segment, begin+offset, end+offset, alphabet)-offset);
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

    @Override public boolean has(int position, byte[] seq) {
        if (position < 0) throw new IndexOutOfBoundsException();
        if (position + seq.length > len) return false;
        long i = position + offset;
        for (byte c : seq) {
            if (c != segment.get(JAVA_BYTE, i++)) return false;
        }
        return true;
    }

    public static boolean has(MemorySegment left, long pos, MemorySegment right, long begin, int rLen) {
        long end = begin+rLen;
//        if (left != right)
//            return mismatch(left, pos, pos+ rLen, right, begin, end) < 0;
        // else: manually compare due to JDK-8306866

        if (rLen > B_LEN) {
            for (long ve = pos+B_SP.loopBound(rLen); pos < ve; pos += B_LEN, begin += B_LEN) {
                ByteVector l = fromMemorySegment(B_SP,  left, pos,   LITTLE_ENDIAN);
                ByteVector r = fromMemorySegment(B_SP, right, begin, LITTLE_ENDIAN);
                if (!l.eq(r).allTrue()) return false;
            }
            rLen = (int)(end-begin);
        }

        if (U == null) {
            end = begin + rLen;
            for (; begin < end && left.get(JAVA_BYTE, pos) == right.get(JAVA_BYTE, begin); ++begin)
                ++pos;
        } else {
            Object lBase =  left.isNative() ? null :  left.array().orElse(null);
            Object rBase = right.isNative() ? null : right.array().orElse(null);
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

    @Override public boolean has(int pos, Rope rope, int begin, int end) {
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

    @Override public boolean hasAnyCase(int position, byte[] uppercaseSequence) {
        if (position < 0) throw new IndexOutOfBoundsException(position);
        if (position + uppercaseSequence.length > len) return false;

        var segment = this.segment;
        long phys = position + offset;
        for (byte expected : uppercaseSequence) {
            byte actual = segment.get(JAVA_BYTE, phys++);
            if (actual != expected && ((actual < 'a' || actual > 'z') || actual - 32 != expected))
                return false;
        }
        return true;
    }

    @Override public int lsbHash(int begin, int end) {
        int bits = rangeLen(begin, end), h = 0, bit = 0;
        if (bits > 32)
            begin = end - (bits = 32);
        long physBegin = begin + offset;
        MemorySegment s = this.segment;
        if (U != null) {
            byte[] base = utf8; //(byte[]) s.array().orElse(null);
            physBegin += s.address() + (base == null ? 0 : U8_BASE);
            while (bit < bits)
                h |= (U.getByte(base, physBegin++)&1) << bit++;
        } else {
            while (bit < bits)
                h |= (s.get(JAVA_BYTE, physBegin + bit) & 1) << bit++;
        }
        return h;
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
            h = FNV_PRIME * (h ^ (0xff&U.getByte(base, offset+i)));
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


    public int hashCode(int hash) {
        if (U == null)
            return hashCode(hash, segment, offset, len);
        else
            return hashCode(hash, utf8, segment.address()+offset, len);
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
                if (U.getLong(lBase, lOff) != U.getLong(rBase, rOff)) { diff = -1; break; }
            }
        }
        if ((diff&3) == 0) {
            for (; lOff+4 < lEnd; lOff += 4, rOff += 4) {
                if (U.getInt(lBase, lOff) != U.getInt(rBase, rOff)) break;
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
        return compare1_1(utf8, segment.address()+offset, len,
                          o.utf8, o.segment.address()+o.offset, o.len);
    }

    public final int compareTo(TwoSegmentRope o) {
        return compare1_2(segment, offset, len,
                          o.fst, o.fstOff, o.fstLen, o.snd, o.sndOff, o.sndLen);
    }

    @Override public int compareTo(SegmentRope o, int begin, int end) {
        return compare1_1(utf8, segment.address()+offset, len,
                o.utf8, o.segment.address()+o.offset+begin, end-begin);
    }

    @Override public int compareTo(TwoSegmentRope o, int begin, int end) {
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        return compare1_2(segment, offset, len,
                          o.fst, o.fstOff+begin, Math.min(o.fstLen,end)-begin,
                          o.snd, o.sndOff+Math.max(0, begin-o.fstLen), end-o.fstLen);
    }
}