package com.github.alexishuf.fastersparql.model.rope;

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
    static final Unsafe U;
    public static final int U8_UNSAFE_BASE;

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
        U8_UNSAFE_BASE = u == null ? 0 : u.arrayBaseOffset(byte[].class);
    }

    public long offset;
    protected byte @Nullable [] utf8;
    public MemorySegment segment;

    public SegmentRope() {
        this(ByteRope.EMPTY_SEGMENT, 0, 0);
    }
    public SegmentRope(ByteBuffer buffer) {
        this(MemorySegment.ofBuffer(buffer), 0, buffer.remaining());
    }
    public SegmentRope(MemorySegment segment, long offset, int len) {
        super(len);
        if (offset < 0 || len < 0)
            throw new IllegalArgumentException("Negative offset/len");
        if (offset+len > segment.byteSize())
            throw new IndexOutOfBoundsException("offset+len > segment.byteSize");
        this.offset = offset;
        this.utf8 = len == 0 ? ByteRope.EMPTY_UTF8 : null;
        this.segment = segment;
    }

    public SegmentRope(byte @NonNull [] utf8, int offset, int len) {
        super(len);
        if (offset < 0 || len < 0)
            throw new IllegalArgumentException("Negative offset/len");
        if (offset+len > utf8.length)
            throw new IndexOutOfBoundsException("offset+len > utf8.length");
        this.segment = MemorySegment.ofArray(utf8);
        this.utf8 = utf8;
        this.offset = offset;
    }

    public static SegmentRope of(Object cs) {
        if (cs instanceof SegmentRope sr) return sr;
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
    @Override public int     backingArrayOffset() { return (int)offset; }



    public ByteBuffer asBuffer() {
        int iOffset = (int) offset;
        if (utf8 != null)
            return ByteBuffer.wrap(utf8, iOffset, len);
        return segment.asByteBuffer().position(iOffset).limit(iOffset+len);
    }

    public void wrap(SegmentRope other) {
        this.segment = other.segment;
        this.utf8 = other.utf8;
        this.offset = other.offset;
        this.len = other.len;
    }

    public void slice(long offset, int len) {
        this.offset = offset;
        this.len = len;
    }

    public void wrapSegment(MemorySegment segment, long offset, int len) {
        this.segment = segment;
        this.utf8 = null;
        this.offset = offset;
        this.len = len;
    }

    public void wrapBuffer(ByteBuffer buffer) {
        segment = MemorySegment.ofBuffer(buffer);
        utf8 = null;
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
        return segment.get(JAVA_BYTE, offset+i);
    }

    @Override public byte[] copy(int begin, int end, byte[] dest, int offset) {
        int rLen = rangeLen(begin, end);
        MemorySegment.copy(segment, JAVA_BYTE, this.offset + begin, dest,
                offset, rLen);
        return dest;
    }

    @Override public int write(OutputStream out) throws IOException {
        if (utf8 != null) {
            out.write(utf8, (int)offset, len);
            return len;
        }
        return super.write(out);
    }

    @Override public SegmentRope sub(int begin, int end) {
        int rLen = rangeLen(begin, end);
        return rLen == len ? this : new SegmentRope(segment, offset + begin, rLen);
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
            end += B_LEN-1; // the while above always overdraws from end
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

    static boolean has(MemorySegment left, long pos, MemorySegment right, long begin, int rLen) {
        long end = begin+rLen;
//        if (left != right)
//            return mismatch(left, pos, pos+ rLen, right, begin, end) < 0;
        // else: manually compare since due to JDK-8306866

        if (rLen > B_LEN) {
            for (long ve = pos+B_SP.loopBound(rLen); pos < ve; pos += B_LEN, begin += B_LEN) {
                ByteVector l = fromMemorySegment(B_SP,  left, pos,   LITTLE_ENDIAN);
                ByteVector r = fromMemorySegment(B_SP, right, begin, LITTLE_ENDIAN);
                if (!l.eq(r).allTrue()) return false;
            }
            rLen = (int)(end-begin);
        }

        Object lBase =  left.array().orElse(null);
        Object rBase = right.array().orElse(null);
        pos += left.address() + (lBase == null ? 0 : U8_UNSAFE_BASE);
        begin += right.address() + (rBase == null ? 0 : U8_UNSAFE_BASE);
        end = begin + rLen;
        for (; begin < end && U.getByte(lBase, pos) == U.getByte(rBase, begin); ++begin)
            ++pos;
        return begin == end;

//        return compareTo(left, pos, rLen, right, begin, rLen) == 0;

//        while (begin < end) {
//            if (left.get(JAVA_BYTE, pos++) != right.get(JAVA_BYTE, begin++)) return false;
//        }
//        return true;
    }

    @Override public boolean has(int pos, Rope rope, int begin, int end) {
        int rLen = end - begin;
        if (pos < 0 || rLen < 0) throw new IndexOutOfBoundsException();
        if (pos + rLen > len) return false;

        long i = pos+offset;
        MemorySegment seg = this.segment;
        if (rope instanceof TwoSegmentRope t) {
            return t.has(begin, this, pos, pos+rLen);
        } else if (rope instanceof SegmentRope s) {
            return has(seg, i, s.segment, s.offset+begin, rLen);
        } else {
            while (begin < end) {
                if (seg.get(JAVA_BYTE, i++) != rope.get(begin++)) return false;
            }
        }
        return true;
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
        while (bit < bits)
            h |= (s.get(JAVA_BYTE, physBegin + bit) & 1) << bit++;
        return h;
    }

    static int hashCode(int h, MemorySegment segment, long begin, long end) {
        for (long i = begin; i < end; i++)
            h = 31 * h + segment.get(JAVA_BYTE, i);
        return h;
    }

    @Override public int hashCode() {
        return hashCode(0, segment, offset, offset+len);
    }

    @Override public @NonNull String toString() {
        if (len == 0) return "";
        if (utf8 != null)
            return new String(utf8, (int)offset, len, UTF_8);
        return super.toString();
    }

    @Override public String toString(int begin, int end) {
        if (utf8 != null)
            return new String(utf8, (int)offset+begin, end-begin, UTF_8);
        return super.toString(begin, end);
    }

    public static int cmp(Object lBase, long lOff, int lLen,
                          Object rBase, long rOff, int rLen) {
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

    public static int compareTo(MemorySegment left, long lOff, int lLen,
                                MemorySegment right, long rOff, int rLen) {
        if (U != null) {
            Object lBase = left.array().orElse(null);
            Object rBase = right.array().orElse(null);
            lOff +=  left.address() + (lBase == null ? 0 : U8_UNSAFE_BASE);
            rOff += right.address() + (rBase == null ? 0 : U8_UNSAFE_BASE);
            return cmp(lBase, lOff, lLen, rBase, rOff, rLen);
        }
        //vectorization helps, but is slower than JAVA_INT_UNALIGNED. Using Vector.lane() is
        // too slow. Best vector implementation consisted of left.compare(NE, right).firstTrue()
        // followed by JAVA_BYTE scalar accesses. ByteVector.sub() cannot be used as byte overflows
        // render the comparison invalid.

        // On DictFindBench, reading ints is faster than reading longs. likely because most
        // calls will mismatch in the first 4 bytes for CompositeDict.

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
        return compareTo(segment, offset, len, o.segment, o.offset, o.len);
    }
    public final int compareTo(TwoSegmentRope o) {
        MemorySegment seg = this.segment;
        // compare all we can on the left side with all we can of the right side
        long offset = this.offset;
        int fstLen = o.fstLen, len = Math.min(this.len, fstLen);
        int diff = compareTo(seg, offset, len, o.fst, o.fstOff, len);
        if (diff != 0) return diff;
        if (fstLen > len)
            return -1; // left side exhausted before right side

        // update [offset, offset+len) range
        offset += len;
        len = this.len-len;

        // compare whatever remains on the left side with the second segment of o
        return compareTo(seg, offset, len, o.snd, o.sndOff, o.sndLen);
    }

    @Override public int compareTo(SegmentRope o, int begin, int end) {
        return compareTo(segment, offset, len, o.segment, o.offset+begin, end-begin);
    }

    @Override public int compareTo(TwoSegmentRope o, int begin, int end) {
        MemorySegment seg = this.segment;
        int fstLen = o.fstLen, oLen = fstLen-begin, len = Math.min(this.len, oLen);
        long offset = this.offset;
        if (len > 0) { // compare this vs fst segment
            int diff = compareTo(seg, offset, len, o.fst, o.fstOff+begin, len);
            if (diff != 0) return diff;
        }
        if (oLen > len)
            return -1; // left side is already exhausted, right is larger.

        // advance [offset, offset+len) on our side
        offset += len;
        len = this.len-len;
        // make [begin, end) relative to o.snd
        if ((end -= fstLen) < 0)
            return len; // o is exhausted, will return 0 if equal, >0 if this is larger
        begin = Math.max(0, begin-fstLen);
        oLen = end-begin;

        // compare [offset, offset+len) and [begin, end) unlike earlier, length may differ
        return compareTo(seg, offset, len, o.snd, o.sndOff+begin, oLen);
    }
}