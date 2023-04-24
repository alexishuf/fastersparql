package com.github.alexishuf.fastersparql.model.rope;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.foreign.MemorySegment.mismatch;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static jdk.incubator.vector.ByteVector.fromMemorySegment;

public class SegmentRope extends Rope {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_LEN = B_SP.length();

    protected int offset;
    protected byte @Nullable [] utf8;
    protected MemorySegment segment;

    public SegmentRope() {
        this(ByteRope.EMPTY_SEGMENT, 0, 0);
    }
    public SegmentRope(ByteBuffer buffer) {
        this(MemorySegment.ofBuffer(buffer), 0, buffer.remaining());
    }
    public SegmentRope(MemorySegment segment, int offset, int len) {
        super(len);
        if (offset < 0 || len < 0)
            throw new IllegalArgumentException("Negative offset/len");
        if (offset+len > segment.byteSize())
            throw new IndexOutOfBoundsException("offset+len > segment.byteSize");
        this.offset = offset;
        this.segment = segment;
        this.utf8 = segment.byteSize() == 0 ? ByteRope.EMPTY_UTF8 : null;
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
    public int               offset()             { return offset; }
    public byte @Nullable [] backingArray()       { return utf8; }
    @Override public int     backingArrayOffset() { return offset; }

    public void wrapBuffer(ByteBuffer buffer) {
        segment = MemorySegment.ofBuffer(buffer);
        offset = 0;
        long size = segment.byteSize();
        if (size > Integer.MAX_VALUE) throw new IllegalArgumentException("buffer is too big");
        len = (int) size;
    }

    public void wrapEmptyBuffer() {
        segment = ByteRope.EMPTY_SEGMENT;
        offset = 0;
        len = 0;
    }

    @Override public byte get(int i) {
        if (i < 0 || i >= len) throw new IndexOutOfBoundsException(i);
        return segment.get(JAVA_BYTE, offset + i);
    }

    @Override public byte[] copy(int begin, int end, byte[] dest, int offset) {
        int rLen = rangeLen(begin, end);
        MemorySegment.copy(segment, JAVA_BYTE, this.offset + begin, dest,
                offset, rLen);
        return dest;
    }

    @Override public int write(OutputStream out) throws IOException {
        if (utf8 != null) {
            out.write(utf8, offset, len);
        } else {
            byte[] buf = new byte[128];
            for (int i = offset, end = i + len; i < end; i += 128) {
                int n = Math.min(end - i, 128);
                MemorySegment.copy(segment, JAVA_BYTE, i, buf, 0, n);
                out.write(buf, 0, n);
            }
        }
        return len;
    }

    @Override public SegmentRope sub(int begin, int end) {
        int rLen = rangeLen(begin, end);
        return rLen == len ? this : new SegmentRope(segment, offset + begin, rLen);
    }

    @Override public int skipUntil(int begin, int end, char c0) {
        int rLen = rangeLen(begin, end), offset = this.offset;
        begin += offset;
        end += offset;
        Vector<Byte> c0Vec = B_SP.broadcast(c0);
        MemorySegment segment = this.segment;
        for (int e = begin + B_SP.loopBound(rLen); begin < e; begin += B_LEN) {
            int lane = fromMemorySegment(B_SP, segment, begin, LITTLE_ENDIAN).eq(c0Vec).firstTrue();
            if (lane < B_LEN) return begin - offset + lane;
        }
        while (begin < end && segment.get(JAVA_BYTE, begin) != c0) ++begin;
        return begin - offset;
    }

    @Override public int skipUntil(int begin, int end, char c0, char c1) {
        int rLen = rangeLen(begin, end), offset = this.offset;
        begin += offset;
        end += offset;
        Vector<Byte> c0Vec = B_SP.broadcast(c0);
        Vector<Byte> c1Vec = B_SP.broadcast(c1);
        MemorySegment segment = this.segment;
        for (int e = begin + B_SP.loopBound(rLen); begin < e; begin += B_LEN) {
            ByteVector vec = fromMemorySegment(B_SP, segment, begin, LITTLE_ENDIAN);
            int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).firstTrue();
            if (lane < B_LEN) return begin - offset + lane;
        }
        for (byte c; begin < end && (c=segment.get(JAVA_BYTE, begin)) != c0 && c != c1;) ++begin;
        return begin - offset;
    }

    @Override public int skipUntilLast(int begin, int end, char c0) {
        rangeLen(begin, end); // checks range
        int offset = this.offset;
        begin += offset;
        end += offset;
        Vector<Byte> c0Vec = B_SP.broadcast(c0);
        MemorySegment segment = this.segment;
        int i = end;
        while ((i -= B_LEN) >= begin) {
            int lane = fromMemorySegment(B_SP, segment, i, LITTLE_ENDIAN).eq(c0Vec).lastTrue();
            if (lane >= 0) return i - offset + lane;
        }
        i += B_LEN-1; // the while above always overdraws from i
        for (; i >= begin; --i) {
            if (segment.get(JAVA_BYTE, i) == c0) return i-offset;
        }
        return end-offset;
    }

    @Override public int skipUntilLast(int begin, int end, char c0, char c1) {
        rangeLen(begin, end); // check range
        int offset = this.offset;
        begin += offset;
        end += offset;
        Vector<Byte> c0Vec = B_SP.broadcast(c0);
        Vector<Byte> c1Vec = B_SP.broadcast(c1);
        MemorySegment segment = this.segment;
        int i = end;
        while ((i -= B_LEN) >= begin) {
            ByteVector vec = fromMemorySegment(B_SP, segment, i, LITTLE_ENDIAN);
            int lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).lastTrue();
            if (lane >= 0) return i - offset + lane;
        }
        i += B_LEN-1; // the while above always overdraws from i
        for (byte c; i >= begin; --i) {
            if ((c=segment.get(JAVA_BYTE, i)) == c0 || c == c1) return i-offset;
        }
        return end-offset;
    }

    private boolean isEscapedPhys(int begin, int i) {
        int not = i - 1;
        while (not >= begin && segment.get(JAVA_BYTE, not) == '\\') --not;
        return ((i - not) & 1) == 0;
    }

    @Override public boolean isEscaped(int i) { return isEscapedPhys(0, i+offset); }
    @Override public boolean isEscaped(int begin, int i) {
        return isEscapedPhys(begin+offset, i+offset);
    }

    @Override public int skipUntilUnescaped(int begin, int end, char c) {
        int rLen = rangeLen(begin, end), offset = this.offset, i = begin + offset, e;
        MemorySegment segment = this.segment;
        var cVec = B_SP.broadcast(c);
        for (e = i + B_SP.loopBound(rLen); i < e; i += B_LEN) {
            long found = fromMemorySegment(B_SP, segment, i, LITTLE_ENDIAN).eq(cVec).toLong();
            for (int lane = 0; (lane+=numberOfTrailingZeros(found>>>lane)) < 64; ++lane)
                if (!isEscapedPhys(begin, i+lane)) return i+lane-offset;
        }
        for (e = offset + end; i < e; ++i) {
            byte v = segment.get(JAVA_BYTE, i);
            if (v == '\\') ++i; // skip next byte
            else if (v == c) return i-offset;
        }
        return i-offset;
    }

    @Override public int skip(int begin, int end, int[] alphabet) {
        rangeLen(begin, end); // checks bounds
        int offset = this.offset;
        boolean stopOnNonAscii = (alphabet[3] & 0x80000000) == 0;
        for (begin += offset, end += offset; begin < end; ++begin) {
            byte c = segment.get(JAVA_BYTE, begin);
            if (c >= 0) { // c is ASCII
                if ((alphabet[c >> 5] & (1 << c)) == 0)
                    break; // c is not in alphabet
            } else if (stopOnNonAscii) {
                break; // non-ASCII  not allowed by alphabet
            }
        }
        return begin-offset;
    }

    @Override public int skipWS(int begin, int end) {
        rangeLen(begin, end); // check bounds
        int offset = this.offset;
        begin += offset;
        end += offset;
        for (byte c; begin < end && (c = segment.get(JAVA_BYTE, begin)) <= ' ' && c >= 0; ) ++begin;
        return begin - offset;
    }

    @Override public int reverseSkipUntil(int begin, int end, char c) {
        rangeLen(begin, end); // check bounds
        int offset = this.offset;
        begin += offset;
        end += offset - 1;
        while (end >= begin && segment.get(JAVA_BYTE, end) != c) --end;
        return Math.max(begin, end) - offset;
    }

    @Override public boolean has(int position, byte[] seq) {
        if (position < 0) throw new IndexOutOfBoundsException();
        if (position + seq.length > len) return false;
        position += offset;
        for (byte c : seq) {
            if (c != segment.get(JAVA_BYTE, position++)) return false;
        }
        return true;
    }

    @Override public boolean has(int pos, Rope rope, int begin, int end) {
        int rLen = end - begin;
        if (pos < 0 || rLen < 0) throw new IndexOutOfBoundsException();
        if (pos + rLen > len) return false;

        int i = pos+offset, e = i+rLen;
        MemorySegment seg = this.segment;
        if (rope instanceof SegmentRope s) {
            MemorySegment oSeg = s.segment;
            begin += s.offset;
            if (oSeg != seg)
                return mismatch(seg, i, e, oSeg, begin, begin+rLen) < 0;
            // manually compare since MemorySegment.mismatch() on JDK 20 will assume
            // any two ranges in the same segment to be equal.
            for (int ve = i+B_SP.loopBound(rLen); i < ve; i += B_LEN, begin += B_LEN) {
                ByteVector v = fromMemorySegment(B_SP,  seg,     i, LITTLE_ENDIAN);
                ByteVector u = fromMemorySegment(B_SP, oSeg, begin, LITTLE_ENDIAN);
                if (!v.eq(u).allTrue()) return false;
            }
            for (; i < e && seg.get(JAVA_BYTE, i) == oSeg.get(JAVA_BYTE, begin); ++i)
                ++begin;
            return i == e;
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

        MemorySegment segment = this.segment;
        int phys = position + offset;
        for (byte expected : uppercaseSequence) {
            byte actual = segment.get(JAVA_BYTE, phys++);
            if (actual != expected && ((actual < 'a' || actual > 'z') || actual - 32 != expected))
                return false;
        }
        return true;
    }

    //    private static final long LSB_MASK = 0x0101010101010101L;
    @Override public int lsbHash(int begin, int end) {
        int bits = rangeLen(begin, end), h = 0, bit = 0;
        if (bits > 32)
            begin = end - (bits = 32);
        begin += offset;
        MemorySegment s = this.segment;
//        for (; bit+8 < bits; bit += 8)
//            h |= Long.compress(s.get(JAVA_LONG_UNALIGNED, begin+bit), LSB_MASK) << bit;
        while (bit < bits)
            h |= (s.get(JAVA_BYTE, begin + bit) & 1) << bit++;
        return h;
    }

    @Override public int hashCode() {
        int h = 0;
        MemorySegment segment = this.segment;
        for (int i = offset, end = i + len; i < end; i++)
            h = 31 * h + segment.get(JAVA_BYTE, i);
        return h;
    }

    @Override public int compareTo(@NonNull Rope o) {
        MemorySegment segment = this.segment;
        int common = Math.min(len, o.len), i = 0, offset = this.offset, diff = 0;
        while (i < common && (diff = segment.get(JAVA_BYTE, i+offset) - o.get(i)) == 0) ++i;
        return diff == 0 ? len - o.len : diff;
    }

    @Override public int compareTo(Rope o, int begin, int end) {
        MemorySegment segment = this.segment;
        int oLen = end - begin, common = Math.min(len, oLen), offset = this.offset, i = 0, diff = 0;
        while (i < common && (diff = segment.get(JAVA_BYTE, i+offset) - o.get(begin++)) == 0)
            ++i;
        return diff == 0 ? len - oLen : diff;
    }
}