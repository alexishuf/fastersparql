package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.concurrent.AffinityShallowPool;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.*;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Collection;

import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOf;
import static jdk.incubator.vector.ByteVector.fromArray;

@SuppressWarnings("UnusedReturnValue")
public final class ByteRope extends SegmentRope implements ByteSink<ByteRope, ByteRope> {
    private static final int POOL_COL = AffinityShallowPool.reserveColumn();
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_LEN = B_SP.length();
    private static final int READLINE_CHUNK = 128;

    static final byte[] EMPTY_UTF8 = new byte[0];
    static final MemorySegment EMPTY_SEGMENT = MemorySegment.ofArray(EMPTY_UTF8);

    public static final ByteRope EMPTY = new ByteRope(EMPTY_UTF8);
    public static final ByteRope A = new ByteRope(new byte[]{'a'});
    public static final ByteRope DQ = new ByteRope(new byte[]{'"'});
    public static final ByteRope GT = new ByteRope(new byte[]{'>'});
    public static final ByteRope DT_MID = new ByteRope(new byte[]{'"', '^', '^'});
    public static final ByteRope DT_MID_LT = new ByteRope(new byte[]{'"', '^', '^', '<'});


    /** Create an empty {@link ByteRope} */
    public ByteRope() { super(EMPTY_SEGMENT, 0, 0); }

    /** Create an empty {@link ByteRope} backed by an array of the given capacity */
    public ByteRope(int capacity) { super(new byte[capacity], 0, 0); }

    /** Copy the UTF-8 of {@code r}. */
    public ByteRope(Rope r) { super(r.toArray(0, r.len), 0, r.len); }

    private static byte[] asArray(Object o) {
        return switch (o) {
            case null -> EMPTY_UTF8;
            case Rope r -> r.toArray(0, r.len);
            case byte[] a -> a;
            default -> o.toString().getBytes(UTF_8);
        };
    }

    /** Copy the UTF-8 of {@code o}. */
    public ByteRope(Object o) { this(asArray(o)); }

    /** Equivalent to {@code ByteRope(utf8, 0, utf8.length)} */
    public ByteRope(byte[] utf8) { super(utf8, 0, utf8.length); }

    /**
     * Create a ByteRope backed by {@code len} bytes starting at {@code utf8[offset]}.
     *
     * <p>{@code utf8} is kept by reference. Therefore, changes to its content will be
     * visible in the {@link ByteRope} instance. Use {@link #ByteRope(Object)}
     * for a copying constructor.</p>
     *
     * @param utf8 backing array (kept by reference) with the UTF-8 string
     * @param offset start of the UTF-8 string in {@code utf8}
     * @param len length (in bytes) of the UTF-8 string in {@code utf8}.
     */
    public ByteRope(byte[] utf8, int offset, int len) { super(utf8, offset, len); }

    public static ByteRope pooled(int capacity) {
        ByteRope br = AffinityShallowPool.get(POOL_COL);
        byte[] u8 = br == null ? null : br.utf8;
        if (u8 == null || u8.length < capacity) {
            u8 = ArrayPool.bytesAtLeast(capacity, u8);
            if (br == null) return new ByteRope(u8);
        }
        br.utf8    = u8;
        br.segment = MemorySegment.ofArray(u8);
        br.len     = 0;
        return br;
    }

    @Override public void recycle() {
        if (offset != 0 || this == EMPTY || utf8 == null) raiseImmutable();
        len = 0;
        if (AffinityShallowPool.offer(POOL_COL, this) != null) {
            ArrayPool.BYTE.offer(utf8, utf8.length);
            wrapEmptyBuffer();
        }
    }

    public void recycleUtf8() {
        byte[] utf8 = this.utf8;
        if (offset != 0 || this == EMPTY || utf8 == null) raiseImmutable();
        len = 0;
        if (ArrayPool.BYTE.offer(utf8, utf8.length) == null)
            wrapEmptyBuffer();
    }

    public byte @NonNull[] u8() {//noinspection DataFlowIssue
        return utf8;
    }

    @Override public boolean isEmpty() { return super.isEmpty(); }

    private void raiseImmutable() {
        throw new UnsupportedOperationException("Mutating ByteRope over shared byte[] utf8");
    }

    private int postIncLen(int increment) {
        ensureFreeCapacity(increment);
        int len = this.len;
        this.len += increment;
        return len;
    }

    @Override public ByteRope take() {
        var copy = new ByteRope(toArray(0, len));
        len = 0;
        return copy;
    }

    @Override public @This ByteRope ensureFreeCapacity(int increment) {
        byte[] u8 = utf8;
        if (offset != 0 || this == EMPTY || u8 == null) raiseImmutable();
        int len = this.len, required = len+increment;
        if (required > u8.length) {
            utf8 = u8 = Arrays.copyOf(u8, Math.max(required, u8.length+(u8.length>>1)));
            segment = MemorySegment.ofArray(u8);
        }
        return this;
    }

    @SuppressWarnings("unused") public @This ByteRope ensureCapacity(int capacity) {
        ensureFreeCapacity(capacity-len);
        return this;
    }

    /** Get {@code this.utf8} or a copy with {@code length == this.len}. */
    public byte[] fitBytes() {
        byte[] utf8 = u8();
        return utf8.length == len ? utf8 : copyOf(utf8, len);
    }


    public @This ByteRope clear() {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        len = 0;
        return this;
    }

    public @This ByteRope replace(char c, char r) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        int i = 0;
        Vector<Byte> cVec = B_SP.broadcast(c);
        byte[] utf8 = u8();
        for (int j, e = B_SP.loopBound(len); i < e; i += B_SP.length()) {
            for (var vec = fromArray(B_SP, utf8, i); (j = vec.eq(cVec).firstTrue()) != B_LEN; )
                vec = vec.withLane(j, utf8[i+j] = (byte) r);
        }
        for (int e = len; i < e; ++i)
            if (utf8[i] == c) utf8[i] = (byte) r;
        return this;
    }

    public @This ByteRope erase(int begin, int end) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        if (end < len) {
            byte[] utf8 = u8();
            arraycopy(utf8, end, utf8, begin, len-end);
        }
        len -= Math.min(len, end-begin);
        return this;
    }

//    /**
//     * Read as many bytes as possible from {@code in} and stop if there is at least one
//     * {@code mark} within the read bytes. If EOF is reached for {@code in}, insert a
//     * {@code mark} if it did not end in {@code mark}.
//     *
//     * @return the number of bytes read.
//     */
//    public int eagerReadUntilEnforced(InputStream in, char mark) throws IOException {
//        int sum = 0, n;
//        while ((n = in.read(utf8, len, utf8.length-len)) != -1) {
//            sum += n;
//            int i = len, e = len += n;
//            if (skipUntil(i, e, mark) < e) break;
//            if (e == utf8.length) ensureFreeCapacity(Math.min(4096, e));
//        }
//        if (n == -1 && utf8[len-1] != mark)
//            append(mark);
//        return sum;
//    }

    /** Analogous to {@link BufferedReader#readLine()}. */
    public boolean readLine(InputStream in) throws IOException {
        boolean got = false;
        ensureFreeCapacity(READLINE_CHUNK);
        if (in instanceof BufferedInputStream) {
            in.mark(READLINE_CHUNK);
            for (int n; (n = in.read(u8(), len, READLINE_CHUNK)) != -1; ) {
                got = true;
                int begin = len, eol;
                long breakLenAndIndex = skipUntilLineBreak(begin, len += n);
                if ((eol = (int)breakLenAndIndex) < len) {
                    in.reset();
                    //noinspection ResultOfMethodCallIgnored
                    in.read(utf8, begin, (len = eol) + (int)(breakLenAndIndex>>>32) - begin);
                    break;
                } else {
                    in.mark(READLINE_CHUNK);
                    ensureFreeCapacity(READLINE_CHUNK);
                }
            }

        } else {
            int sum = 0, c;
            for (int free = u8().length-len; (c = in.read()) != -1 && c != '\n'; --free, ++sum) {
                if (free-- == 0) {
                    int newLength = utf8.length + (utf8.length >> 1);
                    free = (utf8 = copyOf(utf8, newLength)).length - len;
                    segment = null;
                }
                utf8[len++] = (byte) c;
            }
            if (sum > 0 && utf8[len-1] == '\r')
                --len; // unget() '\r'
            got = sum > 0 || c == '\n';
        }
        if (segment == null)
            segment = MemorySegment.ofArray(utf8);
        return got;
    }

    public OutputStream asOutputStream() {
        return new OutputStream() {
            @Override public void write(int b) { append((byte) b); }
            @Override public void write(byte @NonNull [] b) { append(b); }
            @Override public void write(byte @NonNull [] b, int off, int len) { append(b, off, len); }
        };
    }

    public @This ByteRope fill(int value) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        Arrays.fill(u8(), 0, len, (byte)value);
        return this;
    }

    @Override public @This ByteRope append(MemorySegment segment, byte @Nullable [] array,
                                           long offset, int len) {
        int out = postIncLen(len);
        if (U != null) {
            if (offset + len > segment.byteSize())
                throw new IndexOutOfBoundsException("offset:len ranges violates [0, segment.byteSize()] bounds");
            if (array == null && !segment.isNative())
                throw new IllegalArgumentException("byte[] not given for heap segment");
            U.copyMemory(array, segment.address() + offset + (array == null ? 0 : U8_BASE), this.utf8, U8_BASE + out, len);
        } else {
            MemorySegment.copy(segment, offset, this.segment, out, len);
        }
        return this;
    }

    @Override public @This ByteRope append(byte[] utf8, int offset, int len) {
        int out = postIncLen(len);
        arraycopy(utf8, offset, u8(), out, len);
        return this;
    }

    @Override public @This ByteRope append(Rope r, int begin, int end) {
        int out = postIncLen(max(0, end-begin));
        r.copy(begin, end, utf8, out);
        return this;
    }

    @Override public @This ByteRope append(byte c) {
        ensureFreeCapacity(1);
        u8()[len++] = c;
        return this;
    }

    @Override public @This ByteRope append(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r) return append(r, begin, end);

        ensureFreeCapacity(end-begin);
        for (; begin < end; ++begin) {
            char c = cs.charAt(begin);
            if (Character.isSurrogate(c))
                return encoderAppend(cs, begin, end);
            appendCodePoint(c);
        }
        return this;
    }

    private @This ByteRope encoderAppend(CharSequence cs, int begin, int end) {
        var enc = UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        var cb = CharBuffer.wrap(cs, begin, end);
        CoderResult result;
        do {
            ensureFreeCapacity(cb.remaining()*4);
            byte[] utf8 = u8();
            var bb = ByteBuffer.wrap(utf8, len, utf8.length - len);
            result = enc.encode(cb, bb, true);
            len = bb.position();
        } while (result.isOverflow());
        return this;
    }

    public @This ByteRope append(Collection<?> coll) {
        append('[');
        boolean first = true;
        for (Object o : coll) {
            if (first) first = false;
            else       append(',').append(' ');
            append(o);
        }
        append(']');
        return this;
    }

    public @This ByteRope append(Object o) {
        switch (o) {
            case Rope          r -> append(r);
            case byte[]        a -> append(a, 0, a.length);
            case Collection<?> c -> append(c);
            case Object[]      a -> append(Arrays.asList(a));
            case    int[]      a -> append(Arrays.toString(a));
            case   long[]      a -> append(Arrays.toString(a));
            case double[]      a -> append(Arrays.toString(a));
            case  float[]      a -> append(Arrays.toString(a));
            case  short[]      a -> append(Arrays.toString(a));
            case null            -> {}
            default              -> append(o.toString().getBytes(UTF_8));
        }
        return this;
    }

    public @This ByteRope append(long n) {
        if (n == 0)
            return append('0');
        else if (n == Long.MIN_VALUE)
            return append(Long.toString(n));
        int sign = (int) (n >>> 63);
        n = Math.abs(n);
        postIncLen(sign + (int) Math.floor(Math.log10(n) + 1));
        int i = len;
        byte[] utf8 = u8();
        for (long rem = n%10; n  > 0; n = n/10, rem = n%10)
            utf8[--i] = (byte)('0' + rem);
        if (sign == 1)
            utf8[--i] = '-';
        return this;
    }

    @Override public @This ByteRope repeat(byte c, int n) {
        int out = postIncLen(n);
        byte[] utf8 = u8();
        while (n-- > 0) utf8[out++] = c;
        return this;
    }

    public @This ByteRope appendEscapingLF(Object o) {
        Rope r = Rope.of(o);
        ensureFreeCapacity(r.len()+8);
        for (int consumed = 0, i, end = r.len(); consumed < end; consumed = i+1) {
            i = r.skipUntil(consumed, end, '\n');
            append(r, consumed, i);
            if (i < end)
                append('\\').append('n');
        }
        return this;
    }

    public @This ByteRope unAppend(int n) {
        if (n > len) throw new IllegalArgumentException("n ("+n+") > size ("+ len +")");
        len -= n;
        return this;
    }
}
