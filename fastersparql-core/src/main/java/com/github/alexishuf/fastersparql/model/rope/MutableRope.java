package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import jdk.incubator.vector.Vector;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.jetbrains.annotations.Contract;

import java.io.*;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.B_LEN;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.B_SP;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static jdk.incubator.vector.ByteVector.fromArray;

@SuppressWarnings({"UnusedReturnValue", "resource"})
public sealed class MutableRope extends SegmentRope
        implements ByteSink<MutableRope, FinalSegmentRope> permits PooledMutableRope0 {
    public static final int BYTES = 16 + 2*4 + 8+2*4 + 2*4;
    private static final int READLINE_CHUNK = 128;

    private Bytes bytes;

    /**
     * Create a new {@link MutableRope} backed by the given {@code bytes} {@link Bytes}
     * object and set the {@link MutableRope} length to {@code len}.
     */
    public MutableRope(Orphan<Bytes> bytes, int len) {
        super();
        setBytesAndLeakPrevious(bytes.takeOwnership(this), len);
    }

    /** Create an empty {@link MutableRope} */
    protected MutableRope() {
        super();
        bytes = Bytes.EMPTY;
    }

    /** Create an empty {@link MutableRope} backed by an array of the given capacity */
    public MutableRope(int capacity) { this(Bytes.atLeast(capacity), 0); }


    @Override public void close() {
        if (bytes != Bytes.EMPTY) {
            bytes.recycle(this);
            setBytesAndLeakPrevious(Bytes.EMPTY, 0);
        }
    }

    private void setBytesAndLeakPrevious(Bytes bytes, int len) {
        this.bytes   = bytes;
        this.segment = bytes.segment;
        this.utf8    = bytes.arr;
        this.offset  = 0;
        this.len     = len;
    }

    @SuppressWarnings("DataFlowIssue") public byte @NonNull[] u8() {return utf8;}

    @Override public boolean isEmpty() { return super.isEmpty(); }

    @Override public SegmentRope sub(int begin, int end) {
        return FinalSegmentRope.asFinal(this, begin, end);
    }

    @Override public FinalSegmentRope take() {return FinalSegmentRope.asFinal(this);}

    @Override public FinalSegmentRope takeUntil(int len) {
        if (len == 0)
            return EMPTY;
        byte[] u8 = u8();
        int suffixLen = this.len-len;
        if (suffixLen < 0)
            throw new IndexOutOfBoundsException(len);
        var copy = FinalSegmentRope.asFinal(this, 0, len);
        this.len = suffixLen;
        if (suffixLen > 0)
            arraycopy(u8, len, bytes.arr, 0, suffixLen);
        return copy;
    }

    private int postIncLen(int increment) {
        ensureFreeCapacity(increment);
        int len = this.len;
        this.len += increment;
        return len;
    }

    @Contract("_ -> this") @Override public @This MutableRope ensureFreeCapacity(int increment) {
        int len = this.len, required = len+increment;
        byte[] utf8 = u8();
        if (required > utf8.length)
            grow(required, utf8, len);
        return this;
    }

    private void grow(int required, byte[] utf8, int len) {
        int newLen = max(required, utf8.length + (utf8.length >> 1));
        setBytesAndLeakPrevious(Bytes.grow(bytes, this, len, newLen), len);
    }

    @SuppressWarnings("unused") public @This MutableRope ensureCapacity(int capacity) {
        ensureFreeCapacity(capacity-len);
        return this;
    }

    @Override public int freeCapacity() {
        byte[] u8 = utf8;
        return u8 == null ? 0 : u8.length;
    }

    public @This MutableRope clear() {
        len = 0;
        return this;
    }

    public @This MutableRope replace(char c, char r) {
        byte[] utf8 = Objects.requireNonNull(this.utf8);
        int i = 0;
        if (LowLevelHelper.ENABLE_VEC && len > B_LEN) {
            Vector<Byte> cVec = B_SP.broadcast(c);
            for (int j, e = B_SP.loopBound(len); i < e; i += B_SP.length()) {
                for (var vec = fromArray(B_SP, utf8, i); (j = vec.eq(cVec).firstTrue()) != B_LEN; )
                    vec = vec.withLane(j, utf8[i + j] = (byte) r);
            }
        }
        for (int e = len; i < e; ++i)
            if (utf8[i] == c) utf8[i] = (byte) r;
        return this;
    }

    public @This MutableRope erase(int begin, int end) {
        byte[] utf8 = u8();
        if (end < len)
            arraycopy(utf8, end, utf8, begin, len-end);
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
        byte[] u8 = u8();
        if (in instanceof BufferedInputStream) {
            in.mark(READLINE_CHUNK);
            for (int n; (n = in.read(u8, len, READLINE_CHUNK)) != -1; ) {
                got = true;
                int begin = len, eol;
                long breakLenAndIndex = skipUntilLineBreak(begin, len += n);
                if ((eol = (int)breakLenAndIndex) < len) {
                    in.reset();
                    //noinspection ResultOfMethodCallIgnored
                    in.read(u8, begin, (len = eol) + (int)(breakLenAndIndex>>>32) - begin);
                    break;
                } else {
                    in.mark(READLINE_CHUNK);
                    ensureFreeCapacity(READLINE_CHUNK);
                    u8 = u8();
                }
            }
        } else {
            int sum = 0, c;
            for (int free = u8.length-len; (c = in.read()) != -1 && c != '\n'; --free, ++sum) {
                if (free-- == 0) {
                    ensureFreeCapacity(len>>1);
                    free = (u8 = u8()).length - len;
                }
                u8[len++] = (byte) c;
            }
            if (sum > 0 && u8[len-1] == '\r')
                --len; // unget() '\r'
            got = sum > 0 || c == '\n';
        }
        return got;
    }

    public OutputStream asOutputStream() {
        return new OutputStream() {
            @Override public void write(int b) { append((byte) b); }
            @Override public void write(byte @NonNull [] b) { append(b); }
            @Override public void write(byte @NonNull [] b, int off, int len) { append(b, off, len); }
        };
    }

    public @This MutableRope fill(int value) {
        Arrays.fill(u8(), 0, len, (byte)value);
        return this;
    }

    @Override public @This MutableRope append(MemorySegment segment, byte @Nullable [] array,
                                              long offset, int len) {
        int out = postIncLen(len);
        if (LowLevelHelper.U != null) {
            if (offset + len > segment.byteSize())
                throw new IndexOutOfBoundsException("offset:len ranges violates [0, segment.byteSize()] bounds");
            if (array == null && !segment.isNative())
                throw new IllegalArgumentException("byte[] not given for heap segment");
            LowLevelHelper.U.copyMemory(array, segment.address() + offset + (array == null ? 0 : LowLevelHelper.U8_BASE), this.utf8, LowLevelHelper.U8_BASE + out, len);
        } else {
            MemorySegment.copy(segment, offset, this.segment, out, len);
        }
        return this;
    }

    @Override public @This MutableRope append(byte[] utf8, int offset, int len) {
        int out = postIncLen(len);
        arraycopy(utf8, offset, u8(), out, len);
        return this;
    }

    @Override public @This MutableRope append(Rope r, int begin, int end) {
        int out = postIncLen(max(0, end-begin));
        r.copy(begin, end, utf8, out);
        return this;
    }

    @Override public @This MutableRope append(SegmentRope r, int begin, int end) {
        int out = postIncLen(max(0, end-begin));
        r.copy(begin, end, utf8, out);
        return this;
    }

    @Override public @This MutableRope appendCharSequence(CharSequence cs, int begin, int end) {
        ensureFreeCapacity(RopeFactory.requiredBytes(cs, begin, end));
        len = RopeEncoder.charSequence2utf8(cs, begin, end, u8(), len);
        return this;
    }

    @Override public @This MutableRope appendCodePoint(int code) {
        ensureFreeCapacity(RopeFactory.requiredBytesByCodePoint(code));
        len = RopeEncoder.codePoint2utf8(code, u8(), len);
        return this;
    }

    @Override public @This MutableRope append(byte c) {
        ensureFreeCapacity(1);
        u8()[len++] = c;
        return this;
    }

    public @This MutableRope append(Collection<?> coll) {
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

    public @This MutableRope append(Object o) {
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

    public @This MutableRope append(long n) {
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

    @Override public @This MutableRope repeat(byte c, int n) {
        int out = postIncLen(n);
        byte[] utf8 = u8();
        while (n-- > 0) utf8[out++] = c;
        return this;
    }

    public @This MutableRope appendEscapingLF(Object o) {
        var r = Rope.asRope(o);
        ensureFreeCapacity(r.len()+8);
        for (int consumed = 0, i, end = r.len(); consumed < end; consumed = i+1) {
            i = r.skipUntil(consumed, end, '\n');
            append(r, consumed, i);
            if (i < end)
                append('\\').append('n');
        }
        return this;
    }

    public @This MutableRope unAppend(int n) {
        if (n > len)
            throw new IllegalArgumentException("n ("+n+") > size ("+ len +")");
        len -= n;
        return this;
    }
}
