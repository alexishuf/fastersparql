package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.Supplier;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

@MustCall("take")
public class RopeFactory {
    private static final int CHUNK_SIZE = 128;
    private static final int FULL_CHUNK = CHUNK_SIZE-32;
    private static final int BYTES = 16 + 6*4 + 20+CHUNK_SIZE;

    private static final Supplier<RopeFactory> FAC = new Supplier<>() {
        @Override public RopeFactory get() {return new RopeFactory();}
        @Override public String toString() {return "RopeFactory.FAC";}
    };
    private static final Alloc<RopeFactory> ALLOC = new Alloc<>(RopeFactory.class,
            "RopeFactory.ALLOC", Alloc.THREADS*32, FAC, BYTES);
    static {
        Primer.INSTANCE.sched(ALLOC::prime);
    }

    private MemorySegment chunkSegment;
    private byte[] chunk, dst;
    private int chunkPos, dstPos;
    private boolean live;

    private RopeFactory() {
        chunkSegment = MemorySegment.ofArray(dst = chunk = new byte[CHUNK_SIZE]);
    }

    /**
     * Get a {@link RopeFactory} with enough capacity for {@code bytes}.
     *
     * <p>Unlike {@link MutableRope}, {@link RopeFactory} will not dynamically grow its storage
     * as UTF-8 bytes are appended. Use {@link #requiredBytes(CharSequence, int, int)} and its
     * overloads to compute an adequate value for {@code bytes}. If only {@link Rope}s,
     * {@code byte} and {@code byte[]} are to be appended, {@link Rope#len}, and
     * {@code byte[].length} can be directly used to compute {@code required}.</p>
     *
     * <p><strong>Important</strong>: {@link #take()} MUST be called on the returned
     * {@link RopeFactory}.</p>
     *
     * @param bytes number of bytes of the new {@link FinalSegmentRope}.
     * @return a {@link RopeFactory}.
     */
    public static @Owning RopeFactory make(int bytes) {
        var fac = ALLOC.create();
        if (fac.live)
            throw new IllegalStateException("Got live RopeFactory from pool");
        fac.live = true;
        byte[] dst = fac.chunk;
        if (fac.chunkPos + bytes < dst.length) {
            fac.dstPos = fac.chunkPos;
        } else {
            if (bytes < CHUNK_SIZE && fac.chunk.length > FULL_CHUNK) {
                fac.chunk        = dst = new byte[CHUNK_SIZE];
                fac.chunkSegment = MemorySegment.ofArray(dst);
                fac.chunkPos     = 0;
            } else {
                dst = new byte[bytes];
            }
            fac.dstPos = 0;
        }
        fac.dst = dst;
        return fac;
    }

    /** How many UTF-8 bytes are required to encode {@code s} */
    public static int requiredBytes(String s) {
        return requiredBytesCS(s, 0, s.length());
    }
    /** How many UTF-8 bytes are required to encode {@code s} */
    public static int requiredBytes(StringBuilder s) {
        return requiredBytesCS(s, 0, s.length());
    }
    /** How many UTF-8 bytes are required to encode {@code r} */
    public static int requiredBytes(Rope r) { return r.len; }
    /** How many UTF-8 bytes are required to encode {@code r.sub(begin, end)} */
    public static int requiredBytes(Rope ignored, int begin, int end) { return end-begin; }
    /** How many UTF-8 bytes are required to encode the given Unicode code point */
    public static int requiredBytesByCodePoint(int codePoint) {
        if (codePoint < 0x000080) return 1;
        if (codePoint < 0x000800) return 2;
        if (codePoint < 0x010000) return 3;
        if (codePoint < 0x10FFFF) return 4;
        else                      return 3; // REPLACEMENT CHAR
    }
    /** How many UTF-8 bytes are required to encode {@code cs} */
    public static int requiredBytes(CharSequence cs) {
        if (cs instanceof Rope r) return r.len;
        return requiredBytesCS(cs, 0, cs.length());
    }
    /** How many UTF-8 bytes are required to encode {@code cs.subSequence(begin, end)} */
    public static int requiredBytes(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope) return end-begin;
        return requiredBytesCS(cs, begin, end);
    }

    private static int requiredBytesCS(CharSequence cs, int begin, int end) {
        int n = end-begin;
        for (int i = begin; i < end; i++) {
            char c = cs.charAt(i);
            if (c > 0x7f)
                n += c >= 0x800 && (c < MIN_SURROGATE || c > MAX_SURROGATE) ? 2 : 1;
        }
        return n;
    }

    /**
     * Gets a {@link FinalSegmentRope} with the result of all previous {@code add()} calls on
     * {@code this} and return {@code this} {@link RopeFactory} to the global pool.
     *
     * <p><strong>Important</strong>: There must be no call to any method in {@code this}
     * {@link RopeFactory} after this method is called, since the {@link RopeFactory} will return
     * to the pool and may be used by another thread.</p>
     *
     * @return a {@link FinalSegmentRope}.
     */
    public FinalSegmentRope take() {
        if (live) {
            MemorySegment seg = chunkSegment;
            int begin = 0, len = dstPos;
            byte[] dst = this.dst;
            if (dst == chunk) {
                begin    = chunkPos;
                chunkPos = len;
                len     -= begin;
            } else {
                this.dst    = chunk;
                this.dstPos = chunkPos;
                seg         = MemorySegment.ofArray(dst);
            }
            live = false;
            FinalSegmentRope rope;
            if (len > 1)
                rope = new FinalSegmentRope(seg, dst, begin, len);
            else if (len == 0)
                rope = FinalSegmentRope.EMPTY;
            else
                rope = SINGLE_CHAR_ROPES[dst[begin]];
            ALLOC.offer(this);
            return rope;
        } else {
            throw new IllegalStateException("duplicate/concurrent close()");
        }
    }
    private static final FinalSegmentRope[] SINGLE_CHAR_ROPES;
    static {
        byte[] chars = new byte[128];
        MemorySegment charsSegment = MemorySegment.ofArray(chars);
        for (int i = 0; i < chars.length; i++)
            chars[i] = (byte)i;
        FinalSegmentRope[] ropes = new FinalSegmentRope[128];
        for (int i = 0; i < ropes.length; i++)
            ropes[i] = new FinalSegmentRope(charsSegment, chars, i, 1);
        SINGLE_CHAR_ROPES = ropes;
    }

    public @This RopeFactory add(CharSequence cs) {
        if (cs instanceof Rope r)
            return add(r);
        dstPos = RopeEncoder.charSequence2utf8(cs, 0, cs.length(), dst, dstPos);
        return this;
    }

    public @This RopeFactory add(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r)
            return add(r, begin, end);
        dstPos = RopeEncoder.charSequence2utf8(cs, begin, end, dst, dstPos);
        return this;
    }

    public @This RopeFactory add(byte b) {
        dst[dstPos++] = b;
        return this;
    }

    public @This RopeFactory add(char c) {
        dstPos = RopeEncoder.char2utf8(c, dst, dstPos);
        return this;
    }

    public @This RopeFactory add(long n) {
        if (n >= 0 && n < 10) {
            dst[dstPos++] = (byte)('0'+n);
        } else if (n == Long.MIN_VALUE) {
            add(MIN_LONG_U8);
        } else {
            int sign = (int)(n>>>63), i = dstPos+sign+(int)Math.floor(Math.log10(n)+1);
            dstPos = i;
            n = Math.abs(n);
            for (long rem = n%10; n  > 0; n = n/10, rem = n%10)
                dst[--i] = (byte)('0' + rem);
            if (sign == 1)
                dst[--i] = '-';
        }
        return this;
    }
    private static final byte[] MIN_LONG_U8 = Long.toString(Long.MIN_VALUE).getBytes(UTF_8);

    public @This RopeFactory add(Rope r) {
        int len = r.len;
        r.copy(0, len, dst, dstPos);
        dstPos += len;
        return this;
    }

    public @This RopeFactory add(MutableRope r) {
        int len = r.len;
        arraycopy(r.u8(), (int)r.offset, dst, dstPos, len);
        dstPos += len;
        return this;
    }

    public @This RopeFactory add(Rope r, int begin, int end) {
        r.copy(begin, end, dst, dstPos);
        dstPos += end-begin;
        return this;
    }

    public @This RopeFactory add(MutableRope r, int begin, int end) {
        int len = end - begin;
        arraycopy(r.u8(), (int)r.offset+begin, dst, dstPos, len);
        dstPos += len;
        return this;
    }

    public @This RopeFactory add(byte[] u8) {
        arraycopy(u8, 0, dst, dstPos, u8.length);
        dstPos += u8.length;
        return this;
    }

    public @This RopeFactory add(byte[] u8, int begin, int end) {
        arraycopy(u8, begin, dst, dstPos, end-begin);
        dstPos += end-begin;
        return this;
    }

    public @This RopeFactory add(MemorySegment segment, long offset, int len) {
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, dst, dstPos, len);
        dstPos += len;
        return this;
    }
}
