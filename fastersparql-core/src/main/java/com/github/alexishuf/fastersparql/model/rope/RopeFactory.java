package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.mustcall.qual.Owning;

import java.util.function.Supplier;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;

@MustCall("take")
public final class RopeFactory extends BaseRopeFactory<RopeFactory> {
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

    private boolean live;

    private RopeFactory() {super(CHUNK_SIZE);}

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
        fac.reserve(bytes);
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
            var rope = take0();
            live = false;
            ALLOC.offer(this);
            return rope;
        } else {
            throw new IllegalStateException("duplicate/concurrent close()");
        }
    }
}
