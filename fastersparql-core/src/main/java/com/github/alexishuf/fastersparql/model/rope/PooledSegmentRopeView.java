package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class PooledSegmentRopeView extends PooledSegmentRopeView0 implements SafeCloseable {
    private static final int BYTES = SegmentRopeView.BYTES + 2*4 + 64;
    private static final boolean DEBUG = PooledSegmentRopeView.class.desiredAssertionStatus();
    private static final Supplier<PooledSegmentRopeView> FAC = new Supplier<>() {
        @Override public PooledSegmentRopeView get() {return new PooledSegmentRopeView();}
        @Override public String toString() {return "PooledSegmentRope.FAC";}
    };
    private static final Alloc<PooledSegmentRopeView> ALLOC = new Alloc<>(
            PooledSegmentRopeView.class, "PooledSegmentRope.ALLOC",
            Alloc.THREADS*64, FAC, BYTES);
    static { Primer.INSTANCE.sched(ALLOC::prime); }

    @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;

    private PooledSegmentRopeView() { }

    /** Gets or creates a {@link PooledSegmentRopeView} and {@link #wrapEmpty()} */
    public static @Owning PooledSegmentRopeView ofEmpty() {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrapEmpty();
        return r;
    }

    /** Gets or creates a {@link PooledSegmentRopeView} and {@link #wrap(MemorySegment)} {@code segment} */
    public static @Owning PooledSegmentRopeView of(MemorySegment segment) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(segment);
        return r;
    }

    /** Gets or creates a {@link PooledSegmentRopeView} and {@link #wrap(byte[])} {@code u8} */
    public static @Owning PooledSegmentRopeView of(byte[] u8) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(u8);
        return r;
    }

    /**
     * Gets or creates a {@link PooledSegmentRopeView} and forward the given arguments to
     * {@link #wrap(MemorySegment, byte[])}
     * @return the wrapping {@link PooledSegmentRopeView}
     */
    public static @Owning PooledSegmentRopeView of(MemorySegment segment, byte @Nullable[] utf8) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(segment, utf8);
        return r;
    }

    /**
     * Gets or creates a {@link PooledSegmentRopeView} and forward the given arguments to
     * {@link #wrap(MemorySegment, long, int)}
     * @return the wrapping {@link PooledSegmentRopeView}
     */
    public static @Owning PooledSegmentRopeView of(MemorySegment segment, long offset, int len) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(segment, offset, len);
        return r;
    }

    /**
     * Gets or creates a {@link PooledSegmentRopeView} and forward the given arguments to
     * {@link #wrap(MemorySegment, byte[], long, int)}
     * @return the wrapping {@link PooledSegmentRopeView}
     */
    public static @Owning PooledSegmentRopeView of(MemorySegment segment, byte @Nullable[] utf8,
                                                   long offset, int len) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(segment, utf8, offset, len);
        return r;
    }

    /**
     * Gets or creates a {@link PooledSegmentRopeView} and forward the given arguments to
     * {@link #wrap(ByteBuffer)}
     * @return the wrapping {@link PooledSegmentRopeView}
     */
    public static @Owning PooledSegmentRopeView of(ByteBuffer bb) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(bb);
        return r;
    }

    /**
     * Gets or creates a {@link PooledSegmentRopeView} and forward the given arguments to
     * {@link #wrap(SegmentRope)}
     * @return the wrapping {@link PooledSegmentRopeView}
     */
    public static @Owning PooledSegmentRopeView of(SegmentRope rope) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(rope);
        return r;
    }

    /**
     * Gets or creates a {@link PooledSegmentRopeView} and forward the given arguments to
     * {@link #wrap(SegmentRope, int, int)}
     * @return the wrapping {@link PooledSegmentRopeView}
     */
    public static @Owning PooledSegmentRopeView of(SegmentRope rope, int offset, int len) {
        var r = ALLOC.create();
        r.pooled = false;
        r.wrap(rope, offset, len);
        return r;
    }

    /**
     * Returns {@code this} to the global pool so it can be returned from a future
     * {@code of()} call
     */
    @Override public void close() {
        boolean bad = pooled;
        pooled = true;
        wrapEmpty();
        if (DEBUG)
            VarHandle.fullFence();
        if (bad || !pooled)
            throw new IllegalStateException("duplicate/concurrent close()");
        ALLOC.offer(this);
    }
}

abstract class PooledSegmentRopeView0 extends SegmentRopeView {
    protected boolean pooled;
}
