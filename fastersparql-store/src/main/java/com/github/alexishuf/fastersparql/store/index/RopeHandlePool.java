package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.util.BS;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class RopeHandlePool {
    private static final int CAPACITY = 1024;

    private static final VarHandle LOCK;
    static {
        try {
            LOCK = MethodHandles.lookup().findStaticVarHandle(RopeHandlePool.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private static int plainLock;
    private static final int[] hasSingle = BS.init(null, CAPACITY);
    private static final int[] hasTwo = BS.init(null, CAPACITY);
    private static final SegmentRope[] single = new SegmentRope[CAPACITY];
    private static final TwoSegmentRope[] two = new TwoSegmentRope[CAPACITY];

    public static SegmentRope segmentRope() {
        SegmentRope local = null;
        while ((int)LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            int i = BS.nextSet(hasSingle, 0);
            if (i >= 0) {
                local = single[i];
                single[i] = null;
            }
        } finally {
            LOCK.setRelease(0);
        }
        return local == null ? new SegmentRope() : local;
    }

    public static TwoSegmentRope twoSegmentRope() {
        TwoSegmentRope local = null;
        while ((int)LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            int i = BS.nextSet(hasTwo, 0);
            if (i >= 0) {
                local = two[i];
                two[i] = null;
            }
        } finally {
            LOCK.setRelease(0);
        }
        return local == null ? new TwoSegmentRope() : local;
    }

    public static @Nullable SegmentRope offer(SegmentRope r) {
        if (r == null) return null;
        r.wrap(ByteRope.EMPTY);
        int end = hasSingle.length << 5;
        while ((int)LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            int i = BS.nextClear(hasSingle, 0);
            if (i != end) {
                hasSingle[i>>5] |= 1 << i;
                single[i] = r;
                return null;
            }
        } finally {
            LOCK.setRelease(0);
        }
        return r;
    }

    public static @Nullable TwoSegmentRope offer(TwoSegmentRope r) {
        if (r == null) return null;
        r.wrapFirst(ByteRope.EMPTY);
        r.wrapSecond(ByteRope.EMPTY);
        int end = hasTwo.length << 5;
        while ((int)LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            int i = BS.nextClear(hasTwo, 0);
            if (i != end) {
                hasTwo[i>>5] |= 1 << i;
                two[i] = r;
                return null;
            }
        } finally {
            LOCK.setRelease(0);
        }
        return r;
    }
}
