package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;

import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.EMPTY_SEGMENT;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.EMPTY_UTF8;

public class PooledTwoSegmentRope extends PooledTwoSegmentRope0 implements SafeCloseable {
    private static final boolean DEBUG = PooledTwoSegmentRope.class.desiredAssertionStatus();
    private static final int BYTES = TwoSegmentRope.BYTES + (2*4) + 64;
    private static final Supplier<PooledTwoSegmentRope> FAC = PooledTwoSegmentRope::new;
    private static final Alloc<PooledTwoSegmentRope> ALLOC = new Alloc<>(
            PooledTwoSegmentRope.class, "PooledTwoSegmentRope",
            Alloc.THREADS*64, FAC, BYTES);
    static { Primer.INSTANCE.sched(ALLOC::prime); }

    @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;

    public static PooledTwoSegmentRope ofEmpty() {
        PooledTwoSegmentRope r = ALLOC.create();
        r.pooled = false;
        return r;
    }

    private PooledTwoSegmentRope() { pooled = true; }

    @Override public void close() {
        boolean bad = pooled;
        if (!bad) {
            pooled = true;
            wrapFirst (EMPTY_SEGMENT, EMPTY_UTF8, 0, 0);
            wrapSecond(EMPTY_SEGMENT, EMPTY_UTF8, 0, 0);
        }
        if (DEBUG)
            VarHandle.fullFence();
        if (bad || !pooled)
            throw new IllegalStateException("duplicate/concurrent close()");
        ALLOC.offer(this);
    }
}

abstract class PooledTwoSegmentRope0 extends TwoSegmentRope {
    protected boolean pooled;
}
