package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;

import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

public final class PooledMutableRope extends MutableRope {
    public static final int BYTES = MutableRope.BYTES + 2*4;
    private static final boolean DEBUG = PooledMutableRope.class.desiredAssertionStatus();
    private static final Supplier<PooledMutableRope> FAC = new Supplier<>() {
        @Override public PooledMutableRope get() {return new PooledMutableRope();}
        @Override public String toString() {return "PooledMutableRope.FAC";}
    };
    private static final Alloc<PooledMutableRope> ALLOC = new Alloc<>(PooledMutableRope.class,
            "PooledMutableRope.ALLOC", Alloc.THREADS*64, FAC,
            16 /*header*/ + 8+2*4 /*SegmentRope*/ + 2*4 /* MutableSegmentRope*/);

    static { Primer.INSTANCE.sched(ALLOC::prime); }

    public static PooledMutableRope get() {
        var r = ALLOC.create();
        r.pooled = false;
        return r;
    }

    public static PooledMutableRope getWithCapacity(int capacity) {
        var r = ALLOC.create();
        r.pooled = false;
        r.ensureFreeCapacity(capacity);
        return r;
    }

    private boolean pooled;

    private PooledMutableRope() { pooled = true; }

    @Override public void close() {
        boolean bad = pooled;
        if (!bad) {
            pooled = true;
            super.close();
        }
        if (DEBUG)
            VarHandle.fullFence();
        if (bad || !pooled)
            throw new IllegalStateException("duplicate/concurrent close()");
        ALLOC.offer(this);
    }
}
