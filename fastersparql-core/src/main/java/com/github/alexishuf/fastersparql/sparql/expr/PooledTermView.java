package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;

public final class PooledTermView extends PooledTermView0 implements AutoCloseable {
    private static final int BYTES = TermView.BYTES + 2*4 + 64;
    private static final boolean DEBUG = PooledTermView.class.desiredAssertionStatus();
    private static final SegmentRope EMPTY_STRING_LOCAL = EMPTY_STRING.local();

    private static final java.util.function.Supplier<PooledTermView> FAC = new java.util.function.Supplier<>() {
        @Override public PooledTermView get() {
            PooledTermView view = new PooledTermView();
            view.pooled = true;
            return view;
        }

        @Override public String toString() {
            return "PooledTermView.FAC";
        }
    };
    private static final Alloc<PooledTermView> ALLOC = new Alloc<>(PooledTermView.class,
            "PooledTermView.ALLOC", Alloc.THREADS*64, FAC, BYTES);
    static {
        Primer.INSTANCE.sched(ALLOC::prime);
    }

    @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;

    public static PooledTermView ofEmptyString() {
        PooledTermView view = ALLOC.create();
        view.pooled = false;
        return view;
    }

    public static PooledTermView of(SegmentRope shared, SegmentRope local, boolean suffixShared) {
        var v = ALLOC.create();
        v.pooled = false;
        v.wrap(shared, local, suffixShared);
        return v;
    }

    public static PooledTermView of(SegmentRope shared, MemorySegment localSeg,
                                    byte @Nullable [] localU8, long localOff, int localLen,
                                    boolean suffixShared) {
        var v = ALLOC.create();
        v.pooled = false;
        v.wrap(shared, localSeg, localU8, localOff, localLen, suffixShared);
        return v;
    }

    @Override public void close() {
        boolean bad = pooled;
        pooled = true;
        updateShared(EMPTY, local.wrap(EMPTY_STRING_LOCAL), true);
        if (DEBUG)
            VarHandle.fullFence();
        if (bad || !pooled)
            throw new IllegalStateException("duplicate/concurrent close");
        ALLOC.offer(this);
    }
}
abstract sealed class PooledTermView0 extends TermView permits PooledTermView {
    protected boolean pooled;
}
