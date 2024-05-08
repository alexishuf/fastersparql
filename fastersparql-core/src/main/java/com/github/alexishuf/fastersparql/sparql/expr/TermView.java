package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;

public sealed class TermView extends Term permits PooledTermView {
    public static final int BYTES = Term.BYTES + 2*4;
    protected final SegmentRopeView local;

    public TermView() { this.local = (SegmentRopeView)first(); }

    public @This TermView wrap(SegmentRope shared, SegmentRope local, boolean suffixShared) {
        updateShared(shared, this.local.wrap(local), suffixShared);
        assert validate();
        return this;
    }

    public @This TermView wrap(SegmentRope shared, MemorySegment localSeg,
                               byte @Nullable[] localU8, long localOff, int localLen,
                               boolean suffixShared) {
        updateShared(shared, this.local.wrap(localSeg, localU8, localOff, localLen), suffixShared);
        assert validate();
        return this;
    }
}
