package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityPool;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RopeHandlePool {
    private static final int CAPACITY = 1024;
    private static final AffinityPool<SegmentRope> POOL
            = new AffinityPool<>(SegmentRope.class, CAPACITY);

    public static SegmentRope segmentRope() {
        var r = POOL.get();
        return r == null ? new SegmentRope() : r;
    }

    public static @Nullable SegmentRope offer(SegmentRope r) {
        return POOL.offer(r);
    }
}
