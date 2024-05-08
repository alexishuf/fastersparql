package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RopeHandlePool {
    private static final int CAPACITY = 8192;
    private static final Alloc<SegmentRopeView> ALLOC = new Alloc<>(SegmentRopeView.class,
            "RopeHandlePool", CAPACITY, SegmentRopeView::new,
            16 + 4*2 + 8+4*2 + 4*2);
    static { Primer.INSTANCE.sched(ALLOC::prime); }

    public static SegmentRopeView segmentRope() {
        return ALLOC.create();
    }

    public static @Nullable SegmentRopeView offer(SegmentRopeView r) {return ALLOC.offer(r);}
}
