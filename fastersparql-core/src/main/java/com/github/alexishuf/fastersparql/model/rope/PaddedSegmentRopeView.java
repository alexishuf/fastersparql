package com.github.alexishuf.fastersparql.model.rope;

public final class PaddedSegmentRopeView extends SegmentRopeView {
    public static final int BYTES = SegmentRopeView.BYTES + 64;

    @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
}
