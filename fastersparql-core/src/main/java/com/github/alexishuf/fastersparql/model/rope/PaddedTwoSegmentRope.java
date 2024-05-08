package com.github.alexishuf.fastersparql.model.rope;

import java.lang.foreign.MemorySegment;

public class PaddedTwoSegmentRope extends TwoSegmentRope {
    public static final int BYTES = TwoSegmentRope.BYTES + 64;

    @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;

    public PaddedTwoSegmentRope() {}

    public PaddedTwoSegmentRope(SegmentRope first, SegmentRope snd) {
        super(first, snd);
    }

    public PaddedTwoSegmentRope(MemorySegment fst, long fstOff, int fstLen, MemorySegment snd, long sndOff, int sndLen) {
        super(fst, fstOff, fstLen, snd, sndOff, sndLen);
    }
}
