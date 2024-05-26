package com.github.alexishuf.fastersparql.model.rope;

public final class PrivateRopeFactory extends BaseRopeFactory<PrivateRopeFactory> {

    public PrivateRopeFactory() {super(CHUNK_SIZE);}
    public PrivateRopeFactory(int initialChunkSize) {super(initialChunkSize);}

    public PrivateRopeFactory alloc(int bytes) {
        reserve(bytes);
        return this;
    }

    public FinalSegmentRope take() {return take0();}

    public FinalSegmentRope asFinal(SegmentRope r) {
        return alloc(r.len).add(r).take0();
    }
    public FinalSegmentRope asFinal(SegmentRope r, int begin, int end) {
        return alloc(end-begin).add(r, begin, end).take0();
    }
}
