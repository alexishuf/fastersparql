package com.github.alexishuf.fastersparql.model.rope;

import java.lang.foreign.MemorySegment;

public abstract sealed class PrivateRopeFactory extends BaseRopeFactory<PrivateRopeFactory> {
    public static final class Naked extends PrivateRopeFactory implements NakedRopeFactory {
        public Naked(boolean alloc) {
            super(alloc);
        }
        @Override public MemorySegment  segment() {return segment0();}
        @Override public byte[]            utf8() {return    utf80();}
        @Override public int              begin() {return   begin0();}
        @Override public int                len() {return     len0();}
        @Override public void             close() {          done0();}
    }

    public static PrivateRopeFactory create() {
        return new Naked(true);
    }
    public static PrivateRopeFactory createLazyAlloc() {
        return new Naked(false);
    }

    protected PrivateRopeFactory(boolean alloc) {
        super(alloc);
    }

    public PrivateRopeFactory alloc(int bytes) {
        reserve(bytes);
        return this;
    }

    public FinalSegmentRope take() {return take0();}

    public Naked naked() {return (Naked)this;}

    public FinalSegmentRope asFinal(SegmentRope r) {
        return alloc(r.len).add(r).take0();
    }
    public FinalSegmentRope asFinal(SegmentRope r, int begin, int end) {
        return alloc(end-begin).add(r, begin, end).take0();
    }
}
