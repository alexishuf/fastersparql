package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.EMPTY_SEGMENT;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.EMPTY_UTF8;

public final class TermInfo {
    private static final TwoSegmentRope TSR_EMPTY = new TwoSegmentRope();

    public Type type;
    public @Nullable Term term;
    public FinalSegmentRope shared;
    public @Nullable SegmentRope localRope;
    public MemorySegment localSeg;
    public byte @Nullable [] localU8;
    public long localOff;
    public int localLen;
    public boolean suffixShared, stable;
    public MemorySegment sharedSeg;
    public byte @Nullable [] sharedU8;
    public long sharedOff;
    public int sharedLen;

    public Term  term() { return Objects.requireNonNull(term); }

    public enum Type {
        EMPTY,
        TERM,
        SHARED_AND_SEGMENT,
        UNINTERNABLE,
    }

    public Type setEmpty() {
        type         = Type.EMPTY;
        term         = null;
        shared       = EMPTY;
        localRope    = null;
        localSeg     = EMPTY_SEGMENT;
        localU8      = EMPTY_UTF8;
        localOff     = 0L;
        localLen     = 0;
        sharedSeg     = EMPTY_SEGMENT;
        sharedU8      = EMPTY_UTF8;
        sharedOff     = 0L;
        sharedLen     = 0;
        suffixShared = false;
        stable       = false;
        return Type.EMPTY;
    }

    public Type setTerm(@Nullable FinalTerm t) {
        term   = t;
        stable = true;
        if (t != null) {
            var s        = t.finalShared();
            type         = Type.TERM;
            suffixShared = t.sharedSuffixed();
            shared       = s;
            localRope    = t.local();
            localSeg     = localRope.segment;
            localU8      = localRope.utf8;
            localOff     = localRope.offset;
            localLen     = localRope.len;
            sharedSeg    = s.segment;
            sharedU8     = s.utf8;
            sharedOff    = s.offset;
            sharedLen    = s.len;
        } else {
            type         = Type.EMPTY;
            suffixShared = false;
            shared       = EMPTY;
            localRope    = null;
            localSeg     = EMPTY_SEGMENT;
            localU8      = EMPTY_UTF8;
            localOff     = 0L;
            localLen     = 0;
            sharedSeg    = null;
            sharedU8     = null;
            sharedOff    = 0L;
            sharedLen    = 0;
        }
        return Type.TERM;
    }

    public Type setTerm(@Nullable Term t) {
        term         = t;
        if (t == null) {
            type         = Type.EMPTY;
            localRope    = null;
            localSeg     = EMPTY_SEGMENT;
            localU8      = EMPTY_UTF8;
            localOff     = 0L;
            localLen     = 0;
            sharedSeg    = null;
            sharedU8     = null;
            sharedOff    = 0L;
            sharedLen    = 0;
            suffixShared = false;
        } else {
            type = Type.TERM;
            localRope = t.local();
            localSeg     = localRope.segment;
            localU8      = localRope.utf8;
            localOff     = localRope.offset;
            localLen     = localRope.len;
            var s        = t.shared();
            sharedSeg    = s.segment;
            sharedU8     = s.utf8;
            sharedOff    = s.offset;
            sharedLen    = s.len;
        }
        if (t instanceof FinalTerm) {
            stable       = true;
            shared       = t.finalShared();
        } else {
            stable       = false;
            shared       = EMPTY;
        }
        return Type.TERM;
    }

    public Type setSharedAndSegment(boolean stable, FinalSegmentRope shared,
                                    MemorySegment localSeg, byte @Nullable[] localU8,
                                    long localOff, int localLen, boolean suffixShared) {
        var s             = shared == null ? EMPTY : shared;
        if (localLen == 0 && s.len == 0) {
            this.type = Type.EMPTY;
            if (localSeg == null) {
                localSeg = EMPTY_SEGMENT;
                localU8  = EMPTY_UTF8;
                localOff = 0;
            }
        } else {
            this.type = Type.SHARED_AND_SEGMENT;
        }
        this.stable       = stable;
        this.suffixShared = suffixShared;
        this.term         = null;
        this.localRope    = null;
        this.localSeg     = localSeg;
        this.localU8      = localU8;
        this.localOff     = localOff;
        this.localLen     = localLen;
        this.shared       = s;
        this.sharedSeg    = s.segment;
        this.sharedU8     = s.utf8;
        this.sharedOff    = s.offset;
        this.sharedLen    = s.len;
        return Type.SHARED_AND_SEGMENT;
    }

    public Type setUninternable(boolean stable, TwoSegmentRope tsr, boolean localIsSnd) {
        if (tsr == null)
            tsr = TSR_EMPTY;
        this.type         = tsr.len == 0 ? Type.EMPTY : Type.UNINTERNABLE;
        this.stable       = stable;
        this.term         = null;
        this.localRope    = null;
        this.shared       = EMPTY;
        if (localIsSnd) {
            this.suffixShared = false;
            this.sharedSeg    = tsr.fst;
            this.sharedU8     = tsr.fstU8;
            this.sharedOff    = tsr.fstOff;
            this.sharedLen    = tsr.fstLen;
            this.localSeg     = tsr.snd;
            this.localU8      = tsr.sndU8;
            this.localOff     = tsr.sndOff;
            this.localLen     = tsr.sndLen;
        } else {
            this.suffixShared = true;
            this.sharedSeg    = tsr.snd;
            this.sharedU8     = tsr.sndU8;
            this.sharedOff    = tsr.sndOff;
            this.sharedLen    = tsr.sndLen;
            this.localSeg     = tsr.fst;
            this.localU8      = tsr.fstU8;
            this.localOff     = tsr.fstOff;
            this.localLen     = tsr.fstLen;
        }
        return Type.UNINTERNABLE;
    }
}
