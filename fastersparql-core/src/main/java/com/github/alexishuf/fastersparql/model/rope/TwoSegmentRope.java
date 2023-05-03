package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class TwoSegmentRope extends PlainRope {
    public MemorySegment fst, snd;
    public long fstOff, sndOff;
    public int fstLen, sndLen;

    public TwoSegmentRope() {
        super(0);
        fst = ByteRope.EMPTY_SEGMENT;
        snd = ByteRope.EMPTY_SEGMENT;
    }

    public TwoSegmentRope(SegmentRope first, SegmentRope snd) {
        super(first.len+snd.len);
        this.fst = first.segment;
        this.fstOff = first.offset;
        this.fstLen = first.len;
        this.snd = first.segment;
        this.sndOff = first.offset;
        this.sndLen = first.len;
    }

    public TwoSegmentRope(MemorySegment fst, long fstOff, int fstLen, MemorySegment snd, long sndOff, int sndLen) {
        super(fstLen+sndLen);
        this.fst = fst;
        this.snd = snd;
        this.fstOff = fstOff;
        this.sndOff = sndOff;
        this.fstLen = fstLen;
        this.sndLen = sndLen;
    }

    public void wrapFirst(MemorySegment segment, long off, int len) {
        fst = segment;
        fstOff = off;
        fstLen = len;
        this.len = len+sndLen;
    }

    public void wrapSecond(MemorySegment segment, long off, int len) {
        snd = segment;
        sndOff = off;
        sndLen = len;
        this.len = fstLen+len;
    }

    public void wrapFirst(SegmentRope rope) {
        fst = rope.segment;
        fstOff = rope.offset;
        fstLen = rope.len;
        this.len = fstLen+sndLen;
    }

    public void wrapSecond(SegmentRope rope) {
        snd = rope.segment;
        sndOff = rope.offset;
        sndLen = rope.len;
        this.len = fstLen+sndLen;
    }

    public void flipSegments() {
        MemorySegment seg = fst;
        long off          = fstOff;
        int len           = fstLen;
        fst    = snd;
        fstOff = sndOff;
        fstLen = sndLen;
        snd    = seg;
        sndOff = off;
        sndLen = len;
    }

    public MemorySegment firstSegment()  { return fst; }
    public long          firstOff()      { return fstOff; }
    public int           firstLen()      { return fstLen; }
    public MemorySegment secondSegment() { return snd; }
    public long          secondOff()     { return sndOff; }
    public int           secondLen()     { return sndLen; }

    protected void checkRange(int begin, int end) {
        int len = this.len;
        String msg;
        if      (end   < begin) msg = "Range with end < begin";
        else if (begin <     0) msg = "Negative begin";
        else if (end   >   len) msg = "Range overflows Rope end";
        else return;
        throw new IndexOutOfBoundsException(msg);
    }

    @Override public byte get(int i) {
        if (i < 0 || i >= len) throw new IndexOutOfBoundsException();
        if (i < fstLen) return fst.get(JAVA_BYTE, fstOff+i);
        return snd.get(JAVA_BYTE, sndOff+(i-fstLen));
    }

    @Override public byte[] copy(int begin, int end, byte[] dest, int offset) {
        checkRange(begin, end);
        if (begin < fstLen) {
            int n = Math.min(end, fstLen)-begin;
            MemorySegment.copy(fst, JAVA_BYTE, fstOff+begin, dest, offset, n);
            offset += n;
        }
        begin = Math.max(0, begin-fstLen);
        int n = (end-fstLen)-begin;
        if (n > 0) {
            MemorySegment.copy(snd, JAVA_BYTE, sndOff+begin, dest, offset, n);
        }
        return dest;
    }

    @Override public Rope sub(int begin, int end) {
        checkRange(begin, end);
        if (end-begin == len) return this;
        var r = new TwoSegmentRope();
        if (begin < fstLen)
            r.wrapFirst(fst, fstOff+begin, Math.min(fstLen, end)-begin);
        int e = end-fstLen;
        if (e > 0) {
            begin = Math.max(0, begin - fstLen);
            r.wrapSecond(snd, sndOff+begin, e-begin);
        }
        return r;
    }

    @Override public int skipUntil(int begin, int end, char c0) {
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            i = (int)(SegmentRope.skipUntil(fst, begin+fstOff, e+fstOff, c0)-fstOff);
            if (i < e) return i;
        }
        if ((e = end-fstLen) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + (int)(SegmentRope.skipUntil(snd, sndOff+i, sndOff+e, c0)-sndOff);
        }
        return end;
    }

    @Override public int skipUntil(int begin, int end, char c0, char c1) {
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            i = (int)(SegmentRope.skipUntil(fst, begin+fstOff, e+fstOff, c0, c1)-fstOff);
            if (i < e) return i;
        }
        if ((e = end-fstLen) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + (int)(SegmentRope.skipUntil(snd, sndOff+i, sndOff+e, c0, c1)-sndOff);
        }
        return end;
    }

    @Override public int skipUntilLast(int begin, int end, char c0) {
        checkRange(begin, end);
        int e = end-fstLen, i = Math.max(0, begin-fstLen);
        if (e > 0) {
            i = (int)(SegmentRope.skipUntilLast(snd, sndOff+i, sndOff+e, c0)-sndOff);
            if (i < e) return fstLen+i;
        }
        if (begin < fstLen) {
            e = Math.min(fstLen, end);
            i = (int)(SegmentRope.skipUntilLast(fst, fstOff+begin, fstOff+e, c0)-fstOff);
            if (i < e) return i;
        }
        return end;
    }


    @Override public int skipUntilLast(int begin, int end, char c0, char c1) {
        checkRange(begin, end);
        int e = end-fstLen, i = Math.max(0, begin-fstLen);
        if (e > 0) {
            i = (int)(SegmentRope.skipUntilLast(snd, sndOff+i, sndOff+e, c0, c1)-sndOff);
            if (i < e) return fstLen+i;
        }
        if (begin < fstLen) {
            e = Math.min(fstLen, end);
            i = (int)(SegmentRope.skipUntilLast(fst, fstOff+begin, fstOff+e, c0, c1)-fstOff);
            if (i < e) return i;
        }
        return end;
    }

    @Override public int skip(int begin, int end, int[] alphabet) {
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            i = (int)(SegmentRope.skip(fst, fstOff+begin, fstOff+e, alphabet)-fstOff);
            if (i < e) return i;
        }
        if ((e = Math.max(0, end-fstLen)) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + (int)(SegmentRope.skip(snd, sndOff+i, sndOff+e, alphabet)-sndOff);
        }
        return end;
    }

    @Override public boolean has(int pos, Rope rope, int begin, int end) {
        int rLen = end - begin;
        long physPos;
        MemorySegment lSeg;
        if (pos+ rLen <= fstLen) {
            lSeg = fst;
            physPos = fstOff+pos;
        } else if (pos >= fstLen) {
            lSeg = snd;
            physPos = sndOff+pos;
        } else {
            return super.has(pos, rope, begin, end);
        }
        MemorySegment rSeg;
        long physBegin;
        if (rope instanceof SegmentRope s) {
            rSeg = s.segment;
            physBegin = s.offset + begin;
        } else if (rope instanceof TwoSegmentRope t) {
            boolean onFirst = begin < t.fstLen;
            if (onFirst && end > t.fstLen) return super.has(pos, rope, begin, end);
            if (onFirst) {
                rSeg = t.fst;
                physBegin = t.fstOff+begin;
            } else {
                rSeg = t.snd;
                physBegin = t.sndOff+begin;
            }
        } else {
            return super.has(pos, rope, begin, end);
        }
        return SegmentRope.has(lSeg, physPos, rSeg, physBegin, rLen);
    }

    @Override public int hashCode() {
        int h = SegmentRope.hashCode(0, fst, fstOff, fstOff + fstLen);
        return SegmentRope.hashCode(h, snd, sndOff, sndOff+sndLen);
    }

    @Override public int compareTo(@NonNull Rope o) {
        if      (o instanceof SegmentRope    s) return -s.compareTo(this);
        else if (o instanceof TwoSegmentRope t) return compareTo(t);
        else                                    return super.compareTo(o);
    }

    @Override public int compareTo(PlainRope o) {
        return o instanceof SegmentRope s ? -s.compareTo(this) : compareTo((TwoSegmentRope) o);
    }

    @Override public int compareTo(SegmentRope o) { return -o.compareTo(this); }

    @Override public int compareTo(TwoSegmentRope o) {
        // keep two ranges, defined by:
        //   - lSeg, lOff, lLen
        //   - rSeg, rOff, rLen
        //
        MemorySegment lSeg = fst,      rSeg = o.fst;
        long          lOff = fstOff,   rOff = o.fstOff;
        int           lLen = fstLen,   rLen = o.fstLen;
        boolean       lFst = true,     rFst = true;

        // if any of the ranges starts empty, advance it instead of exiting due to common == 0
        if (lLen == 0) { lSeg =   snd; lOff =   sndOff; lLen =   sndLen; lFst = false;}
        if (rLen == 0) { rSeg = o.snd; rOff = o.sndOff; rLen = o.sndLen; rFst = false;}

        // iterate comparing the common length between the ranges and then moving one
        // (or both ranges they had the same length)
        int common, diff;
        while (true) {
            if ((common = Math.min(lLen, rLen)) == 0)
                return lLen - rLen; // one side exhausted, largest wins
            if ((diff = SegmentRope.compareTo(lSeg, lOff, common, rSeg, rOff, common)) != 0)
                return diff; // found a mismatching byte
            // advance ranges
            lOff += common; lLen -= common;
            rOff += common; rLen -= common;
            if (lLen == 0 && lFst) { lSeg =   snd; lOff =   sndOff; lLen =   sndLen; lFst = false;}
            if (rLen == 0 && rFst) { rSeg = o.snd; rOff = o.sndOff; rLen = o.sndLen; rFst = false;}
        }
    }

    @Override public int compareTo(Rope o, int begin, int end) {
        if      (o instanceof SegmentRope    s) return compareTo(s, begin, end);
        else if (o instanceof TwoSegmentRope t) return compareTo(t, begin, end);
        else                                    return super.compareTo(o, begin, end);
    }

    @Override public int compareTo(PlainRope o, int begin, int end) {
        return o instanceof SegmentRope s
                ? compareTo(s, begin, end)
                : compareTo((TwoSegmentRope)o, begin, end);
    }

    @Override public int compareTo(SegmentRope o, int begin, int end) {
        MemorySegment lSeg = fst,      rSeg = o.segment;
        long          lOff = fstOff,   rOff = o.offset+begin;
        int           lLen = fstLen,   rLen = end-begin;
        boolean lFst = true;

        // maybe our first segment is empty, then we should start at snd
        if (lLen == 0) { lSeg = snd; lOff = sndOff; lLen = sndLen; lFst = false; }

        // iterate comparing the common length between the ranges and then moving one
        // (or both ranges they had the same length)
        int common, diff;
        while (true) {
            if ((common = Math.min(lLen, rLen)) == 0)
                return lLen - rLen; // one side exhausted, largest wins
            if ((diff = SegmentRope.compareTo(lSeg, lOff, common, rSeg, rOff, common)) != 0)
                return diff; // found a mismatching byte
            // advance ranges
            lOff += common; lLen -= common;
            rOff += common; rLen -= common;
            if (lLen == 0 && lFst) { lSeg = snd; lOff = sndOff; lLen = sndLen; lFst = false; }
        }
    }

    @Override public int compareTo(TwoSegmentRope o, int begin, int end) {
        // the following locals simulate o.sub(begin, end)
        long o_fstOff = o.fstOff+begin, o_sndOff = o.sndOff+Math.max(0, begin-o.fstLen);
        int  o_fstLen = Math.max(0, Math.min(end, o.fstLen)-begin),
             o_sndLen = end-begin-o_fstLen;

        // keep two ranges, defined by:
        //   - lSeg, lOff, lLen
        //   - rSeg, rOff, rLen
        //
        MemorySegment lSeg = fst,      rSeg = o.fst;
        long          lOff = fstOff,   rOff = o_fstOff;
        int           lLen = fstLen,   rLen = o_fstLen;
        boolean       lFst = true,     rFst = true;

        // if any of the ranges starts empty, advance it instead of exiting due to common == 0
        if (lLen == 0) { lSeg =   snd; lOff =   sndOff; lLen =   sndLen; lFst = false; }
        if (rLen == 0) { rSeg = o.snd; rOff = o_sndOff; rLen = o_sndLen; rFst = false; }

        // iterate comparing the common length between the ranges and then moving one
        // (or both ranges they had the same length)
        int common, diff;
        while (true) {
            if ((common = Math.min(lLen, rLen)) == 0)
                return lLen - rLen; // one side exhausted, largest wins
            if ((diff = SegmentRope.compareTo(lSeg, lOff, common, rSeg, rOff, common)) != 0)
                return diff; // found a mismatching byte
            // advance ranges
            lOff += common; lLen -= common;
            rOff += common; rLen -= common;
            if (lLen == 0 && lFst) { lSeg =   snd; lOff =   sndOff; lLen =   sndLen; lFst = false; }
            if (rLen == 0 && rFst) { rSeg = o.snd; rOff = o_sndOff; rLen = o_sndLen; rFst = false; }
        }
    }
}
