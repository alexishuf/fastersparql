package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.lang.foreign.MemorySegment;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare2_2;
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
        this.snd = snd.segment;
        this.sndOff = snd.offset;
        this.sndLen = snd.len;
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

    public void shallowCopy(TwoSegmentRope other) {
        fst    = other.fst;
        fstOff = other.fstOff;
        fstLen = other.fstLen;
        snd    = other.snd;
        sndOff = other.sndOff;
        sndLen = other.sndLen;
        len    = other.len;
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
        if (begin < 0 || end > rope.len) throw new IndexOutOfBoundsException();
        if (pos+rLen > len) return false;

        long fstOff = this.fstOff+pos, sndOff = this.sndOff+Math.max(0, pos-this.fstLen);
        int fstLen = Math.min(rLen, this.fstLen-pos), sndLen = pos+rLen-this.fstLen;

        //noinspection IfCanBeSwitch
        if (rope instanceof SegmentRope s) {
            return compare1_2(s.segment, s.offset+begin, rLen,
                              fst, fstOff, fstLen, snd, sndOff, sndLen) == 0;
        } else {
            MemorySegment o_fst, o_snd;
            long o_fstOff, o_sndOff;
            int o_fstLen;
            if (rope instanceof TwoSegmentRope t) {
                o_fst = t.fst; o_fstOff = t.fstOff; o_fstLen = t.fstLen;
                o_snd = t.snd; o_sndOff = t.sndOff;
            } else {
                Term t = (Term) rope;
                SegmentRope r;
                o_fst = (r = t. first()).segment; o_fstOff = r.offset; o_fstLen = r.len;
                o_snd = (r = t.second()).segment; o_sndOff = r.offset;
            }
            o_fstOff += begin;
            o_sndOff += Math.max(0, begin-o_fstLen);
            o_fstLen = Math.min(o_fstLen, end)-begin;
            return compare2_2(  fst,   fstOff,   fstLen,   snd,   sndOff,   sndLen,
                              o_fst, o_fstOff, o_fstLen,
                              o_snd, o_sndOff, rLen-o_fstLen) == 0;
        }
    }

    @Override public int fastHash(int begin, int end) {
        int h, nFst = Math.min(4, end-begin), nSnd = Math.min(12, end-(begin+4));
        if (begin+nFst < fstLen) {
            h = SegmentRope.hashCode(FNV_BASIS, fst, fstOff+begin, nFst);
        } else {
            h = FNV_BASIS;
            for (int i = 0; i < nFst; i++)
                h = FNV_PRIME * (h ^ (0xff&get(begin+i)));
        }
        begin = end-nSnd;
        if (begin > fstLen) {
            h = SegmentRope.hashCode(h, snd, sndOff+(begin-fstLen), nSnd);
        } else {
            for (int i = 0; i < nSnd; i++)
                h = FNV_PRIME * (h ^ (0xff&get(begin+i)));
        }
        return h;
    }

    @Override public int hashCode() {
        int h = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
        return SegmentRope.hashCode(h, snd, sndOff, sndLen);
    }

    @Override public int compareTo(SegmentRope o) {
        return -compare1_2(o.segment, o.offset, o.len,
                           fst, fstOff, fstLen, snd, sndOff, sndLen);
    }

    @Override public int compareTo(TwoSegmentRope o) {
        return compare2_2(  fst,   fstOff,   fstLen,   snd,   sndOff,   sndLen,
                          o.fst, o.fstOff, o.fstLen, o.snd, o.sndOff, o.sndLen);
    }

    @Override public int compareTo(SegmentRope o, int begin, int end) {
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        return -compare1_2(o.segment, o.offset+begin, end-begin,
                           fst, fstOff, fstLen, snd, sndOff, sndLen);
    }

    @Override public int compareTo(TwoSegmentRope o, int begin, int end) {
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        // the following locals simulate o.sub(begin, end)
        long o_fstOff = o.fstOff+begin, o_sndOff = o.sndOff+Math.max(0, begin-o.fstLen);
        int  o_fstLen = Math.max(0, Math.min(end, o.fstLen)-begin),
             o_sndLen = end-begin-o_fstLen;
        return compare2_2(  fst,   fstOff,   fstLen,   snd,   sndOff,   sndLen,
                                     o.fst, o_fstOff, o_fstLen, o.snd, o_sndOff, o_sndLen);
    }
}
