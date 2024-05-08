package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;

import java.lang.foreign.MemorySegment;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.*;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.HAS_UNSAFE;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class TwoSegmentRope extends PlainRope {
    public static final int BYTES = 16 + 2*4 + 2*(4+4+8+4);
    public MemorySegment fst, snd;
    public byte[] fstU8, sndU8;
    public long fstOff, sndOff;
    public int fstLen, sndLen;


    public TwoSegmentRope() {
        super(0);
        fst = MutableRope.EMPTY_SEGMENT;
        snd = MutableRope.EMPTY_SEGMENT;
        fstU8 = MutableRope.EMPTY_UTF8;
        sndU8 = MutableRope.EMPTY_UTF8;
    }

    public TwoSegmentRope(SegmentRope first, SegmentRope snd) {
        super(first.len+snd.len);
        this.fst = first.segment;
        this.fstU8 = first.utf8;
        this.fstOff = first.offset;
        this.fstLen = first.len;
        this.snd = snd.segment;
        this.sndU8 = snd.utf8;
        this.sndOff = snd.offset;
        this.sndLen = snd.len;
    }

    public TwoSegmentRope(MemorySegment fst, long fstOff, int fstLen, MemorySegment snd, long sndOff, int sndLen) {
        super(fstLen+sndLen);
        this.fst = fst;
        this.snd = snd;
        this.fstU8 = (byte[])fst.heapBase().orElse(null);
        this.sndU8 = (byte[])snd.heapBase().orElse(null);
        this.fstOff = fstOff;
        this.sndOff = sndOff;
        this.fstLen = fstLen;
        this.sndLen = sndLen;
    }

    public void shallowCopy(TwoSegmentRope other) {
        fst    = other.fst;
        fstU8  = other.fstU8;
        fstOff = other.fstOff;
        fstLen = other.fstLen;
        snd    = other.snd;
        sndU8  = other.sndU8;
        sndOff = other.sndOff;
        sndLen = other.sndLen;
        len    = other.len;
    }

    public void wrapFirst(MemorySegment segment, byte[] utf8, long off, int len) {
        fst      = segment;
        fstU8    = utf8;
        fstOff   = off;
        fstLen   = len;
        this.len = len+sndLen;
    }

    public void wrapSecond(MemorySegment segment, byte[] utf8, long off, int len) {
        snd      = segment;
        sndU8    = utf8;
        sndOff   = off;
        sndLen   = len;
        this.len = fstLen+len;
    }

    public void wrapFirst(SegmentRope rope) {
        fst      = rope.segment;
        fstU8    = rope.utf8;
        fstOff   = rope.offset;
        fstLen   = rope.len;
        this.len = fstLen+sndLen;
    }

    public void wrapSecond(SegmentRope rope) {
        snd      = rope.segment;
        sndU8    = rope.utf8;
        sndOff   = rope.offset;
        sndLen   = rope.len;
        this.len = fstLen+sndLen;
    }

    public void flipSegments() {
        MemorySegment seg = fst;
        byte[] u8         = fstU8;
        long off          = fstOff;
        int len           = fstLen;
        fst    = snd;
        fstU8  = sndU8;
        fstOff = sndOff;
        fstLen = sndLen;
        snd    = seg;
        sndU8  = u8;
        sndOff = off;
        sndLen = len;
    }

    @SuppressWarnings("unused") public MemorySegment firstSegment()  { return fst; }
    @SuppressWarnings("unused") public long          firstOff()      { return fstOff; }
    @SuppressWarnings("unused") public int           firstLen()      { return fstLen; }
    @SuppressWarnings("unused") public MemorySegment secondSegment() { return snd; }
    @SuppressWarnings("unused") public long          secondOff()     { return sndOff; }
    @SuppressWarnings("unused") public int           secondLen()     { return sndLen; }

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
        if (LowLevelHelper.U == null)
            return copySafe(begin, end, dest, offset);
        if (offset+(end-begin) > dest.length)
            throw new IndexOutOfBoundsException("Copying [begin, end) overflows dest at offset");
        offset += LowLevelHelper.U8_BASE;
        if (begin < fstLen) {
            int n = Math.min(end, fstLen)-begin;
            LowLevelHelper.U.copyMemory(fstU8, (fstU8 == null ? 0 : LowLevelHelper.U8_BASE)+fst.address()+fstOff+begin,
                         dest, offset, n);
            offset += n;
        }
        begin = Math.max(0, begin-fstLen);
        int n = (end - fstLen) - begin;
        if (n > 0) {
            LowLevelHelper.U.copyMemory(sndU8, (sndU8==null ? 0 : LowLevelHelper.U8_BASE)+snd.address()+sndOff+begin,
                         dest, offset, n);
        }
        return dest;
    }

    private byte[] copySafe(int begin, int end, byte[] dest, int offset) {
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
            r.wrapFirst(fst, fstU8, fstOff+begin, Math.min(fstLen, end)-begin);
        int e = end-fstLen;
        if (e > 0) {
            begin = Math.max(0, begin - fstLen);
            r.wrapSecond(snd, sndU8, sndOff+begin, e-begin);
        }
        return r;
    }

    @SuppressWarnings("unused") @Override public int skipUntil(int begin, int end, char c0) {
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            i = (int)(SegmentRope.skipUntil(fst, fstU8, begin+fstOff, e+fstOff, c0)-fstOff);
            if (i < e) return i;
        }
        if ((e = end-fstLen) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + (int)(SegmentRope.skipUntil(snd, sndU8, sndOff+i, sndOff+e, c0)-sndOff);
        }
        return end;
    }

    @SuppressWarnings("unused") @Override public int skipUntil(int begin, int end, char c0, char c1) {
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            i = (int)(SegmentRope.skipUntil(fst, fstU8, begin+fstOff, e+fstOff, c0, c1)-fstOff);
            if (i < e) return i;
        }
        if ((e = end-fstLen) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + (int)(SegmentRope.skipUntil(snd, sndU8, sndOff+i, sndOff+e, c0, c1)-sndOff);
        }
        return end;
    }

    @SuppressWarnings("unused") @Override public int skipUntilLast(int begin, int end, char c0) {
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


    @SuppressWarnings("unused") @Override public int skipUntilLast(int begin, int end, char c0, char c1) {
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

    private int skipSafe(int begin, int end, int[] alphabet) {
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            i = (int)(SegmentRope.skipSafe(fst, fstOff+begin, fstOff+e, alphabet)-fstOff);
            if (i < e) return i;
        }
        if ((e = Math.max(0, end-fstLen)) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + (int)(SegmentRope.skipSafe(snd, sndOff+i, sndOff+e, alphabet)-sndOff);
        }
        return end;
    }

    @SuppressWarnings("unused") @Override public int skip(int begin, int end, int[] alphabet) {
        if (LowLevelHelper.U == null)
            return skipSafe(begin, end, alphabet);
        checkRange(begin, end);
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen) {
            byte[] fstU8 = this.fstU8;
            long off = fstOff+fst.address();
            if (fstU8 != null)
                off += LowLevelHelper.U8_BASE;
            i = (int)(SegmentRope.skipUnsafe(fstU8, off+begin, off+e, alphabet)-off);
            if (i < e) return i;
        }
        if ((e = Math.max(0, end-fstLen)) > 0) {
            i = Math.max(0, begin-fstLen);
            long off = sndOff+snd.address();
            byte[] sndU8 = this.sndU8;
            if (sndU8 != null)
                off += LowLevelHelper.U8_BASE;
            return fstLen + (int)(SegmentRope.skipUnsafe(sndU8, off+i, off+e, alphabet)-off);
        }
        return end;
    }

    @SuppressWarnings("unused") @Override public boolean has(int position, byte[] seq) {
        if (!HAS_UNSAFE)
            return super.has(position, seq);
        if (position < 0) throw new IndexOutOfBoundsException(position);
        if (position+seq.length > len) return false;
        int fLen;
        long sOff = snd.address()+sndOff;
        if (position < fstLen) {
            fLen = Math.min(seq.length, fstLen-position);
            long fOff = fst.address()+fstOff+position;
            return compare1_2(seq, 0, seq.length,
                              fstU8, fOff, fLen, sndU8, sOff, seq.length-fLen) == 0;
        } else {
            int sPos = position - fstLen;
            return compare1_1(seq, 0, seq.length,
                              sndU8, sOff+sPos, Math.min(seq.length, sndLen-sPos)) == 0;
        }
    }

    @SuppressWarnings("unused") @Override public boolean has(int pos, Rope rope, int begin, int end) {
        if (!HAS_UNSAFE)
            return hasNoUnsafe(pos, rope, begin, end);
        int rLen = end - begin;
        if (begin < 0 || end > rope.len) throw new IndexOutOfBoundsException();
        if (pos+rLen > len) return false;

        long fstOff = this.fst.address()+this.fstOff+pos;
        long sndOff = this.snd.address()+this.sndOff+Math.max(0, pos-this.fstLen);
        int fstLen = Math.min(rLen, this.fstLen-pos), sndLen = pos+rLen-this.fstLen;

        if (rope instanceof SegmentRope s) {
            return compare1_2(s.utf8, s.segment.address()+s.offset+begin, rLen,
                              fstU8, fstOff, fstLen, sndU8, sndOff, sndLen) == 0;
        } else {
            byte[] o_fst, o_snd;
            long o_fstOff, o_sndOff;
            int o_fstLen;
            if (rope instanceof TwoSegmentRope t) {
                o_fst = t.fstU8; o_fstOff = t.fst.address()+t.fstOff; o_fstLen = t.fstLen;
                o_snd = t.sndU8; o_sndOff = t.snd.address()+t.sndOff;
            } else {
                Term t = (Term) rope;
                SegmentRope r;
                o_fst = (r = t. first()).utf8; o_fstOff = r.segment.address()+r.offset; o_fstLen = r.len;
                o_snd = (r = t.second()).utf8; o_sndOff = r.segment.address()+r.offset;
            }
            o_fstOff += begin;
            o_sndOff += Math.max(0, begin-o_fstLen);
            o_fstLen = Math.min(o_fstLen, end)-begin;
            return compare2_2(fstU8, fstOff,   fstLen,
                              sndU8, sndOff,   sndLen,
                              o_fst, o_fstOff, o_fstLen,
                              o_snd, o_sndOff, rLen-o_fstLen) == 0;
        }
    }

    public boolean hasNoUnsafe(int pos, Rope rope, int begin, int end) {
        int rLen = end - begin;
        if (begin < 0 || end > rope.len) throw new IndexOutOfBoundsException();
        if (pos+rLen > len) return false;

        long fstOff = this.fstOff+pos, sndOff = this.sndOff+Math.max(0, pos-this.fstLen);
        int fstLen = Math.min(rLen, this.fstLen-pos), sndLen = pos+rLen-this.fstLen;

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

    @Override public boolean equals(Object o) {
        if (o instanceof TwoSegmentRope r) {
            if (r.len != len) {
                return false;
            } else if (fst == r.fst && snd == r.snd && fstOff == r.fstOff && sndOff == r.sndOff
                                             && fstLen == r.fstLen && sndLen == r.sndLen) {
                return true;
            } else if (HAS_UNSAFE) {
                return compare2_2(fstU8, fstOff+fst.address(), fstLen,
                                  sndU8, sndOff+snd.address(), sndLen,
                                  r.fstU8, r.fstOff+r.fst.address(), r.fstLen,
                                  r.sndU8, r.sndOff+r.snd.address(), r.sndLen) == 0;
            } else {
                return safeEqualsTSR(r);
            }
        }
        return super.equals(o);
    }

    private boolean safeEqualsTSR(TwoSegmentRope r) {
        return compare2_2(  fst,   fstOff,   fstLen, snd,   sndOff,   sndLen,
                          r.fst, r.fstOff, r.fstLen, r.snd, r.sndOff, r.sndLen) == 0;
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

    @Override public void appendTo(StringBuilder sb, int begin, int end) {
        try (var d = RopeDecoder.create()) {
            int n = Math.min(fstLen, end)-begin;
            if (n > 0)
                d.write(sb, fst, fstOff+begin, n);
            n = end-begin-n;
            if (n > 0)
                d.write(sb, snd, sndOff+end-n, n);
        }
    }

    @Override public int compareTo(SegmentRope o) {
        if (HAS_UNSAFE) {
            return -compare1_2(o.utf8, o.segment.address()+o.offset, o.len,
                               fstU8, fst.address()+fstOff, fstLen,
                               sndU8, snd.address()+sndOff, sndLen);
        } else {
            return -compare1_2(o.segment, o.offset, o.len,
                    fst, fstOff, fstLen, snd, sndOff, sndLen);
        }
    }

    @Override public int compareTo(TwoSegmentRope o) {
        if (HAS_UNSAFE) {
            return compare2_2(fstU8, fst.address()+fstOff, fstLen,
                              sndU8, snd.address()+sndOff, sndLen,
                              o.fstU8, o.fst.address()+o.fstOff, o.fstLen,
                              o.sndU8, o.snd.address()+o.sndOff, o.sndLen);
        } else {
            return compare2_2(fst, fstOff, fstLen, snd, sndOff, sndLen,
                              o.fst, o.fstOff, o.fstLen, o.snd, o.sndOff, o.sndLen);
        }
    }

    @Override public int compareTo(SegmentRope o, int begin, int end) {
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        if (HAS_UNSAFE) {
            return -compare1_2(o.utf8, o.segment.address()+o.offset + begin, end - begin,
                               fstU8, fst.address()+fstOff, fstLen,
                               sndU8, snd.address()+sndOff, sndLen);
        } else {
            return -compare1_2(o.segment, o.offset + begin, end - begin,
                    fst, fstOff, fstLen, snd, sndOff, sndLen);
        }
    }

    @Override public int compareTo(TwoSegmentRope o, int begin, int end) {
        if (!HAS_UNSAFE)
            return compareToNoUnsafe(o, begin, end);
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        // the following locals simulate o.sub(begin, end)
        long o_fstOff = o.fst.address()+o.fstOff+begin;
        long o_sndOff = o.snd.address()+o.sndOff+Math.max(0, begin-o.fstLen);
        int  o_fstLen = Math.max(0, Math.min(end, o.fstLen)-begin),
             o_sndLen = end-begin-o_fstLen;
        return compare2_2(fstU8, fst.address()+fstOff, fstLen,
                          sndU8, snd.address()+sndOff, sndLen,
                          o.fstU8, o_fstOff, o_fstLen, o.sndU8, o_sndOff, o_sndLen);
    }
    private int compareToNoUnsafe(TwoSegmentRope o, int begin, int end) {
        if (begin < 0 || end > o.len) throw new IndexOutOfBoundsException();
        // the following locals simulate o.sub(begin, end)
        long o_fstOff = o.fstOff+begin, o_sndOff = o.sndOff+Math.max(0, begin-o.fstLen);
        int  o_fstLen = Math.max(0, Math.min(end, o.fstLen)-begin),
                o_sndLen = end-begin-o_fstLen;
        return compare2_2(  fst,   fstOff,   fstLen,   snd,   sndOff,   sndLen,
                o.fst, o_fstOff, o_fstLen, o.snd, o_sndOff, o_sndLen);
    }
}
