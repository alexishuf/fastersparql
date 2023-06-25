package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;

public final class SortedStandaloneDict extends Dict {
    final long emptyId;

    public SortedStandaloneDict(Path file) throws IOException {
        super(file);
        if ((flags & (byte)(LOCALITY_MASK >>> FLAGS_BIT)) != 0)
            throw new UnsupportedOperationException();
        this.emptyId = nStrings > 0 && readOff(0) == readOff(1) ? MIN_ID : NOT_FOUND;
        quickValidateOffsets(-1);
    }

    @Override public AbstractLookup polymorphicLookup() { return lookup(); }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup extends AbstractLookup {
        private final SegmentRope tmp = new SegmentRope(seg, 0, 1);
        private TwoSegmentRope termTmp = null;
        private final SegmentRope out = new SegmentRope();

        public SortedStandaloneDict dict() { return SortedStandaloneDict.this; }

        public long find(PlainRope rope) {
            if (rope.len == 0)
                return emptyId;
            if (U == null || !(rope instanceof SegmentRope s))
                return safeFind(rope);
            long lo = 0, hi = nStrings-1;
            byte[] rBase = s.utf8;
            long rOff = s.segment.address() + s.offset;
            int rLen = rope.len;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOffUnsafe(mid);
                int len = (int)(readOffUnsafe(mid+1) - off);
                int diff = SegmentRope.compare1_1(null, valBase+off, len, rBase, rOff, rLen);
                if      (diff > 0) hi   = mid - 1;
                else if (diff < 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        private long safeFind(PlainRope rope) {
            long lo = 0, hi = nStrings-1;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOffUnsafe(mid);
                tmp.slice(off, (int) (readOffUnsafe(mid+1) - off));
                int diff = rope.compareTo(tmp);
                if      (diff < 0) hi   = mid - 1;
                else if (diff > 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        @Override public long find(Term term) {
            if (term == null)
                return NOT_FOUND;
            if (U == null)
                return safeFind(term);
            long lo = 0, hi = nStrings-1;
            SegmentRope termFst = term.first(), termSnd = term.second();
            long termFstOff = termFst.segment.address()+termFst.offset;
            long termSndOff = termSnd.segment.address()+termSnd.offset;
            int termFstLen = termFst.len, termSndLen = termSnd.len;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOffUnsafe(mid);
                int len = (int)(readOffUnsafe(mid+1) - off);
                int diff = compare1_2(null, valBase+off, len,
                                      termFst.utf8, termFstOff, termFstLen,
                                      termSnd.utf8, termSndOff, termSndLen);
                if      (diff > 0) hi   = mid - 1;
                else if (diff < 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        private long safeFind(Term term) {
            TwoSegmentRope termTmp = this.termTmp;
            if (termTmp == null) this.termTmp = termTmp = new TwoSegmentRope();
            termTmp.wrapFirst(term.first());
            termTmp.wrapSecond(term.second());
            return safeFind(termTmp);
        }

        public SegmentRope get(long id)  {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOffUnsafe(id - 1);
            out.wrapSegment(seg, null, off, (int)(readOffUnsafe(id) - off));
            return out;
        }
    }
}
