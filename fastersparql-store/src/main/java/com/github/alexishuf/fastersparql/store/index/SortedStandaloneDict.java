package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

public final class SortedStandaloneDict extends Dict {
    final long emptyId;

    public SortedStandaloneDict(Path file) throws IOException {
        super(file);
        if ((flags & (byte)(LOCALITY_MASK >>> FLAGS_BIT)) != 0)
            throw new UnsupportedOperationException();
        this.emptyId = nStrings > 0 && readOff(0) == readOff(1) ? MIN_ID : NOT_FOUND;
    }

    @Override public AbstractLookup polymorphicLookup() { return lookup(); }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup extends AbstractLookup {
        private final SegmentRope tmp = new SegmentRope(seg, 0, 0);
        private final SegmentRope out = new SegmentRope();

        public SortedStandaloneDict dict() { return SortedStandaloneDict.this; }

        public long find(PlainRope rope) {
            if (rope.len == 0)
                return emptyId;
            if (UNSAFE != null && rope instanceof SegmentRope s)
                return find(s.segment, s.offset, s.len);
            long lo = 0, hi = nStrings-1;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOff(mid);
                tmp.slice(off, (int) (readOff(mid+1) - off));
                int diff = rope.compareTo(tmp);
                if      (diff < 0) hi   = mid - 1;
                else if (diff > 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        private long find(MemorySegment rSeg, long rOff, int rLen) {

            long lo = 0, hi = nStrings-1;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOff(mid);
                int diff = cmp(off, (int)(readOff(mid+1) - off), rSeg, rOff, rLen);
                if      (diff > 0) hi   = mid - 1;
                else if (diff < 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        public SegmentRope get(long id)  {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOff(id - 1);
            out.wrapSegment(seg, off, (int)(readOff(id) - off));
            return out;
        }
    }
}
