package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.io.IOException;
import java.nio.file.Path;

public final class StandaloneDict extends Dict {
    public StandaloneDict(Path file) throws IOException {
        super(file);
    }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup extends AbstractLookup {
        private final SegmentRope tmp = new SegmentRope(), out = new SegmentRope();

        public StandaloneDict dict() { return StandaloneDict.this; }

        public long find(PlainRope rope) {
            if (rope.len == 0)
                return emptyId;
            long lo = 0, hi = nStrings-1;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOff(mid);
                tmp.wrapSegment(seg, off, (int) (readOff(mid+1) - off));
                int diff = rope.compareTo(tmp);
                if      (diff < 0) hi   = mid - 1;
                else if (diff > 0) lo   = mid + 1;
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
