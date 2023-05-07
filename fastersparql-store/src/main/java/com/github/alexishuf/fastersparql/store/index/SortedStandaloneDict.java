package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.U8_UNSAFE_BASE;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.cmp;

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
        private final SegmentRope tmp = new SegmentRope(seg, 0, 1);
        private final SegmentRope out = new SegmentRope();

        public SortedStandaloneDict dict() { return SortedStandaloneDict.this; }

        public long find(PlainRope rope) {
            if (rope.len == 0)
                return emptyId;
            long lo = 0, hi = nStrings-1;
            if (UNSAFE != null && rope instanceof SegmentRope s) {
                Object rBase = s.segment.array().orElse(null);
                long rOff = s.segment.address() + (rBase == null ? 0 : U8_UNSAFE_BASE) + s.offset;
                int rLen = rope.len;
                while (lo <= hi) {
                    long mid = ((lo + hi) >>> 1);
                    long off = readOffUnsafe(mid);
                    int len = (int)(readOffUnsafe(mid+1) - off);
                    int diff = cmp(null, valBase+off, len, rBase, rOff, rLen);
                    if      (diff > 0) hi   = mid - 1;
                    else if (diff < 0) lo   = mid + 1;
                    else               return mid + Dict.MIN_ID;
                }
            } else {
                while (lo <= hi) {
                    long mid = ((lo + hi) >>> 1);
                    long off = readOffUnsafe(mid);
                    tmp.slice(off, (int) (readOffUnsafe(mid+1) - off));
                    int diff = rope.compareTo(tmp);
                    if      (diff < 0) hi   = mid - 1;
                    else if (diff > 0) lo   = mid + 1;
                    else               return mid + Dict.MIN_ID;
                }
            }
            return NOT_FOUND;
        }

        public SegmentRope get(long id)  {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOffUnsafe(id - 1);
            out.wrapSegment(seg, off, (int)(readOffUnsafe(id) - off));
            return out;
        }
    }
}
