package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.store.index.Splitter.SharedSide.SUFFIX;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class CompositeDict extends Dict {
    private final boolean sharedOverflow;
    private final Splitter.Mode splitMode;
    private final @Nullable StandaloneDict shared;

    public CompositeDict(Path file, @Nullable StandaloneDict shared) throws IOException {
        super(file);
        long stringsAndFlags = seg.get(LE_LONG, 0);
        if ((stringsAndFlags & SHARED_MASK) == 0) {
            if (shared != null)
                shared.close();
            this.shared = null;
            this.sharedOverflow = false;
        } else if (shared == null) {
            throw new IllegalArgumentException("Dict at "+file+" requires a shared Dict");
        } else {
            if (shared.emptyId == NOT_FOUND)
                throw new IllegalArgumentException("Shared dict does not contain the empty string");
            this.shared = shared;
            this.sharedOverflow = (stringsAndFlags & SHARED_OVF_MASK) != 0;
        }
        boolean prolong     = (stringsAndFlags & PROLONG_MASK) != 0;
        boolean penultimate = (stringsAndFlags & PENULTIMATE_MASK) != 0;
        if (prolong && penultimate)
            throw new IllegalArgumentException("Dict at "+file+" has both prolong and penultimate flags set");
        this.splitMode = prolong ? Splitter.Mode.PROLONG
                : penultimate ? Splitter.Mode.PENULTIMATE : Splitter.Mode.LAST;
    }


    @Override public void validate() throws IOException {
        super.validate();
        var lookup = lookup();
        for (long id = 1; id <= nStrings; id++) {
            PlainRope t = lookup.get(id);
            if (t == null)
                throw new IOException("Malformed "+this+": could not get id "+id);
            switch (t.len == 0 ? 0 : t.get(0)) {
                case 0 -> {
                    if (id != 1)
                        throw new IOException("Malformed "+this+": empty string with id != 1");
                }
                case '"' -> {
                    int i = t.skipUntilUnescaped(1, t.len, '"');
                    if (i == t.len || t.skipUntil(i+1, t.len, '"') != t.len)
                        throw new IOException("Malformed "+this+": bad literal at id "+id+" "+t);
                }
                case '<' -> {
                    int i = t.skipUntilUnescaped(0, t.len, '>');
                    if (i != t.len-1)
                        throw new IOException("Malformed "+this+": bad IRI at id "+id+": "+t);
                }
                case '_' -> {
                    if (t.len < 3 || t.get(1) != ':')
                        throw new IOException("Malformed "+this+": bad bnode at id "+id+": "+t);
                }
                default ->
                        throw new IOException("Malformed "+this+": not an RDF term at id "+id+": "+t);
            }
        }
    }

    /** Get the shared strings {@link Dict} or {@code null} if this is a standalone dict. */
    public @Nullable Dict shared() { return shared; }

    public Lookup lookup() { return new CompositeLookup(); }

    public final class CompositeLookup extends Lookup {
        private final Lookup shared = CompositeDict.this.shared == null
                                    ? null : CompositeDict.this.shared.lookup();
        private final SegmentRope tmp =  new SegmentRope();
        private final TwoSegmentRope out = new TwoSegmentRope();
        private final Splitter split = new Splitter(splitMode);

        @Override public CompositeDict dict() { return CompositeDict.this; }


        public long find(PlainRope rope) {
            if (rope.len == 0)
                return emptyId;
            var b64 = split.b64(switch (split.split(rope)) {
                case NONE -> MIN_ID;
                case PREFIX,SUFFIX -> shared.find(split.shared());
            });
            long id = find(b64, split.local());
            if (id == NOT_FOUND && sharedOverflow)
                id = find(split.b64(EMPTY_ID), rope);
            return id;
        }

        private long find(SegmentRope b64, PlainRope local) {
            long lo = 0, hi = nStrings-1;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = readOff(mid);
                int len = (int) (readOff(mid+1) - off);
                tmp.wrapSegment(seg, off, len);
                int diff = b64.compareTo(tmp, 0, Math.min(b64.len, len));
                if (diff == 0)
                    diff = local.compareTo(tmp, b64.len, len);
                if      (diff < 0) hi   = mid - 1;
                else if (diff > 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        public PlainRope get(long id)  {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOff(id - 1);
            int len = (int)(readOff(id) - off);
            long sId = Splitter.decode(seg, off);
            SegmentRope sharedRope = (SegmentRope) this.shared.get(sId);
            if (sharedRope == null)
                throw new BadSharedId(id, CompositeDict.this, off, len);
            out.wrapFirst(sharedRope);
            out.wrapSecond(seg, off+5, len-5);
            if (Splitter.SharedSide.fromConcatChar(seg.get(JAVA_BYTE, off+4)) == SUFFIX)
                out.flipSegments();
            return out;
        }
    }
}
