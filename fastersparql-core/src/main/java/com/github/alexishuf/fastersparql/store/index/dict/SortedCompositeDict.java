package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.SUFFIX_CHAR;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class SortedCompositeDict extends Dict {
    private final boolean sharedOverflow;
    private final Splitter.Mode splitMode;
    private final SortedStandaloneDict sharedDict;

    public SortedCompositeDict(Path file, SortedStandaloneDict shared) throws IOException {
        super(file);
        long stringsAndFlags = seg.get(LE_LONG, 0);
        if ((stringsAndFlags & SHARED_MASK) == 0) {
            throw new UnsupportedOperationException("Wrong Dict implementation");
        } else if (shared == null) {
            throw new IllegalArgumentException("Dict at "+file+" requires a shared Dict");
        } else {
            if (shared.emptyId == NOT_FOUND)
                throw new IllegalArgumentException("Shared dict does not contain the empty string");
            this.sharedDict = shared;
            this.sharedOverflow = (stringsAndFlags & SHARED_OVF_MASK) != 0;
        }
        boolean prolong     = (stringsAndFlags & PROLONG_MASK) != 0;
        boolean penultimate = (stringsAndFlags & PENULTIMATE_MASK) != 0;
        if (prolong && penultimate)
            throw new IllegalArgumentException("Dict at "+file+" has both prolong and penultimate flags set");
        this.splitMode = prolong ? Splitter.Mode.PROLONG
                : penultimate ? Splitter.Mode.PENULTIMATE : Splitter.Mode.LAST;
    }


    public SortedCompositeDict(Path file) throws IOException {
        this(file, new SortedStandaloneDict(file.resolveSibling("shared")));
    }

    static void validateNtStrings(AbstractLookup lookup) throws IOException {
        @SuppressWarnings("resource") Dict dict = lookup.dict();
        for (long id = 1, n = dict.strings(); id <= n; id++) {
            PlainRope t = lookup.get(id);
            if (t == null)
                throw new IOException("Malformed "+dict+": could not get id "+id);
            switch (t.len == 0 ? 0 : t.get(0)) {
                case 0 -> {
                    if (id != 1)
                        throw new IOException("Malformed "+dict+": empty string with id != 1");
                }
                case '"' -> {
                    int i = t.skipUntilUnescaped(1, t.len, '"');
                    if (i == t.len || t.skipUntil(i+1, t.len, '"') != t.len)
                        throw new IOException("Malformed "+dict+": bad literal at id "+id+" "+t);
                }
                case '<' -> {
                    int i = t.skipUntilUnescaped(0, t.len, '>');
                    if (i != t.len-1)
                        throw new IOException("Malformed "+dict+": bad IRI at id "+id+": "+t);
                }
                case '_' -> {
                    if (t.len < 3 || t.get(1) != ':')
                        throw new IOException("Malformed "+dict+": bad bnode at id "+id+": "+t);
                }
                default ->
                        throw new IOException("Malformed "+dict+": not an RDF term at id "+id+": "+t);
            }
        }
    }

    @Override public void validate() throws IOException {
        super.validate();
        validateNtStrings(lookup());
    }

    /** Get the shared strings {@link Dict} or {@code null} if this is a standalone dict. */
    public @Nullable Dict shared() { return sharedDict; }

    @Override public AbstractLookup polymorphicLookup() { return lookup(); }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup extends AbstractLookup {
        private final AbstractLookup shared = sharedDict == null ? null : sharedDict.lookup();
        private final SegmentRope tmp =  new SegmentRope(seg, 0, 1);
        private final TwoSegmentRope out = new TwoSegmentRope();
        private final TwoSegmentRope termTmp = new TwoSegmentRope();
        private final Splitter split = new Splitter(splitMode);
        private final byte[] b64Base = (byte[]) split.b64(MIN_ID).segment.array().orElse(null);
        private final long b64Off = split.b64(MIN_ID).segment.address();

        @Override public SortedCompositeDict dict() { return SortedCompositeDict.this; }

        @Override public long find(PlainRope rope) {
            var b64 = split.b64(switch (split.split(rope)) {
                case NONE -> sharedDict.emptyId;
                case PREFIX,SUFFIX -> shared.find(split.shared());
            });
            long id = find(b64, split.local());
            if (id == NOT_FOUND && sharedOverflow) {
                split.b64(sharedDict.emptyId);
                id = find(b64, rope);
            }
            return id;
        }

        @Override public long find(Term term) {
            termTmp.wrapFirst(term.first());
            termTmp.wrapSecond(term.second());
            return find(termTmp);
        }

        private long find(SegmentRope b64, PlainRope local) {
            long lo = 0, hi = nStrings-1;
            if (UNSAFE != null && local instanceof SegmentRope s) {
                byte[] lBase = s.utf8; //(byte[]) s.segment.array().orElse(null);
                long lOff = s.segment.address() + s.offset;
                int lLen = local.len;
                while (lo <= hi) {
                    long mid = ((lo + hi) >>> 1);
                    long off = readOffUnsafe(mid);
                    int len = (int) (readOffUnsafe(mid + 1) - off);
                    int diff = SegmentRope.compare1_1(null, valBase + off, 5, b64Base, b64Off, 5);
                    if (diff == 0)
                        diff = SegmentRope.compare1_1(null, valBase + off + 5, len - 5, lBase, lOff, lLen);
                    if      (diff > 0) hi = mid - 1;
                    else if (diff < 0) lo = mid + 1;
                    else               return mid + Dict.MIN_ID;
                }
            } else {
                while (lo <= hi) {
                    long mid = ((lo + hi) >>> 1);
                    long off = readOffUnsafe(mid);
                    int len = (int) (readOffUnsafe(mid+1) - off);
                    tmp.slice(off, len);
                    int diff = b64.compareTo(tmp, 0, Math.min(b64.len, len));
                    if (diff == 0)
                        diff = local.compareTo(tmp, b64.len, len);
                    if      (diff < 0) hi   = mid - 1;
                    else if (diff > 0) lo   = mid + 1;
                    else               return mid + Dict.MIN_ID;
                }
            }
            return NOT_FOUND;
        }


        @Override public TwoSegmentRope get(long id)  {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOffUnsafe(id - 1);
            int len = (int)(readOffUnsafe(id) - off);
            long sId = Splitter.decode(seg, off);
            SegmentRope sharedRope = (SegmentRope) this.shared.get(sId);
            if (sharedRope == null)
                throw new BadSharedId(id, SortedCompositeDict.this, off, len);
            out.wrapFirst(sharedRope);
            out.wrapSecond(seg, off+5, len-5);
            if (seg.get(JAVA_BYTE, off+4) == SUFFIX_CHAR)
                out.flipSegments();
            return out;
        }
    }
}
