package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_1;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.SUFFIX;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class LocalityCompositeDict extends Dict {
    public  static final long        SUFFIX_MASK = 0x8000000000000000L;
    public  static final long         SH_ID_MASK = 0x7fffff8000000000L;
    private static final long FLAGGED_SH_ID_MASK = 0x8fffff8000000000L;
    public  static final long        OFF_MASK    = 0x0000007fffffffffL;
    public  static final int SH_ID_BIT  = numberOfTrailingZeros(SH_ID_MASK);

    public static final int SH_ID_SUFF = 0x01000000;

    private final boolean sharedOverflow, embedSharedId;
    private final Splitter.Mode splitMode;
    private final LocalityStandaloneDict sharedDict;

    /**
     * Creates read-only view into the dictionary stored at the given location.
     *
     * <p>The file contents will be memory-mapped. Thus, the {@link Dict} instance will consume
     * negligible heap and native physical memory. Note that good performance depends on enough
     * free native physical memory to keep as many pages as possible in RAM. If the system as a
     * whole has low levels of free physical memory, the operating system will operations on this
     * dict will be more likely to be slowed down by page faults.</p>
     *
     * <p>Changes to the file will be seen in this instance and may cause unexpected results,
     * such as garbage strings, string length overflows and out of bounds exceptions.</p>
     *
     * @param file to be memory-mapped
     * @param shared the shared dictionary for shared ids
     * @throws IOException              if the file could not be opened or if it could not be memory-mapped.
     * @throws IllegalArgumentException if {@code shared} is null but the shared bit is set in
     *                                  {@code file}.
     */
    public LocalityCompositeDict(Path file, LocalityStandaloneDict shared) throws IOException {
        super(file);
        if ((flags & (byte)(SHARED_MASK >> FLAGS_BIT)) == 0)
            throw new UnsupportedOperationException("Not a shared dict");
        if ((flags & (byte)(LOCALITY_MASK >>> FLAGS_BIT)) == 0)
            throw new UnsupportedOperationException("Not a locality-optimized Dict");
        if (shared.emptyId == NOT_FOUND)
            throw new IllegalArgumentException("Shared dict does not contain the empty string");
        this.sharedDict = shared;
        this.sharedOverflow = (flags & (byte)(SHARED_OVF_MASK  >>> FLAGS_BIT)) != 0;
        this.embedSharedId  = (flags & (byte)(OFF_W_MASK       >>> FLAGS_BIT)) != 0;
        boolean prolong     = (flags & (byte)(PROLONG_MASK     >>> FLAGS_BIT)) != 0;
        boolean penultimate = (flags & (byte)(PENULTIMATE_MASK >>> FLAGS_BIT)) != 0;
        if (prolong && penultimate)
            throw new IllegalArgumentException("Dict at "+file+" has both prolong and penultimate flags set");
        this.splitMode = prolong ? Splitter.Mode.PROLONG
                : penultimate ? Splitter.Mode.PENULTIMATE : Splitter.Mode.LAST;
        quickValidateOffsets(OFF_MASK);
    }

    public LocalityCompositeDict(Path file) throws IOException {
        this(file, new LocalityStandaloneDict(file.resolveSibling("shared")));
    }

    @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
        md.offsetsOff   = OFFS_OFF;
        md.offsetsCount = (seg.get(LE_LONG, 0) & STRINGS_MASK) + 1;
        md.offsetWidth  = 8;
        md.valueWidth   = 1;
    }

    @Override public void validate() throws IOException {
        super.validate();
        SortedCompositeDict.validateNtStrings(lookup());
    }

    public LocalityStandaloneDict shared() { return sharedDict; }

    @Override public AbstractLookup polymorphicLookup() { return lookup(); }

    public Lookup lookup()                       { return new Lookup(null ); }
    public Lookup lookup(@Nullable Thread owner) { return new Lookup(owner); }

    public final class Lookup extends AbstractLookup {
        public final @Nullable Thread owner;
        private final SegmentRope tmp = new SegmentRope(seg, 0, 1);
        private final TwoSegmentRope out = new TwoSegmentRope();
        private final TwoSegmentRope termTmp = new TwoSegmentRope();
        private final LocalityStandaloneDict.Lookup shared = sharedDict.lookup();
        private final Splitter split = new Splitter(splitMode);

        public Lookup(@Nullable Thread owner) {
            this.owner = owner;
        }

        @Override public LocalityCompositeDict dict() { return LocalityCompositeDict.this; }

        @Override public long find(PlainRope string) {
            var side = split.split(string);
            int flShId = (side == SUFFIX ? SH_ID_SUFF : 0) | (int)switch (side) {
                case NONE -> sharedDict.emptyId;
                case PREFIX,SUFFIX -> shared.find(split.shared());
            };
            if (embedSharedId) {
                long id = find(flShId, split.local());
                if (sharedOverflow && id == NOT_FOUND)
                    return find((int) sharedDict.emptyId, string);
                return id;
            } else {
                return findB64(string, flShId&~SH_ID_SUFF);
            }
        }

        @Override public long find(Term term) {
            termTmp.wrapFirst(term.first());
            termTmp.wrapSecond(term.second());
            return find(termTmp);
        }

        private long find(int flShId, PlainRope local) {
            if (UNSAFE == null)
                return coldFind(flShId, local);
            long id = 1;
            if (local instanceof SegmentRope s) {
                byte[] rBase = s.utf8; //(byte[]) s.segment.array().orElse(null);
                long rAddr = s.segment.address() + s.offset;
                while (id <= nStrings) {
                    long off = readOffUnsafe(id - 1);
                    int offerFlShId = (int) ((off & FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                    int diff = offerFlShId - flShId;
                    if (diff == 0) {
                        off &= OFF_MASK;
                        diff = (int) ((readOffUnsafe(id) & OFF_MASK) - off);
                        diff = compare1_1(null, valBase + off, diff, rBase, rAddr, s.len);
                        if (diff == 0)
                            return id;
                    }
                    id = (id << 1) + (diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
                }
            } else {
                TwoSegmentRope tsr = (TwoSegmentRope) local;
                byte[] rBase1 = tsr.fstU8;
                byte[] rBase2 = tsr.sndU8;
                long rAddr1 = tsr.fst.address() + tsr.fstOff;
                long rAddr2 = tsr.snd.address() + tsr.sndOff;
                int rLen1 = tsr.fstLen, rLen2 = tsr.sndLen;
                while (id <= nStrings) {
                    long off = readOffUnsafe(id - 1);
                    int offerFlShId = (int) ((off & FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                    int diff = offerFlShId - flShId;
                    if (diff == 0) {
                        off &= OFF_MASK;
                        diff = (int) ((readOffUnsafe(id) & OFF_MASK) - off);
                        diff = compare1_2(null, valBase+off, diff,
                                          rBase1, rAddr1, rLen1, rBase2, rAddr2, rLen2);
                        if (diff == 0)
                            return id;
                    }
                    id = (id << 1) + (diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
                }
            }
            return NOT_FOUND;
        }

        private long coldFind(int flShId, PlainRope local) {
            long id = 1;
            while (id <= nStrings) {
                long off = readOffUnsafe(id - 1);
                int offerFlShId = (int) ((off & FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                int diff = offerFlShId - flShId;
                if (diff == 0) {
                    off &= OFF_MASK;
                    tmp.slice(off, (int) ((readOffUnsafe(id) & OFF_MASK) - off));
                    diff = tmp.compareTo(local);
                    if (diff == 0)
                        return id;
                }
                id = (id << 1) + (diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        private long findB64(PlainRope string, int flShId) {
            MemorySegment b64 = split.b64(flShId).segment();
            long id = findB64(b64, split.local());
            if (sharedOverflow && id == NOT_FOUND) {
                split.b64(sharedDict.emptyId);
                id = findB64(b64, string);
            }
            return id;
        }

        private long findB64(MemorySegment b64, PlainRope local) {
            int id = 1;
            while (id <= nStrings) {
                long off = readOff(id - 1);
                int len = (int)(readOff(id)-off);
                tmp.slice(off, len);
                int diff = compare1_1(b64, 0, 5, seg, off, 5);
                if (diff == 0)
                    diff =  local.compareTo(tmp, 5, len);
                id = (id << 1) + (~diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        public SegmentRope getShared(long id) {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOffUnsafe(id-1);
            return shared.get(embedSharedId ? (off & SH_ID_MASK) >>> SH_ID_BIT
                                            : Splitter.decode(seg, off));
        }

        public boolean sharedSuffixed(long id)  {
            if (id < MIN_ID || id > nStrings) return false;
            long off = readOffUnsafe(id - 1);
            return embedSharedId ? (off & SUFFIX_MASK) != 0
                                 : readValue(off+4) == SharedSide.SUFFIX_CHAR;
        }

        @Override public TwoSegmentRope get(long id) {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOffUnsafe(id - 1);
            int len;
            SegmentRope sharedRope;
            boolean flip;
            if (embedSharedId) {
                flip = (off & SUFFIX_MASK) != 0;
                sharedRope = shared.get((off & SH_ID_MASK) >>> SH_ID_BIT);
                off &= OFF_MASK;
                len = (int) ((readOffUnsafe(id) & OFF_MASK) - off);
            } else {
                len = (int)(readOffUnsafe(id) - off);
                flip = seg.get(JAVA_BYTE, off+4) == SharedSide.SUFFIX_CHAR;
                sharedRope = shared.get(Splitter.decode(seg, off));
            }
            if (sharedRope == null)
                throw new BadSharedId(id, LocalityCompositeDict.this, off, len);
            out.wrapFirst(sharedRope);
            out.wrapSecond(seg, null, off, len);
            if (flip)
                out.flipSegments();
            return out;
        }
    }
}
