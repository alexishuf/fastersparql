package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_1;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.SUFFIX;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class LocalityCompositeDict extends Dict {
    public  static final long        SUFFIX_MASK = 0x8000000000000000L;
    public  static final long         SH_ID_MASK = 0x7fffff8000000000L;
    private static final long FLAGGED_SH_ID_MASK = 0x8fffff8000000000L;
    public  static final long        OFF_MASK    = 0x0000007fffffffffL;
    public  static final int SH_ID_BIT  = numberOfTrailingZeros(SH_ID_MASK);

    private static final AtomicInteger nextThreadLocalBucket = new AtomicInteger();

    public static final int SH_ID_SUFF = 0x01000000;


    private final boolean sharedOverflow, embedSharedId;
    private final byte tlDictId;
    private final Splitter.Mode splitMode;
    private final LocalityStandaloneDict sharedDict;
    private final long[] litSuffixes;

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
        this.tlDictId = (byte)nextThreadLocalBucket.getAndIncrement();
        this.sharedOverflow = (flags & (byte)(SHARED_OVF_MASK  >>> FLAGS_BIT)) != 0;
        this.embedSharedId  = (flags & (byte)(OFF_W_MASK       >>> FLAGS_BIT)) != 0;
        boolean prolong     = (flags & (byte)(PROLONG_MASK     >>> FLAGS_BIT)) != 0;
        boolean penultimate = (flags & (byte)(PENULTIMATE_MASK >>> FLAGS_BIT)) != 0;
        if (prolong && penultimate)
            throw new IllegalArgumentException("Dict at "+file+" has both prolong and penultimate flags set");
        this.splitMode = prolong ? Splitter.Mode.PROLONG
                : penultimate ? Splitter.Mode.PENULTIMATE : Splitter.Mode.LAST;
        quickValidateOffsets(OFF_MASK);
        this.litSuffixes = scanLitSuffixes();
    }

    private long[] scanLitSuffixes() {
        long[] litSuffixes = ArrayAlloc.longsAtLeast(16);
        try (var lGuard = new Guard<LocalityStandaloneDict.Lookup>(this)) {
            var l = lGuard.set(sharedDict.lookup().takeOwnership(this));
            int nLangSuffixes = 0;
            for (long id = 1; id < sharedDict.nStrings; id++) {
                SegmentRope r = l.get(id);
                if (r != null && r.len > 0 && r.get(0) == '"') {
                    if (nLangSuffixes == litSuffixes.length)
                        litSuffixes = ArrayAlloc.grow(litSuffixes, nLangSuffixes<<1);
                    litSuffixes[nLangSuffixes++] = id;
                }
            }
            return Arrays.copyOf(litSuffixes, nLangSuffixes);
        } finally {
            ArrayAlloc.LONG.offer(litSuffixes, litSuffixes.length);
        }
    }

    public LocalityCompositeDict(Path file) throws IOException {
        this(file, new LocalityStandaloneDict(file.resolveSibling("shared")));
    }

    @Override public void close() {
        super.close();
        for (int i = tlSlot(0); i < tl.length; i += TL_DICTS_PER_THREAD) {
            var l = tl[i];
            if (l != null && l.dict == this && TL.compareAndExchangeAcquire(tl, i, l, null) == l)
                l.toGeneralPool();
        }
    }

    @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
        md.offsetsOff   = OFFS_OFF;
        md.offsetsCount = (seg.get(LE_LONG, 0) & STRINGS_MASK) + 1;
        md.offsetWidth  = 8;
        md.valueWidth   = 1;
    }

    @Override public void validate() throws IOException {
        super.validate();
        try (var lG = new Guard<Lookup>(this)) {
            SortedCompositeDict.validateNtStrings(lG.set(lookup()));
        }
    }

    @SuppressWarnings("unused") public LocalityStandaloneDict shared() { return sharedDict; }

    @Override public Orphan<Lookup> polymorphicLookup() { return lookup(); }

    public Orphan<Lookup> lookup() {
        return lookup((int)Thread.currentThread().threadId());
    }

    public Orphan<Lookup> lookup(int threadId) {
        int slot = tlSlot(threadId);
        var l = tl[slot];
        if (l == null || l.dict != this || TL.compareAndExchangeAcquire(tl, slot, l, null) != l)
            l = LOOKUP.create(threadId).thaw(this);
        return l.releaseOwnership(RECYCLED);
    }

    public Orphan<LocalityLexIt> lexIt() { return new LocalityLexIt.Concrete(lookup(), litSuffixes); }

    private static final int TL_DICTS_PER_THREAD = 64;
    private static final int TL_DICTS_PER_THREAD_MASK = TL_DICTS_PER_THREAD-1;
    private static final int TL_DICTS_PER_THREAD_SHIFT = numberOfTrailingZeros(TL_DICTS_PER_THREAD);
    private static final int TL_THREADS = 2*Alloc.THREADS;
    private static final int TL_THREADS_MASK = TL_THREADS-1;
    static { assert Integer.bitCount(TL_DICTS_PER_THREAD)   == 1; }
    static { assert Integer.bitCount(TL_THREADS) == 1; }
    private static final Lookup[] tl = new Lookup[TL_THREADS* TL_DICTS_PER_THREAD];
    private static final VarHandle TL = MethodHandles.arrayElementVarHandle(Lookup[].class);

    private int tlSlot(int threadId) {
        return ((threadId&TL_THREADS_MASK)<<TL_DICTS_PER_THREAD_SHIFT)
                + (tlDictId&TL_DICTS_PER_THREAD_MASK);
    }

    private static final Supplier<Lookup> LOOKUP_FAC = new Supplier<>() {
        @Override public Lookup get() {return new Lookup.Concrete().takeOwnership(RECYCLED);}
        @Override public String toString() {return "LocalityCompositeDict.LOOKUP_FAC";}
    };
    private static final Alloc<Lookup> LOOKUP = new Alloc<>(Lookup.class,
            "LocalityCompositeDict.LOOKUP",
            Math.max(LOOKUP_POOL_CAPACITY-tl.length, Alloc.THREADS*32),
            LOOKUP_FAC, Lookup.BYTES);
    static { Primer.INSTANCE.sched(LOOKUP::prime); }

    public static abstract sealed class Lookup extends AbstractLookup<Lookup> {
        public static final int BYTES = 16 + 8*4 /*fields*/
                + SegmentRopeView.BYTES /* SegmentRopeView */
                + 2*TwoSegmentRope.BYTES /* TwoSegmentRope */
                + LocalityStandaloneDict.Lookup.BYTES
                + Splitter.BYTES;

        private LocalityCompositeDict dict;
        private final SegmentRopeView tmp = new SegmentRopeView();
        private final TwoSegmentRope out = new TwoSegmentRope();
        private final TwoSegmentRope termTmp = new TwoSegmentRope();
        private LocalityStandaloneDict.Lookup shared;
        private final Splitter split = Splitter.create(Splitter.Mode.LAST).takeOwnership(this);

        private Lookup() {}

        @Override public @Nullable Lookup recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            int threadId = (int)Thread.currentThread().threadId();
            Lookup evicted = dict.open
                    ? (Lookup)TL.getAndSetAcquire(tl, dict.tlSlot(threadId), this)
                    : this;
            if (evicted != null)
                evicted.toGeneralPool();
            return null;
        }

        private void toGeneralPool() {
            shared = Owned.safeRecycle(shared, this);
            dict = null;
            if (LOOKUP.offer(this) != null)
                internalMarkGarbage(RECYCLED);
        }

        @Override protected @Nullable Lookup internalMarkGarbage(Object currentOwner) {
            super.internalMarkGarbage(currentOwner);
            split.recycle(this);
            Owned.safeRecycle(shared, this);
            return null;
        }

        private @This Lookup thaw(LocalityCompositeDict dict) {
            if (dict != this.dict) {
                this.dict = dict;
                this.tmp.wrap(dict.seg, null, 0, 1);
                this.split.mode(dict.splitMode);
                Owned.recycle(this.shared, this);
                this.shared = dict.sharedDict.lookup().takeOwnership(this);
            }
            return this;
        }

        private static final class Concrete extends Lookup implements Orphan<Lookup> {
            @Override public Lookup takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public LocalityCompositeDict dict() { return dict; }


        @Override public long find(PlainRope string) {
            var d = this.dict;
            var side = split.split(string);
            int flShId = (side == SUFFIX ? SH_ID_SUFF : 0) | (int)switch (side) {
                case NONE -> d.sharedDict.emptyId;
                case PREFIX,SUFFIX -> shared.find(split.shared());
            };
            if (d.embedSharedId) {
                long id = find(flShId, split.local());
                if (d.sharedOverflow && id == NOT_FOUND)
                    return find((int)d.sharedDict.emptyId, string);
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
            if (U == null)
                return coldFind(flShId, local);
            var d = dict;
            long id = 1;
            if (local instanceof SegmentRope s) {
                byte[] rBase = s.utf8; //(byte[]) s.segment.array().orElse(null);
                long rAddr = s.segment.address() + s.offset;//(byte[]) s.segment.array().orElse(null);
                if (d.offShift == 3) {
                    while (id <= d.nStrings) {
                        long off = d.readOffUnsafeL(id - 1);
                        int offerFlShId = (int)((off&FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                        int diff = offerFlShId - flShId;
                        if (diff == 0) {
                            off &= OFF_MASK;
                            diff = (int)((d.readOffUnsafeL(id)&OFF_MASK) - off);
                            diff = compare1_1(null, d.valBase+off, diff, rBase, rAddr, s.len);
                            if (diff == 0)
                                return id;
                        }
                        id = (id << 1) + (diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
                    }
                } else {
                    while (id <= d.nStrings) {
                        long off = d.readOffUnsafeI(id - 1);
                        int offerFlShId = (int)((off&FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                        int diff = offerFlShId - flShId;
                        if (diff == 0) {
                            off &= OFF_MASK;
                            diff = (int)((d.readOffUnsafeI(id)&OFF_MASK) - off);
                            diff = compare1_1(null, d.valBase+off, diff, rBase, rAddr, s.len);
                            if (diff == 0)
                                return id;
                        }
                        id = (id << 1) + (diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
                    }
                }
            } else {
                TwoSegmentRope tsr = (TwoSegmentRope) local;
                byte[] rBase1 = tsr.fstU8;
                byte[] rBase2 = tsr.sndU8;
                long rAddr1 = tsr.fst.address() + tsr.fstOff;
                long rAddr2 = tsr.snd.address() + tsr.sndOff;
                int rLen1 = tsr.fstLen, rLen2 = tsr.sndLen;
                while (id <= d.nStrings) {
                    long off = d.readOffUnsafe(id - 1);
                    int offerFlShId = (int) ((off & FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                    int diff = offerFlShId - flShId;
                    if (diff == 0) {
                        off &= OFF_MASK;
                        diff = (int) ((d.readOffUnsafe(id) & OFF_MASK) - off);
                        diff = compare1_2(null, d.valBase+off, diff,
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
            var d = this.dict;
            long id = 1;
            while (id <= d.nStrings) {
                long off = d.readOffUnsafe(id - 1);
                int offerFlShId = (int) ((off & FLAGGED_SH_ID_MASK) >>> SH_ID_BIT);
                int diff = offerFlShId - flShId;
                if (diff == 0) {
                    off &= OFF_MASK;
                    tmp.slice(off, (int) ((d.readOffUnsafe(id) & OFF_MASK) - off));
                    diff = tmp.compareTo(local);
                    if (diff == 0)
                        return id;
                }
                id = (id << 1) + (diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        private long findB64(PlainRope string, int flShId) {
            var d = this.dict;
            MemorySegment b64 = split.b64(flShId).segment();
            long id = findB64(b64, split.local());
            if (d.sharedOverflow && id == NOT_FOUND) {
                split.b64(d.sharedDict.emptyId);
                id = findB64(b64, string);
            }
            return id;
        }

        private long findB64(MemorySegment b64, PlainRope local) {
            var d = this.dict;
            int id = 1;
            while (id <= d.nStrings) {
                long off = d.readOff(id - 1);
                int len = (int)(d.readOff(id)-off);
                int diff = compare1_1(b64, 0, 5, d.seg, off, 5);
                if (diff == 0) {
                    tmp.slice(off, len);
                    diff = local.compareTo(tmp, 5, len);
                    if (diff == 0)
                        return id;
                }
                id = (id << 1) + (~diff >>> 31); // = shId#local < candidate ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        public SegmentRopeView getShared(long id) {
            var d = this.dict;
            if (id < MIN_ID || id > d.nStrings) return null;
            long off = d.readOffUnsafe(id-1);
            return shared.get(d.embedSharedId ? (off & SH_ID_MASK) >>> SH_ID_BIT
                                            : Splitter.decode(d.seg, off));
        }

        public SegmentRopeView getLocal(long id) {
            var d = this.dict;
            if (id < MIN_ID || id > d.nStrings) return null;
            long off = d.readOffUnsafe(id-1);
            int len;
            if (d.embedSharedId) {
                off &= OFF_MASK;
                len = (int)((d.readOff(id)&OFF_MASK) - off);
            } else {
                len = (int)(d.readOff(id)-off);
            }
            tmp.slice(off, len);
            return tmp;
        }

        public boolean sharedSuffixed(long id)  {
            var d = this.dict;
            if (id < MIN_ID || id > d.nStrings) return false;
            long off = d.readOffUnsafe(id - 1);
            return d.embedSharedId ? (off & SUFFIX_MASK) != 0
                                 : d.readValue(off+4) == SharedSide.SUFFIX_CHAR;
        }

        @Override public TwoSegmentRope get(long id) {
            var d = this.dict;
            if (id < MIN_ID || id > d.nStrings) return null;
            long off = d.readOffUnsafe(id - 1);
            int len;
            SegmentRope sharedRope;
            boolean flip;
            if (d.embedSharedId) {
                flip = (off & SUFFIX_MASK) != 0;
                sharedRope = shared.get((off & SH_ID_MASK) >>> SH_ID_BIT);
                off &= OFF_MASK;
                len = (int) ((d.readOffUnsafe(id) & OFF_MASK) - off);
            } else {
                len = (int)(d.readOffUnsafe(id) - off);
                flip = d.seg.get(JAVA_BYTE, off+4) == SharedSide.SUFFIX_CHAR;
                sharedRope = shared.get(Splitter.decode(d.seg, off));
            }
            if (sharedRope == null)
                throw new BadSharedId(id, d, off, len);
            out.wrapFirst(sharedRope);
            out.wrapSecond(d.seg, null, off, len);
            if (flip)
                out.flipSegments();
            return out;
        }
    }

    public static class LocalityLexIt extends LexIt<LocalityLexIt> {
        private static final int BLANK = -3;
        private static final int IRI   = -2;
        private static final int PLAIN = -1;
        private final Lookup lookup;
        private final MutableRope string;
        private final long[] suffixes;
        private int lexEnd, suffix;

        private LocalityLexIt(Orphan<Lookup> lookup, long[] suffixes) {
            this.lookup = lookup.takeOwnership(this);
            this.suffixes = suffixes;
            this.string = new MutableRope(24);
            end();
        }

        private static final class Concrete extends LocalityLexIt implements Orphan<LocalityLexIt> {
            private Concrete(Orphan<Lookup> lookup, long[] suffixes) {super(lookup, suffixes);}
            @Override public LocalityLexIt takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public @Nullable LocalityLexIt recycle(Object currentOwner) {
            internalMarkGarbage(currentOwner);
            lookup.recycle(this);
            string.close();
            return null;
        }

        @Override public void find(PlainRope nt) {
            requireAlive();
            boolean bad = nt.len < 2;
            if (!bad) {
                byte[] u8 = string.clear().ensureFreeCapacity(nt.len+2).u8();
                byte first = nt.get(0);
                switch (first) {
                    case '"', '<' -> {
                        lexEnd = first == '<' ? nt.len-1
                               : nt.reverseSkipUntil(0, nt.len, '"');
                        if (lexEnd > 0) {
                            u8[0] = '_'; u8[1] = ':';
                            string.len = 2;
                            string.append(nt, 1, lexEnd);
                        } else {
                            bad = true;
                        }
                    }
                    case '_' -> {
                        lexEnd = nt.len-1;
                        string.append(nt);
                    }
                    default -> bad = true;
                }
            }
            if (bad) {
                end();
            } else {
                suffix = BLANK;
                string.ensureFreeCapacity(6); /* "@en-US */
            }
        }

        @Override public void end() {
            lexEnd = 1;
            id = NOT_FOUND;
            suffix = suffixes.length;
        }

        @Override public boolean advance() {
            requireAlive();
            long[] suffixes = this.suffixes;
            byte[] u8 = string.u8();
            int suffix = this.suffix;
            id = NOT_FOUND;
            while (id == NOT_FOUND && suffix < suffixes.length) {
                switch (suffix) {
                    case BLANK -> suffix = IRI;
                    case IRI -> {
                        System.arraycopy(u8, 2, u8, 1, string.len-2);
                        u8[0] = '<';
                        u8[lexEnd] = '>';
                        suffix = PLAIN;
                    }
                    case PLAIN -> {
                        u8[0] = '"'; u8[lexEnd] = '"';
                        suffix = 0;
                    }
                    default -> {
                        string.len = lexEnd;
                        string.append(lookup.shared.get(suffixes[suffix++]));
                    }
                }
                id = lookup.find(string);
            }
            this.suffix = suffix;
            return id != NOT_FOUND;
        }
    }
}
