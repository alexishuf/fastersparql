package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public final class SortedStandaloneDict extends Dict {
    final long emptyId;

    public SortedStandaloneDict(Path file) throws IOException {
        super(file);
        if ((flags & (byte)(LOCALITY_MASK >>> FLAGS_BIT)) != 0)
            throw new UnsupportedOperationException();
        this.emptyId = nStrings > 0 && readOff(0) == readOff(1) ? MIN_ID : NOT_FOUND;
        quickValidateOffsets(-1);
    }

    @Override public Orphan<Lookup> polymorphicLookup() { return lookup(); }

    public Orphan<Lookup> lookup() {return LOOKUP.create().thaw(this).releaseOwnership(RECYCLED);}

    private static final Supplier<Lookup> FAC = new Supplier<>() {
        @Override public Lookup get() {return new Lookup.Concrete().takeOwnership(RECYCLED);}
        @Override public String toString() {return "SortedStandaloneDict.FAC";}
    };
    private static final Alloc<Lookup> LOOKUP = new Alloc<>(Lookup.class,
            "SortedStandaloneDict.LOOKUP", LOOKUP_POOL_CAPACITY, FAC, Lookup.BYTES);
    //static { Primer.primeLocal(LOOKUP); }

    public static abstract sealed class Lookup extends AbstractLookup<Lookup> {
        private static final int BYTES = 16 + 4*4 + 2*SegmentRopeView.BYTES + TwoSegmentRope.BYTES;
        private SortedStandaloneDict dict;
        private final SegmentRopeView tmp = new SegmentRopeView();
        private TwoSegmentRope termTmp = null;
        private final SegmentRopeView out = new SegmentRopeView();

        private Lookup() {}

        public @This Lookup thaw(SortedStandaloneDict dict) {
            if (dict != this.dict) {
                this.dict = dict;
                this.tmp.wrap(dict.seg, 0, 1);
            }
            return this;
        }

        @Override public @Nullable Lookup recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            if (LOOKUP.offer(this) != null)
                internalMarkGarbage(RECYCLED);
            return null;
        }

        private static final class Concrete extends Lookup implements Orphan<Lookup> {
            @Override public Lookup takeOwnership(Object o) {return takeOwnership0(o);}
        }

        public SortedStandaloneDict dict() { return dict; }

        public long find(PlainRope rope) {
            var d = dict;
            if (rope.len == 0)
                return d.emptyId;
            if (U == null || !(rope instanceof SegmentRope s))
                return safeFind(rope);
            long lo = 0, hi = d.nStrings-1;
            byte[] rBase = s.utf8;
            long rOff = s.segment.address() + s.offset;
            int rLen = rope.len;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = d.readOffUnsafe(mid);
                int len = (int)(d.readOffUnsafe(mid+1) - off);
                int diff = SegmentRope.compare1_1(null, d.valBase+off, len, rBase, rOff, rLen);
                if      (diff > 0) hi   = mid - 1;
                else if (diff < 0) lo   = mid + 1;
                else               return mid + Dict.MIN_ID;
            }
            return NOT_FOUND;
        }

        private long safeFind(PlainRope rope) {
            var d = dict;
            long lo = 0, hi = d.nStrings-1;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = d.readOffUnsafe(mid);
                tmp.slice(off, (int) (d.readOffUnsafe(mid+1) - off));
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
            var d = dict;
            long lo = 0, hi = d.nStrings-1;
            SegmentRope termFst = term.first(), termSnd = term.second();
            long termFstOff = termFst.segment.address()+termFst.offset;
            long termSndOff = termSnd.segment.address()+termSnd.offset;
            int termFstLen = termFst.len, termSndLen = termSnd.len;
            while (lo <= hi) {
                long mid = ((lo + hi) >>> 1);
                long off = d.readOffUnsafe(mid);
                int len = (int)(d.readOffUnsafe(mid+1) - off);
                int diff = compare1_2(null, d.valBase+off, len,
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
            var d = dict;
            if (id < MIN_ID || id > d.nStrings) return null;
            long off = d.readOffUnsafe(id - 1);
            out.wrap(d.seg, null, off, (int)(d.readOffUnsafe(id) - off));
            return out;
        }
    }
}
