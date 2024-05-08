package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_1;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public class LocalityStandaloneDict extends Dict {
    final long emptyId;

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
     * @throws IOException              if the file could not be opened or if it could not be memory-mapped.
     * @throws IllegalArgumentException if {@code shared} is null but the shared bit is set in
     *                                  {@code file}.
     */
    public LocalityStandaloneDict(Path file) throws IOException {
        super(file);
        if ((flags & (byte)(LOCALITY_MASK >>> FLAGS_BIT)) == 0)
            throw new UnsupportedOperationException();
        // the empty string is the absolute minimal string, thus it is in the leftmost node
        // of the tree. simulate moving left until we reach the leaf and check if it is the
        // empty string
        if (nStrings == 0) {
            this.emptyId = NOT_FOUND;
        } else {
            // the mepty string is the absolute minimal item, walk to the leftmost node
            long id = 1;
            while (id << 1 <= nStrings) id <<= 1;
            // check if the leftmost node is the mepty string
            this.emptyId = readOff(id-1) == readOff(id) ? id : NOT_FOUND;
        }
        quickValidateOffsets(-1);
    }

    @Override public Orphan<Lookup> polymorphicLookup() { return lookup(); }

    public Orphan<Lookup> lookup() {
        Lookup l = LOOKUP.create();
        l.thaw(this);
        return l.releaseOwnership(RECYCLED);
    }

    private static final Supplier<Lookup> LOOKUP_FAC = new Supplier<>() {
        @Override public Lookup get() {
            return new Lookup.Concrete(null).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "LocalityStandaloneDict.LOOKUP_FAC";}
    };
    private static final Alloc<Lookup> LOOKUP = new Alloc<>(Lookup.class,
            "LocalityStandaloneDict.Lookup", LOOKUP_POOL_CAPACITY,
            LOOKUP_FAC, Lookup.BYTES);
    static { Primer.INSTANCE.sched(LOOKUP::prime); }

    public static abstract sealed class Lookup extends AbstractLookup<Lookup> {
        protected static final int BYTES = 16 /*headers*/ + 2*SegmentRopeView.BYTES;
        private LocalityStandaloneDict dict;
        private final SegmentRopeView tmp = new SegmentRopeView();
        private final SegmentRopeView out = new SegmentRopeView();

        private Lookup(LocalityStandaloneDict parent) {
            thaw(parent);
        }

        private void thaw(LocalityStandaloneDict dict) {
            if (this.dict == dict)
                return;
            this.dict = dict;
            tmp.wrap(dict.seg, 0, 1);
            out.wrap(dict.seg, 0, 1);
        }

        @Override public @Nullable Lookup recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            if (LOOKUP.offer(this) != null)
                internalMarkGarbage(RECYCLED);
            return null;
        }

        private static final class Concrete extends Lookup implements Orphan<Lookup> {
            private Concrete(LocalityStandaloneDict parent) {super(parent);}
            @Override public Lookup takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public LocalityStandaloneDict dict() { return dict; }

        @Override public long find(PlainRope rope) {
            var dict = this.dict;
            if (rope.len == 0)
                return dict.emptyId;
            if (U == null) return coldFind(rope);
            long nStrings = dict.nStrings;
            long id = 1;
            if (rope instanceof SegmentRope s) {
                byte[] rBase = s.utf8;
                long rOff = s.segment.address() + s.offset;
                int rLen = s.len;
                while (id <= nStrings) {
                    long off = dict.readOffUnsafe(id - 1);
                    int diff = compare1_1(null, dict.valBase + off,
                            (int)(dict.readOffUnsafe(id) - off),
                            rBase, rOff, rLen);
                    if (diff == 0) return id;
                    id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
                }
            } else {
                TwoSegmentRope tsr = (TwoSegmentRope) rope;
                byte[] rBase1 = tsr.fstU8, rBase2 = tsr.sndU8;
                long rOff1 = tsr.fst.address() + tsr.fstOff;
                long rOff2 = tsr.snd.address() + tsr.sndOff;
                int rLen1 = tsr.fstLen, rLen2 = tsr.sndLen;
                while (id <= nStrings) {
                    long off = dict.readOffUnsafe(id - 1);
                    int diff = (int)(dict.readOffUnsafe(id) - off);
                    diff = compare1_2(null, dict.valBase + off, diff,
                                      rBase1, rOff1, rLen1, rBase2, rOff2, rLen2);
                    if (diff == 0) return id;
                    id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
                }
            }
            return NOT_FOUND;
        }

        private long coldFind(PlainRope rope) {
            long id = 1;
            var dict = this.dict;
            while (id <= dict.nStrings) {
                long off = dict.readOffUnsafe(id - 1);
                tmp.slice(off, (int) (dict.readOffUnsafe(id) - off));
                int diff = tmp.compareTo(rope);
                if (diff == 0) return id;
                id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        @Override public long find(Term term) {
            if (term == null)
                return NOT_FOUND;
            if (U != null) {
                LocalityStandaloneDict dict = this.dict;
                long id = 1;
                SegmentRope termFst = term.first(), termSnd = term.second();
                long termFstOff = termFst.segment.address() + termFst.offset;
                long termSndOff = termSnd.segment.address() + termSnd.offset;
                int termFstLen = termFst.len, termSndLen = termSnd.len;
                while (id <= dict.nStrings) {
                    long off = dict.readOffUnsafe(id - 1);
                    int len = (int) (dict.readOffUnsafe(id) - off);
                    int diff = compare1_2(null, dict.valBase + off, len,
                                          termFst.utf8, termFstOff, termFstLen,
                                          termSnd.utf8, termSndOff, termSndLen);
                    if (diff == 0) return id;
                    id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
                }
            } else {
                return safeFindTerm(term);
            }
            return NOT_FOUND;
        }

        private long safeFindTerm(Term term) {
            long id = 1;
            var dict = this.dict;
            while (id <= dict.nStrings) {
                long off = dict.readOffUnsafe(id - 1);
                int len = (int) (dict.readOffUnsafe(id) - off);
                tmp.slice(off, len);
                int diff = term.compareTo(dict.seg, off, len);
                if (diff == 0) return id;
                id = (id << 1) + (~diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        @Override public SegmentRopeView get(long id) {
            var dict = this.dict;
            if (id < MIN_ID || id > dict.nStrings) return null;
            long off = dict.readOffUnsafe(id - 1);
            out.slice(off, (int)(dict.readOffUnsafe(id)-off));
            return out;
        }
    }
}
