package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_1;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare1_2;

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

    @Override public AbstractLookup polymorphicLookup() { return lookup(); }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup extends AbstractLookup {
        private final SegmentRope tmp = new SegmentRope(seg, 0, 1);
        private final SegmentRope out = new SegmentRope(seg, 0, 1);

        @Override public LocalityStandaloneDict dict() { return LocalityStandaloneDict.this; }

        @Override public long find(PlainRope rope) {
            if (rope.len == 0) return emptyId;
            if (UNSAFE == null) return coldFind(rope);
            long id = 1;
            if (rope instanceof SegmentRope s) {
                byte[] rBase = s.utf8;
                long rOff = s.segment.address() + s.offset;
                int rLen = s.len;
                while (id <= nStrings) {
                    long off = readOffUnsafe(id - 1);
                    int diff = compare1_1(null, valBase + off, (int) (readOffUnsafe(id) - off),
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
                    long off = readOffUnsafe(id - 1);
                    int diff = (int) (readOffUnsafe(id) - off);
                    diff = compare1_2(null, valBase + off, diff,
                                      rBase1, rOff1, rLen1, rBase2, rOff2, rLen2);
                    if (diff == 0) return id;
                    id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
                }
            }
            return NOT_FOUND;
        }

        private long coldFind(PlainRope rope) {
            long id = 1;
            while (id <= nStrings) {
                long off = readOffUnsafe(id - 1);
                tmp.slice(off, (int) (readOffUnsafe(id) - off));
                int diff = tmp.compareTo(rope);
                if (diff == 0) return id;
                id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        @Override public long find(Term term) {
            if (term == null)
                return NOT_FOUND;
            if (UNSAFE != null) {
                long id = 1;
                SegmentRope termFst = term.first(), termSnd = term.second();
                long termFstOff = termFst.segment.address() + termFst.offset;
                long termSndOff = termSnd.segment.address() + termSnd.offset;
                int termFstLen = termFst.len, termSndLen = termSnd.len;
                while (id <= nStrings) {
                    long off = readOffUnsafe(id - 1);
                    int len = (int) (readOffUnsafe(id) - off);
                    int diff = compare1_2(null, valBase + off, len,
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
            while (id <= nStrings) {
                long off = readOffUnsafe(id - 1);
                int len = (int) (readOffUnsafe(id) - off);
                tmp.slice(off, len);
                int diff = term.compareTo(seg, off, len);
                if (diff == 0) return id;
                id = (id << 1) + (~diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
            }
            return NOT_FOUND;
        }

        @Override public SegmentRope get(long id) {
            if (id < MIN_ID || id > nStrings) return null;
            long off = readOffUnsafe(id - 1);
            out.slice(off, (int)(readOffUnsafe(id)-off));
            return out;
        }
    }
}
