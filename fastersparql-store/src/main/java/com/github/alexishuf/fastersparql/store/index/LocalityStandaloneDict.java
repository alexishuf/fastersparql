package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.U8_UNSAFE_BASE;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.cmp;

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
    protected LocalityStandaloneDict(Path file) throws IOException {
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
    }

    @Override public AbstractLookup polymorphicLookup() { return lookup(); }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup extends AbstractLookup {
        private final SegmentRope tmp = new SegmentRope(seg, 0, 1);
        private final SegmentRope out = new SegmentRope(seg, 0, 1);

        @Override public LocalityStandaloneDict dict() { return LocalityStandaloneDict.this; }

        @Override public long find(PlainRope rope) {
            if (rope.len == 0) return emptyId;
            long id = 1;
            if (UNSAFE != null && rope instanceof SegmentRope s) {
                Object rBase = s.segment.array().orElse(null);
                long rOff = s.segment.address() + (rBase == null ? 0 : U8_UNSAFE_BASE) + s.offset;
                int rLen = s.len;
                while (id <= nStrings) {
                    long off = readOffUnsafe(id-1);
                    int diff = cmp(null, valBase+off, (int)(readOffUnsafe(id)-off),
                                   rBase, rOff, rLen);
                    if (diff == 0) return id;
                    id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
                }
            } else {
                while (id <= nStrings) {
                    long off = readOffUnsafe(id - 1);
                    tmp.slice(off, (int) (readOffUnsafe(id) - off));
                    int diff = tmp.compareTo(rope);
                    if (diff == 0) return id;
                    id = (id << 1) + (diff >>> 31); // = rope < tmp ? 2*id : 2*id + 1
                }
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
