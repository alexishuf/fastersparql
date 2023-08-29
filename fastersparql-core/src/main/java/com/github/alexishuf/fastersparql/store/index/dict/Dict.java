package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.OffsetMappedLEValues;
import com.github.alexishuf.fastersparql.store.index.SmallBBPool;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class Dict extends OffsetMappedLEValues implements AutoCloseable {
    public static final long     STRINGS_MASK = 0x00ffffffffffffffL;
    public static final long       FLAGS_MASK = ~STRINGS_MASK;
    public static final long       OFF_W_MASK = 0x0100000000000000L;
    public static final long      SHARED_MASK = 0x0200000000000000L;
    public static final long  SHARED_OVF_MASK = 0x0400000000000000L;
    public static final long     PROLONG_MASK = 0x0800000000000000L;
    public static final long PENULTIMATE_MASK = 0x1000000000000000L;
    public static final long    LOCALITY_MASK = 0x2000000000000000L;
    public static final int         FLAGS_BIT = Long.numberOfTrailingZeros(FLAGS_MASK);

    static final int OFFS_OFF = 8;

    public static final long NOT_FOUND = 0;
    public static final long MIN_ID = 1;

    protected final long nStrings;
    protected final byte flags;

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
     * @throws IOException if the file could not be opened or if it could not be memory-mapped.
     * @throws IllegalArgumentException if {@code shared} is null but the shared bit is set in
     *                                  {@code file}.
     */
    protected Dict(Path file) throws IOException {
        super(file, Arena.openShared());
        long stringsAndFlags = seg.get(LE_LONG, 0);
        this.flags = (byte) (stringsAndFlags >>> FLAGS_BIT);
        this.nStrings = stringsAndFlags & STRINGS_MASK;
    }

    public static Dict loadStandalone(Path path) throws IOException {
        return isLocality(path) ? new LocalityStandaloneDict(path) : new SortedStandaloneDict(path);
    }

    public static Dict loadComposite(Path path, Dict shared) throws IOException {
        return isLocality(path) ? new LocalityCompositeDict(path, (LocalityStandaloneDict) shared)
                                : new SortedCompositeDict(path, (SortedStandaloneDict) shared);
    }

    public static Dict load(Path path) throws IOException {
        long hdr = readHeader(path);
        if ((hdr & SHARED_MASK) != 0) {
            if ((hdr & LOCALITY_MASK) != 0) return new LocalityCompositeDict(path);
            else                            return new SortedCompositeDict(path);
        } else {
            if ((hdr & LOCALITY_MASK) != 0) return new LocalityStandaloneDict(path);
            else                            return new SortedStandaloneDict(path);
        }
    }

    private static long readHeader(Path path) throws IOException {
        ByteBuffer bb = SmallBBPool.smallDirectBB().order(ByteOrder.LITTLE_ENDIAN).limit(8);
        try (var ch = FileChannel.open(path, StandardOpenOption.READ)) {
            if (ch.read(bb, 0) != 8) throw new IOException("Could not read 8 bytes from " + path);
            return bb.getLong(0);
        } finally {
            SmallBBPool.releaseSmallDirectBB(bb);
        }
    }

    private static boolean isLocality(Path path) throws IOException {

        boolean isLocality;
        ByteBuffer bb = SmallBBPool.smallDirectBB().order(ByteOrder.LITTLE_ENDIAN).limit(8);
        try (var ch = FileChannel.open(path, StandardOpenOption.READ)) {
            if (ch.read(bb, 0) != 8) throw new IOException("Could not read 8 bytes from " + path);
            isLocality = (bb.getLong(0) & LOCALITY_MASK) != 0;
        } finally {
            SmallBBPool.releaseSmallDirectBB(bb);
        }
        return isLocality;
    }

    @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
        long stringsAndFlags = seg.get(LE_LONG, 0);
        md.offsetsOff   = OFFS_OFF;
        md.offsetsCount = (stringsAndFlags & STRINGS_MASK) + 1;
        md.offsetWidth  = (stringsAndFlags & OFF_W_MASK) == 0 ? 8 : 4;
        md.valueWidth   = 1;
    }

    public abstract AbstractLookup polymorphicLookup();
    public void validate() throws IOException {
        var lookup = polymorphicLookup();
        for (int id = 1; id <= nStrings; id++) {
            PlainRope str = lookup.get(id);
            if (str.len < 0)
                throw new IOException("Malformed "+this+": string with id "+id+" has negative length");
            long found = lookup.find(str);
            if (found != id)
                throw new IOException("Malformed "+this+": expected "+id+" for find("+str+"), got "+found);
        }
    }

    public String dump() {
        var sb = new StringBuilder();
        var lookup = this instanceof SortedCompositeDict c ? c.lookup()
                                                     : ((SortedStandaloneDict)this).lookup();
        for (long i = MIN_ID; i <= nStrings; i++) {
            PlainRope r = lookup.get(i);
            if (r == null)
                throw new RuntimeException("Valid string not found");
            sb.append(r).append('\n');
        }
        sb.setLength(Math.max(0, sb.length()-1));
        return sb.toString();
    }


    /** Get the number of strings stored in this dictionary */
    public long strings() {
        return nStrings;
    }

    /**
     * Whether the ids in this dict enumerate a sorted list of strings.
     *
     * <p>This will return false if the dict uses a shared dict or if strings are
     * stored in a layout that is optimized to improve spatial locality.</p>
     */
    public boolean isSorted() {
        return (flags & (byte)((LOCALITY_MASK|SHARED_MASK) >>> FLAGS_BIT)) == 0;
    }

    public abstract static class AbstractLookup {

        public abstract Dict dict();

        /**
         * Find an id such that {@code rope.equals(get(id))}.
         *
         * @param rope A rope to be searched in this dictionary
         * @return If the string was found, an {@code id} such that {@link #get(long)} returns a
         *         rope equals to {@code rope}. Else return {@link #NOT_FOUND}.
         */
        public abstract long find(PlainRope rope);

        /**
         * Analogous to {@link #find(PlainRope)}, but takes a {@link Term}.
         *
         * @param term The {@link Term} whose string representation is to be looked up
         * @return see {@link #find(PlainRope)}
         */
        @SuppressWarnings("unused") public abstract long find(Term term);

        /**
         * Get the string corresponding to the given id in this dictionary. Valid ids are in the
         * [{@link #MIN_ID}, {@link #MIN_ID}{@code +}{@link #strings()}) range.
         *
         * <p><strong>WARNING:</strong>. To provide zero-copy and zero-alloc guarantees, this
         * method returns a reference to a {@link PlainRope} held by the {@link AbstractLookup} instance.
         * Such instance will be mutated by subsequent invocations of this method. If it is
         * wrapped by another {@link SegmentRope} or {@link TwoSegmentRope}, then that wrapper
         * will not observe changes from a new call to this method, since it will be pointing to
         * read-only mapped memory.</p>
         *
         * @param id  id of the string to load.
         * @return  {@code null} if {@code id < }{@link #MIN_ID} or
         *          {@code id >= }{@link #MIN_ID}+{@link #nStrings}. Else get a {@link PlainRope}
         *          containing the string. <strong>WARNING: </strong> contents of the returned
         *          {@link PlainRope} may change on subsequent invocations of this method.
         */
       public abstract PlainRope get(long id);
    }

    public static final class BadSharedId extends IllegalStateException {
        public BadSharedId(long id, Dict dict, long off, int len) {
            super(String.format("String %d (%s) at %s refers to invalid shared string",
                    id, new SegmentRope(dict.seg, off, len), dict));
        }
    }

}