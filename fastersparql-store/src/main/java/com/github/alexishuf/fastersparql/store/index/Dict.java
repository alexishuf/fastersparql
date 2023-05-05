package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.nio.file.Path;

public abstract class Dict extends OffsetMappedLEValues implements AutoCloseable {
    public static final long     STRINGS_MASK = 0x00ffffffffffffffL;
    public static final long       FLAGS_MASK = ~STRINGS_MASK;
    public static final long       OFF_W_MASK = 0x0100000000000000L;
    public static final long      SHARED_MASK = 0x0200000000000000L;
    public static final long  SHARED_OVF_MASK = 0x0400000000000000L;
    public static final long     PROLONG_MASK = 0x0800000000000000L;
    public static final long PENULTIMATE_MASK = 0x1000000000000000L;
    public static final int         FLAGS_BIT = Long.numberOfTrailingZeros(FLAGS_MASK);
    private static final int OFFS_OFF         = 8;

    public static final long NOT_FOUND = 0;
    public static final long MIN_ID = 1;
    public static final long EMPTY_ID = MIN_ID;

    protected final long nStrings;
    protected final long emptyId;

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
        super(file, SegmentScope.auto());
        long stringsAndFlags = seg.get(LE_LONG, 0);
        this.nStrings = stringsAndFlags & STRINGS_MASK;
        this.emptyId = nStrings > 0 && readOff(0) == readOff(1) ? MIN_ID : NOT_FOUND;
    }

    @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
        long stringsAndFlags = seg.get(LE_LONG, 0);
        md.offsetsOff   = OFFS_OFF;
        md.offsetsCount = (stringsAndFlags & STRINGS_MASK) + 1;
        md.offsetWidth  = (stringsAndFlags & OFF_W_MASK) == 0 ? 8 : 4;
        md.valueWidth   = 1;
    }

    public void validate() throws IOException {
        SegmentRope prev = new SegmentRope(), curr = new SegmentRope();
        for (long i = 0; i < nStrings; i++) {
            long begin = readOff(i), end = readOff(i+1);
            if (end == begin && i != 0)
                throw new IOException("Malformed "+this+": has empty string at id "+(i+1));
            if (end < begin)
                throw new IOException("Malformed "+this+": string with id "+(i+1)+" has negative length");
            curr.wrapSegment(seg, begin, (int)(end-begin));
            if (i > 0 ) {
                int diff = curr.compareTo(prev);
                if (diff < 0)
                    throw new IOException("Malformed "+this+": string with id "+(i+1)+" ("+curr+") is smaller than previous ("+prev+")");
                if (diff == 0)
                    throw new IOException("Malformed "+this+": string with id "+(i+1)+" ("+curr+") is a duplicate of previous");
            }
            prev.wrapSegment(seg, begin, (int) (end-begin));
        }
    }

    public String dump() {
        var sb = new StringBuilder();
        var lookup = this instanceof CompositeDict c ? c.lookup()
                                                     : ((StandaloneDict)this).lookup();
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


    public abstract static class Lookup {

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
         * Get the string corresponding to the given id in this dictionary. Valid ids are in the
         * [{@link #MIN_ID}, {@link #MIN_ID}{@code +}{@link #strings()}) range.
         *
         * <p><strong>WARNING:</strong>. To provide zero-copy and zero-alloc guarantees, this
         * method returns a reference to a {@link PlainRope} held by the {@link Lookup} instance.
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