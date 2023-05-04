package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.store.index.Splitter.SharedSide;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.store.index.Splitter.SharedSide.SUFFIX;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class Dict extends OffsetMappedLEValues implements AutoCloseable {
    public static final long STRINGS_MASK = 0x00ffffffffffffffL;
    public static final long      FLAGS_MASK = ~STRINGS_MASK;
    public static final long      OFF_W_MASK = 0x0100000000000000L;
    public static final long     SHARED_MASK = 0x0200000000000000L;
    public static final long SHARED_OVF_MASK = 0x0400000000000000L;
    public static final int       FLAGS_BIT = Long.numberOfTrailingZeros(FLAGS_MASK);
    private static final int      OFF_W_BIT = Long.numberOfTrailingZeros(OFF_W_MASK);
    private static final int     SHARED_BIT = Long.numberOfTrailingZeros(SHARED_MASK);
    private static final int SHARED_OVF_BIT = Long.numberOfTrailingZeros(SHARED_OVF_MASK);
    private static final int OFFS_OFF = 8;

    public static final long NOT_FOUND = 0;
    public static final long MIN_ID = 1;
    public static final long EMPTY_ID = MIN_ID;

    private final long nStrings;
    private final @Nullable Dict shared;
    private final boolean sharedOverflow;
    private final long emptyId;

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
     * @param shared shared {@link Dict} that shall be used to resolve prefixes and suffixes
     *               of strings in this dictionary. Will only be used if the shared bit in
     *               {@code file} is set. Ownership of any non-null value is taken by the
     *               constructor (if unnecessary, {@code shared} will be closed). If the
     *               shared bit is set in {@code file}, {@code shared} must not be null.
     * @throws IOException if the file could not be opened or if it could not be memory-mapped.
     * @throws IllegalArgumentException if {@code shared} is null but the shared bit is set in
     *                                  {@code file}.
     */
    public Dict(Path file, @Nullable Dict shared) throws IOException {
        super(file, SegmentScope.auto());
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
        this.nStrings = stringsAndFlags & STRINGS_MASK;
        this.emptyId = nStrings > 0 && readOff(0) == readOff(1) ? MIN_ID : NOT_FOUND;
    }

    public Dict(Path file) throws IOException {
        this(file, null);
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
        if (shared != null) {
            Lookup lookup = lookup();
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
    }

    public String dump() {
        var sb = new StringBuilder();
        Lookup lookup = lookup();
        for (long i = MIN_ID; i <= nStrings; i++) {
            PlainRope r = lookup.get(i);
            if (r == null)
                throw new RuntimeException("Valid string not found");
            sb.append(r).append('\n');
        }
        sb.setLength(Math.max(0, sb.length()-1));
        return sb.toString();
    }

    /** Get the shared strings {@link Dict} or {@code null} if this is a standalone dict. */
    public @Nullable Dict shared() { return shared; }

    /** Get the number of strings stored in this dictionary */
    public long strings() {
        return nStrings;
    }

    public Lookup lookup() { return new Lookup(); }

    public final class Lookup {
        private final Lookup shared = Dict.this.shared == null ? null : Dict.this.shared.lookup();
        private final SegmentRope tmp = new SegmentRope();
        private final SegmentRope    out1 = shared == null ? tmp : new SegmentRope();
        private final TwoSegmentRope out2 = new TwoSegmentRope();
        private final Splitter split = new Splitter();

        public Dict dict() { return Dict.this; }

        /**
         * Find an id such that {@code rope.equals(get(id))}.
         *
         * @param rope A rope to be searched in this dictionary
         * @return If the string was found, an {@code id} such that {@link #get(long)} returns a
         *         rope equals to {@code rope}. Else return {@link #NOT_FOUND}.
         */
        public long find(PlainRope rope) {
            if (rope.len == 0)
                return emptyId;
            if (shared != null) {
                var b64 = split.b64(switch (split.split(rope)) {
                    case NONE -> MIN_ID;
                    case PREFIX,SUFFIX -> shared.find(split.shared());
                });
                long id = find(b64, split.local());
                if (id == NOT_FOUND && sharedOverflow)
                    id = find(split.b64(EMPTY_ID), rope);
                return id;
            } else {
                long lo = 0, hi = nStrings-1;
                while (lo <= hi) {
                    long mid = ((lo + hi) >>> 1);
                    long off = readOff(mid);
                    tmp.wrapSegment(seg, off, (int) (readOff(mid+1) - off));
                    int diff = rope.compareTo(tmp);
                    if      (diff < 0) hi   = mid - 1;
                    else if (diff > 0) lo   = mid + 1;
                    else               return mid + Dict.MIN_ID;
                }
                return NOT_FOUND;
            }
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
         * <p>If {@link #shared()} is {@code null}, a non-null returns of this method will
         * always be a {@link SegmentRope}. Else a non-null return will always be a instance
         * of {@link TwoSegmentRope}.</p>
         *
         * @param id  id of the string to load.
         * @return  {@code null} if {@code id < }{@link #MIN_ID} or
         *          {@code id >= }{@link #MIN_ID}+{@link #nStrings}. Else get a {@link PlainRope}
         *          containing the string. <strong>WARNING: </strong> contents of the returned
         *          {@link PlainRope} may change on subsequent invocations of this method.
         */
       public PlainRope get(long id)  {
           if (id < MIN_ID || id > nStrings) return null;
           long off = readOff(id - 1);
           int len = (int)(readOff(id) - off);
           if (shared == null) {
               out1.wrapSegment(seg, off, len);
               return out1;
           } else {
               long sId = Splitter.decode(seg, off);
               SegmentRope sharedRope = (SegmentRope) this.shared.get(sId);
               if (sharedRope == null)
                   throw new BadSharedId(id, Dict.this, off, len);
               out2.wrapFirst(sharedRope);
               out2.wrapSecond(seg, off+5, len-5);
               if (SharedSide.fromConcatChar(seg.get(JAVA_BYTE, off+4)) == SUFFIX)
                   out2.flipSegments();
               return out2;
           }
       }
    }

    public static final class BadSharedId extends IllegalStateException {
        public BadSharedId(long id, Dict dict, long off, int len) {
            super(String.format("String %d (%s) at %s refers to invalid shared string",
                    id, new SegmentRope(dict.seg, off, len), dict));
        }
    }

}