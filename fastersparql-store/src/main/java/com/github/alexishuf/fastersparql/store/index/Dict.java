package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

public class Dict implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Dict.class);
    public static final long NOT_FOUND = 0;
    public static final long MIN_ID = 1;

    private final Path file;
    private final FileChannel channel;
    private final MemorySegment segment;
    private final long nStrings;
    private final int offsetWidth;

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
     */
    public Dict(Path file) throws IOException {
        this.file = file;
        this.channel = FileChannel.open(file, READ);
        this.segment = channel.map(READ_ONLY, 0, channel.size(), SegmentScope.auto());
        long stringsAndFlags = segment.get(ValueLayout.JAVA_LONG, 0);
        this.nStrings = stringsAndFlags & 0x00ffffffffffffffL;
        this.offsetWidth = 8 >> ((stringsAndFlags>>>56) & 0x01);
    }

    /**
     * Closes the file and hints that memory-mapped pages in physical memory should be evicted.
     *
     * <p>Effectively unmapping the file is done by the JVM and this will happen once after
     * {@code this} is collected</p>
     */
    @Override public void close() {
        try {
            segment.unload();
        } catch (Throwable t) {
            log.info("{}: Ignoring segment.unload() error", this, t);
        }
        try {
            channel.close();
        } catch (IOException e) {
            log.info("Ignoring failure to close mmap()ed {}: {}", channel, e.toString());
        }
    }

    @Override public String toString() { return "Dict["+file+"]"; }

    /**
     * Get the number of strings stored in this dictionary
     */
    public long strings() { return nStrings; }

    /**
     * Find an id such that {@code rope.equals(get(id))}.
     *
     * @param rope A rope to be searched in this dictionary
     * @param tmp If non-null this will be used to wrap slices of the dict during the search.
     *            There is no meaning in the value visible in {@code tmp} after this method
     *            returns. A caller that will perform multiple calls of this method may pass one
     *            such instance to avoid a new allocation of a {@link SegmentRope} on every call.
     * @return If the string was found, an {@code id} such that {@link #get(long)} returns a rope
     *         equals to {@code rope}. Else return {@link #NOT_FOUND}.
     */
    public long find(SegmentRope rope, @Nullable SegmentRope tmp) {
        int len = rope.len;
        if (len == 0) return readOffset(8+offsetWidth) == readOffset(8) ? 1 : 0;
        if (tmp == null) tmp = new SegmentRope();
        long lo = 0, hi = nStrings-1;
        while (lo <= hi) {
            long mid = (lo+hi)>>>1;
            long pos = 8+offsetWidth*mid;
            long off = readOffset(pos);
            tmp.wrapSegment(segment, off, (int) (readOffset(pos+offsetWidth)-off));
            int diff = rope.compareTo(tmp);
            if      (diff < 0) hi   = mid-1;
            else if (diff > 0) lo   = mid+1;
            else               return mid+Dict.MIN_ID;
        }
        return NOT_FOUND;
    }

    /**
     * Get the string corresponding to the given id in this dictionary. Valid ids are in the
     * [{@link #MIN_ID}, {@link #MIN_ID}{@code +}{@link #strings()}) range.
     *
     * @param id id of the string to load.
     * @param dest A {@link SegmentRope} that will be mutated to reflect the contends of the
     *             string identified by {@code id} in this {@link Dict}
     * @return {@code true} iff the id was in the allowed range. If the return is false,
     *         {@code dest} will not be modified.
     */
    public boolean get(long id, SegmentRope dest) {
        if (id < MIN_ID || id > nStrings) return false;
        long pos = 8+offsetWidth*(id-1);
        long offset = readOffset(pos);
        int len = (int) (readOffset(pos + offsetWidth) - offset);
        dest.wrapSegment(segment, offset, len);
        return true;
    }


    /** Equivalent to {@link #get(long, SegmentRope)} but creates a new {@link SegmentRope} or
     *  returns {@code null} if the id is not valid. */
    public @Nullable SegmentRope get(long id) {
        if (id < MIN_ID || id > nStrings) return null;
        long pos = 8+offsetWidth*(id-1);
        long offset = readOffset(pos);
        return new SegmentRope(segment, offset, (int) (readOffset(pos + offsetWidth) - offset));
    }

    /* --- --- --- internal --- --- --- */

    private long readOffset(long pos) {
        if (offsetWidth == 8)
            return segment.get(ValueLayout.JAVA_LONG, pos);
        return segment.get(ValueLayout.JAVA_INT, pos);
    }
}
