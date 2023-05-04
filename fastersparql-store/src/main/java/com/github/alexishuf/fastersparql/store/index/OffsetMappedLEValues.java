package com.github.alexishuf.fastersparql.store.index;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.lang.foreign.ValueLayout.*;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Base class for memory-mapped data structures that contain an "offsets" list where
 * the offsets are byte offset into the mapped file where some value is stored.
 * All values come after the offsets list and not all "values" have offsets in the list.
 *
 * <p>This class purpose is to simplify writing code where width of offsets and values
 * is only known at runtime. It uses {@link Unsafe} internally.</p>
 */
abstract class OffsetMappedLEValues implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OffsetMappedLEValues.class);
    private static final long   INT_MASK = 0x00000000ffffffffL;
    private static final long SHORT_MASK = 0x000000000000ffffL;
    private static final long  BYTE_MASK = 0x00000000000000ffL;
    private static final boolean IS_BE = ByteOrder.nativeOrder() == BIG_ENDIAN;
    protected static final OfLong  LE_LONG  = IS_BE ?  JAVA_LONG.withOrder(LITTLE_ENDIAN) : JAVA_LONG;
    protected static final OfInt   LE_INT   = IS_BE ?   JAVA_INT.withOrder(LITTLE_ENDIAN) : JAVA_INT;
    protected static final OfShort LE_SHORT = IS_BE ? JAVA_SHORT.withOrder(LITTLE_ENDIAN) : JAVA_SHORT;
    private static final @Nullable Unsafe UNSAFE;

    static {
        Unsafe u = null;
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            u = (Unsafe) field.get(null);
        } catch (Throwable ignored) {
            try {
                Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
                u = c.newInstance();
            } catch (Throwable ignored1) {}
        }
        UNSAFE = u;
    }

    protected final FileChannel ch;
    protected final MemorySegment seg;
    protected final @Nullable Arena arena;
    protected final Path path;
    private final long offBase, valBase;
    protected final long offsCount, valEnd;
    protected final int offShift, valShift;
    protected final int offWidth, valWidth;
    protected String stringValue;
    private final IllegalStateException unreachable;

    public OffsetMappedLEValues(Path path, @Nullable Arena arena) throws IOException {
        this(path, arena, arena == null ? SegmentScope.auto() : arena.scope());
    }
    public OffsetMappedLEValues(Path path, @Nullable SegmentScope scope) throws IOException {
        this(path, null, scope);
    }
    private OffsetMappedLEValues(Path path, @Nullable Arena arena,
                                 @Nullable SegmentScope scope) throws IOException {
        this.path = path;
        this.arena = arena;
        this.ch = FileChannel.open(path, StandardOpenOption.READ);
        this.seg = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(),
                          scope == null ? SegmentScope.auto() : scope);

        var md = new Metadata();
        fillMetadata(seg, md);
        md.check(seg.byteSize());

        this.valBase = seg.address();
        this.valEnd = valBase + seg.byteSize();
        this.offBase = valBase + md.offsetsOff;
        this.offWidth = md.offsetWidth;
        this.valWidth = md.valueWidth;
        this.offShift = Integer.numberOfTrailingZeros(offWidth);
        this.valShift = Integer.numberOfTrailingZeros(valWidth);
        this.offsCount = md.offsetsCount;

        this.stringValue = getClass().getSimpleName() + "(" + path + ")";
        this.unreachable = new IllegalStateException("Unreachable code reached for "+this);
    }

    protected static class Metadata {
        /** Offset in bytes into the file where the offsets list starts */
        long offsetsOff;
        /** Number of items (not bytes) in the offsets lists (includes the "end" offset). */
        long offsetsCount;
        /** Number of bytes per item of the offsets list. Must be a power of 2 {@code <= 8} */
        int offsetWidth;
        /** Number of bytes per value referred to by an offset. Must be a power of 2 {@code <= 8} */
        int valueWidth;

        void check(long fileBytes) {
            if (offsetsOff < 0)
                throw new IllegalArgumentException("Negative offsetsOff");
            if (offsetsOff > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Suspisiously large offsetsOff");
            if (offsetsCount < 0)
                throw new IllegalArgumentException("Negative number of items in offsets list");
            if (offsetWidth < 0 || offsetWidth > 8 || Integer.bitCount(offsetWidth) != 1)
                throw new IllegalArgumentException("Bad offsetWidth="+offsetWidth+", expected 2^i <= 8");
            if (valueWidth < 0 || valueWidth > 8 || Integer.bitCount(valueWidth) != 1)
                throw new IllegalArgumentException("Bad valueWidth="+valueWidth+", expected 2^i <= 8");
            if (offsetsOff + offsetsCount*offsetWidth > fileBytes)
                throw new IllegalArgumentException("Offsets lists overflows the file size");
        }
    }

    /** Reads values for all fields of {@code md} from {@code seg}. This will be called early
     *  during {@link OffsetMappedLEValues} construction. Implementations should not call any
     *  {@link OffsetMappedLEValues} methods. */
    protected abstract void fillMetadata(MemorySegment seg, Metadata md);

    protected String computeToString() {
        return  getClass().getSimpleName()+"("+path+")";
    }

    @Override public String toString() {
        return stringValue == null ? stringValue = computeToString() : stringValue;
    }

    @Override public void close() {
        try {
            seg.unload();
        } catch (Throwable t) { log.error("Ignoring failure to unmap {}", path); }
        try {
            ch.close();
        } catch (Throwable t) { log.error("Ignoring failure to close {}", ch); }
    }

    /**
     * Read the index-th offset of the offsets list as a long (without sign-extension).
     *
     * <p><strong>Warning</strong>: this method does not perform bounds check and out of bound
     * checks will cause <strong>UNDEFINED BEHAVIOR</strong> was would happen with native code.
     * Out of bound reads are most likely to either read garbage or kill the JVM, but anything
     * could happen.</p>
     *
     * @param index the zero-based index of the desired offset within the offsets list.
     * @return the byte offset into the mapped file that is stored at the index-th entry of
     *         the offsets list.
     * @throws IndexOutOfBoundsException if {@code index < 0} or {@code index > count} where
     *                                  {@code count} is the numer of items in the offsets list,
     *                                  as determined during construction of {@code this}.
     */
    protected long readOff(long index) {
        if (UNSAFE == null || IS_BE)
            return readFallbackOff(index);
        if (index <  0 | index >= offsCount)
            throw new IndexOutOfBoundsException();
        long addr = offBase + (index << offShift);
        return switch (offShift) {
            case 0  -> UNSAFE.getByte(addr)  & BYTE_MASK;
            case 1  -> UNSAFE.getShort(addr) & SHORT_MASK;
            case 2  -> UNSAFE.getInt(addr)   & INT_MASK;
            case 3  -> UNSAFE.getLong(addr);
            default -> throw unreachable;
        };
    }

    /**
     * Read the value at the {@code offset}-th byte of the {@code mmap()}ed file.
     *
     * @param offset the zero-based index of the desired offset within the offsets list.
     * @return the byte offset into the mapped file that is stored at the index-th entry of
     *         the offsets list.
     * @throws IndexOutOfBoundsException if {@code offset < 0} or {@code offset >= size} where
     *                                   {@code size} is the mapped file size in bytes.
     */
    protected long readValue(long offset) {
        if (UNSAFE == null || IS_BE)
            return readFallbackValue(offset);
        long addr = valBase + offset;
        if (offset < 0 || addr >= valEnd) throw new IndexOutOfBoundsException();
        return switch (valShift) {
            case 0 -> UNSAFE.getByte(addr) & BYTE_MASK;
            case 1 -> UNSAFE.getShort(addr) & SHORT_MASK;
            case 2 -> UNSAFE.getInt(addr) & INT_MASK;
            case 3 -> UNSAFE.getLong(addr);
            default -> throw unreachable;
        };
    }

    private long readFallbackOff(long index) {
        if (index < 0 || index >= offsCount) throw new IndexOutOfBoundsException();
        return readFallback((index<<offShift) + (offBase-valBase), offShift);
    }
    private long readFallbackValue(long off) {
        if (off < 0 || off >= valEnd-valBase) throw new IndexOutOfBoundsException();
        return readFallback(off, valShift);
    }
    private long readFallback(long off, int shift) {
        return switch (shift) {
            case 0 -> seg.get(JAVA_BYTE, off) & BYTE_MASK;
            case 1 -> seg.get(LE_SHORT, off) & SHORT_MASK;
            case 2 -> seg.get(LE_INT, off) & INT_MASK;
            case 3 -> seg.get(LE_LONG, off);
            default -> throw unreachable;
        };
    }
}
