package com.github.alexishuf.fastersparql.store.index;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;

import static com.github.alexishuf.fastersparql.store.index.ValueWidth.LE4B;
import static com.github.alexishuf.fastersparql.store.index.ValueWidth.LE8B;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;
import static jdk.incubator.vector.IntVector.fromMemorySegment;
import static jdk.incubator.vector.IntVector.zero;

public class Triples implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Triples.class);
    public static final long NKEYS_MASK = 0x00ffffffffffffffL;
    public static final long OFF_W_MASK = 0x0100000000000000L;
    public static final long  ID_W_MASK = 0x0200000000000000L;
    public static final int OFF_W_BIT = Long.numberOfTrailingZeros(OFF_W_MASK);
    public static final int  ID_W_BIT = Long.numberOfTrailingZeros( ID_W_MASK);
    public static final int KEYS_AND_FLAGS_OFF = 0;
    public static final int      FIRST_KEY_OFF = 8;
    public static final int           OFFS_OFF = 16;

    private static final VectorSpecies<Integer> I_SP = IntVector.SPECIES_PREFERRED;
    private static final int I_LEN = I_SP.length();
    private static final VectorSpecies<Long> L_SP = LongVector.SPECIES_PREFERRED;

    private final Path path;
    private final FileChannel fileChannel;
    private final MemorySegment seg;
    /** Base-1 id of the first key in this index.*/
    private final long firstKey;
    /** Number of keys in this index */
    private final long nKeys;
    /** The number of bytes in a pair */
    private final int pairWidth;
    private final ValueWidth offWidth, idWidth;

    private final ValueIt noValueIt;
    private final PairIt noPairIt;
    private final SubKeyIt noSubKeyIt;

    public Triples(Path path) throws IOException {
        this.path = path;
        fileChannel = FileChannel.open(path, READ);
        seg = fileChannel.map(READ_ONLY, 0, fileChannel.size(), SegmentScope.auto());
        long keysAndFlags = seg.get(JAVA_LONG, KEYS_AND_FLAGS_OFF);
        nKeys = keysAndFlags & NKEYS_MASK;
        offWidth = ValueWidth.le4If(keysAndFlags, OFF_W_BIT);
        idWidth  = ValueWidth.le4If(keysAndFlags, ID_W_BIT);
        pairWidth = idWidth.bytes()<<1;
        firstKey = seg.get(JAVA_LONG, FIRST_KEY_OFF);
        noValueIt = new ValueIt(idWidth, seg, 0, 0, 0, 0);
        noPairIt = new PairIt(offWidth, idWidth, seg, 0, 0);
        noSubKeyIt = new SubKeyIt(idWidth, seg, 0, 0, 0);
        validateOffsets();
    }

    public void validateOffsets() throws IOException {
        long last = OFFS_OFF+offWidth.idx2addr(nKeys+1);
        long end = seg.byteSize();
        for (int i = 0; i <= nKeys; i++) {
            long off = offWidth.readIdx(seg, OFFS_OFF, i);
            if (off < last)
                throw new IOException("Malformed "+this+": offset "+off+" for "+i+"-th key below previous offset "+last);
            if ((off & 7) != 0)
                throw new IOException("Malformed "+this+": misaligned offset "+off+" for "+i+"-th key");
            if (off > end)
                throw new IOException("Malformed"+this+": offset "+off+" for "+i+"-th key overflows");
        }
    }

    public void validate() throws IOException {
        validateOffsets();
        for (long k = 0; k < nKeys; ++k) {
            long begin = offWidth.readIdx(seg, OFFS_OFF, k);
            long end   = offWidth.readIdx(seg, OFFS_OFF, k+1);
            for (long prevSK = 0, prevV = 0; begin < end; begin += pairWidth) {
                long sk = idWidth.read(seg, begin), v = idWidth.readNext(seg, begin);
                if (sk < prevSK || (sk == prevSK && v < prevV))
                    throw new IOException("Malformed "+this+": ("+sk+" "+v+") < ("+prevSK+" "+prevV+") byte "+begin+", key index "+k);
                if (sk == 0)
                    throw new IOException("Malformed "+this+": zero sub-key at byte "+begin+", key index "+k);
                if (v == 0)
                    throw new IOException("Malformed "+this+": zero value at byte "+begin+", key index "+k);
            }
        }
    }

    @Override public void close() {
        try {
            seg.unload();
        } catch (Throwable t) {
            log.info("{}: Ignoring segment.unload() error ", this, t);
        }
        try {
            fileChannel.close();
        } catch (Throwable t) {
            log.info("Ignoring error when closing FileChannel for {}: ", path, t);
        }
    }

    @Override public String toString() { return "Triples{keys="+nKeys+", path="+path+"]"; }

    public long keysCount() { return nKeys; }

    public long triplesCount() {
        long end = offWidth.readIdx(seg, OFFS_OFF, nKeys);
        long begin = offWidth.readIdx(seg, OFFS_OFF, 0);
        return idWidth.addr2idx(end-begin)>>1;
    }

    /**
     * Whether the given {@code keyId} and {@code subKeyId} are mapped to {@code valueId}
     */
    public boolean contains(long keyId, long subKeyId, long valueId) {
        ValueIt it = values(keyId, subKeyId);
        if (!it.advance()) return false;
        if (it.valueId == valueId) return true;
        int step = idWidth.bytes()<<1;
        long end;
        if (valueId < it.valueId)
            end = it.bottom + (step = -step);
        else
            end = it.top;
        for (long addr = it.midAddress; addr != end && idWidth.read(seg, addr) == subKeyId; addr += step) {
            if (idWidth.read    (seg, addr) != subKeyId) return false;
            if (idWidth.readNext(seg, addr) ==  valueId) return true;
        }
        return false;
    }

    /**
     * Get a {@link PairIt} iterating over all {@code (subKeyId, valueId)} pairs for a given key.
     *
     * @param keyId the key id of which the {@code (subkey, value)} pairs shall be iterated.
     * @return A {@link PairIt} before the first value ({@link PairIt#advance()} must be called).
     */
    public PairIt pairs(long keyId) {
        keyId -= firstKey;
        if (keyId < 0 || keyId >= nKeys) return noPairIt;
        return new PairIt(offWidth, idWidth, seg, offWidth.readIdx(seg, OFFS_OFF, keyId),
                          offWidth.readIdx(seg, OFFS_OFF, keyId+1));
    }

    public static final class PairIt {
        private long address;
        private final long end;
        public long subKeyId, valueId;
        private final ValueWidth offWidth, idWidth;
        private final MemorySegment seg;

        public PairIt(ValueWidth offWidth, ValueWidth idWidth, MemorySegment seg,
                      long address, long end) {
            this.offWidth = offWidth;
            this.idWidth = idWidth;
            this.seg = seg;
            this.address = address;
            this.end = end;
        }

        public long remaining() { return offWidth.addr2idx(end-address); }

        /**
         * Moves this iterator to the next pair, changing {@link #subKeyId} and {@link #valueId}.
         *
         * <p>This class is used instead of an {@link Iterator} to avoid spamming instances of
         * a {@code Pair} class. This method is analogous to the following:</p>
         *
         * <pre>{@code
         *     if (hasNext()) {
         *         Pair p = next();
         *         subKeyId = p.subKeyId;
         *         valueId = p.valueId;
         *         return true;
         *     }
         *     return false;
         * }</pre>
         *
         * @return {@code true} iff there is a new pair in {@link #subKeyId}/{@link #valueId}.
         */
        public boolean advance() {
            long address = this.address;
            if (address == end) return false;
            if (idWidth == LE4B) {
                long l = LE8B.read(seg, address);
                subKeyId = (int)l;
                valueId = (int)(l>>>32);
                this.address += 8;
            } else {
                subKeyId = idWidth.read(seg, address);
                valueId  = idWidth.readNext(seg, address);
                this.address += 16;
            }
            return true;
        }
    }

    /**
     * A {@link ValueIt} iterating over the values for the given key and subkey.
     *
     * @param keyId the key id
     * @param subKeyId the sub-key id
     * @return An iterator of values stored for the given sub-key of the key. The iterator will
     *         be positioned before the first element ({@link ValueIt#advance()} must be called).
     */
    public ValueIt values(long keyId, long subKeyId) {
        keyId -= firstKey;
        if (keyId < 0 || keyId >= nKeys) return noValueIt;
        long bottom = offWidth.readIdx(seg, OFFS_OFF, keyId);
        long top    = offWidth.readIdx(seg, OFFS_OFF, keyId+1);
        long lo = 0, hi = idWidth.addr2idx(top-1-bottom)>>1;
        while (lo <= hi) {
            long mid = (lo+hi)>>>1;
            long midAddr = bottom + idWidth.idx2addr(mid<<1);
            long diff = subKeyId - idWidth.read(seg, midAddr);
            if      (diff < 0) hi = mid-1;
            else if (diff > 0) lo = mid+1;
            else               return new ValueIt(idWidth, seg, bottom, top, midAddr, subKeyId);
        }
        return noValueIt;
    }

    public static final class ValueIt {
        private long address;
        private final long midAddress, bottom, top;
        /** The {@code subKeyId} given to {@link #values(long, long)}. */
        public final long subKeyId;
        /** The current value id of the queried {@code keyId} and {@code subKeyId} after
         *  a {@code true} {@link #advance()}. */
        public long valueId;
        private final ValueWidth idWidth;
        private final MemorySegment seg;

        private ValueIt(ValueWidth idWidth, MemorySegment seg, long bottom, long top,
                        long midAddress, long subKeyId) {
            this.idWidth = idWidth;
            this.seg = seg;
            this.bottom = bottom;
            this.top    = top;
            this.midAddress = midAddress;
            this.address = midAddress;
            this.subKeyId = subKeyId;
        }

        /**
         * Moves this iterator to the next value id of the provided key and sub-key.
         *
         * <p>This class is used instead of an {@link Iterator} to avoid boxing. This method
         * is analogous to the following usage of an {@link Iterator}:</p>
         *
         * <pre>{@code
         *     if (hasNext()) {
         *         this.valueId = next();
         *         return true;
         *     }
         *     return false;
         * }</pre>
         *
         * @return whether there is a new value in {@link #valueId}.
         */
        public boolean advance() {
            long address = this.address;
            if (address == 0)
                return false;
            int pairWidth = idWidth.bytes()<<1;
            if (address >= midAddress) {
                if (address < top && idWidth.read(seg, address) == subKeyId) {
                    valueId = idWidth.readNext(seg, address);
                    this.address += pairWidth;
                    return true;
                }
                this.address = address = midAddress-pairWidth;
            }
            // address < midAddress
            if (address >= bottom && idWidth.read(seg, address) == subKeyId) {
                valueId = idWidth.readNext(seg, address);
                this.address -= pairWidth;
                return true;
            }
            this.address = 0; // remember we reached end
            return false;
        }
    }

    /**
     * Get a {@link SubKeyIt} over the sub-keys of the given key that contain the given value.
     *
     * @param keyId the key id
     * @param valueId the value id. If zero, all sub-keys will match
     * @return A {@link SubKeyIt} positioned before the first element ({@link SubKeyIt#advance()}
     *         must be called)
     */
    public SubKeyIt subKeys(long keyId, long valueId) {
        keyId -= firstKey;
        if (keyId < 0 || keyId > nKeys) return noSubKeyIt;
        long begin = offWidth.readIdx(seg, OFFS_OFF, keyId);
        long end   = offWidth.readIdx(seg, OFFS_OFF, keyId+1);
        return idWidth == LE4B ? new SubKeyIt4(idWidth, seg, begin, end, valueId)
                               : new SubKeyIt (idWidth, seg, begin, end, valueId);
    }

    public static sealed class SubKeyIt {
        protected long address;
        protected final long valueId, end;
        /** The sub-key id fetched by the last {@code true} {@link #advance()} call. */
        public long subKeyId;
        protected final ValueWidth idWidth;
        protected final MemorySegment seg;

        private SubKeyIt(ValueWidth idWidth, MemorySegment seg,
                         long address, long end, long valueId) {
            this.idWidth = idWidth;
            this.seg = seg;
            this.address = address;
            this.end = end;
            this.valueId = valueId;
        }

        /**
         * Move this iterator to the next sub-key id.
         *
         * <p>This iterator is used instead of a standard {@link Iterator} to avoid boxing.
         * It is analogous to:</p>
         *
         * <pre>{@code
         *     if (hasNext()) {
         *         this.subKeyId = next();
         *         return true;
         *     }
         *     return false;
         * }</pre>
         *
         * @return whether there is a new sub-key in #subKeyId.
         */
        public boolean advance() {
            if (address == end)
                return false;
            int pairWidth = idWidth.bytes() << 1;
            while (address < end && idWidth.readNext(seg, address) != valueId)
                address += pairWidth;
            if (address == end)
                return false;
            subKeyId = idWidth.read(seg, address);
            return true;
        }
    }

    public static final class SubKeyIt4 extends SubKeyIt {
        private SubKeyIt4(ValueWidth idWidth, MemorySegment seg,
                          long address, long end, long valueId) {
            super(idWidth, seg, address, end, valueId);
        }

        private static final long ODD = 0xaaaaaaaaaaaaaaaaL;
        @Override public boolean advance() {
            if (address == end)
                return false;
            long i = address, ve = i;
            if (end-i >= I_LEN) {
                var expected = zero(I_SP).blend(valueId, VectorMask.fromLong(I_SP, ODD));
                for (ve = i+(I_SP.loopBound((end-i)>>2)<<2); i < ve ; i += (long)I_LEN <<2) {
                    var eq = fromMemorySegment(I_SP, seg, i, LITTLE_ENDIAN).eq(expected);
                    int lane = eq.firstTrue();
                    if (lane < I_LEN) {
                        i += (long) (lane - 1) <<2;
                        break;
                    }
                }
            }
            if (i == ve) { // not vectorized
                while (i < end && idWidth.readNext(seg, i) != valueId)
                    i += 8;
                if (i >= end) {
                    address = i;
                    return false;
                }
            }
            subKeyId = idWidth.read(seg, i);
            address = i+8;
            return true;
        }
    }

}
