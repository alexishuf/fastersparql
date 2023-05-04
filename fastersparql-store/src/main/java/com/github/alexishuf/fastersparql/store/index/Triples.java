package com.github.alexishuf.fastersparql.store.index;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.nio.file.Path;
import java.util.Iterator;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.IntVector.fromMemorySegment;
import static jdk.incubator.vector.IntVector.zero;

public class Triples extends OffsetMappedLEValues implements AutoCloseable {
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

    /** Base-1 id of the first key in this index.*/
    private final long firstKey;

    private final ValueIt noValueIt;
    private final PairIt noPairIt;
    private final SubKeyIt noSubKeyIt;

    public Triples(Path path) throws IOException {
        super(path, SegmentScope.auto());
        firstKey = seg.get(JAVA_LONG, FIRST_KEY_OFF);
        noValueIt = new ValueIt(0, 0, 0, 0);
        noPairIt = new PairIt(0, 0);
        noSubKeyIt = new SubKeyIt(0, 0, 0);
        validateOffsets();
    }

    @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
        long keysAndFlags = seg.get(LE_LONG, KEYS_AND_FLAGS_OFF);
        md.offsetsCount = (keysAndFlags & NKEYS_MASK) + 1;
        md.offsetsOff = OFFS_OFF;
        md.offsetWidth = (keysAndFlags & OFF_W_MASK) == 0 ? 8 : 4;
        md.valueWidth = (keysAndFlags & ID_W_MASK) == 0 ? 8 : 4;
    }

    public void validateOffsets() throws IOException {
        long last = OFFS_OFF+(offsCount<<offShift);
        long end = seg.byteSize();
        for (int i = 0; i < offsCount; i++) {
            long off = readOff(i);
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
        for (long k = 0, nKeys = offsCount-1; k < nKeys; ++k) {
            long begin = readOff(k), end = readOff(k+1);
            int pairWidth = valWidth<<1;
            for (long prevSK = 0, prevV = 0; begin < end; begin += pairWidth) {
                long sk = readValue(begin), v = readValue(begin+valWidth);
                if (sk < prevSK || (sk == prevSK && v < prevV))
                    throw new IOException("Malformed "+this+": ("+sk+" "+v+") < ("+prevSK+" "+prevV+") byte "+begin+", key index "+k);
                if (sk == 0)
                    throw new IOException("Malformed "+this+": zero sub-key at byte "+begin+", key index "+k);
                if (v == 0)
                    throw new IOException("Malformed "+this+": zero value at byte "+begin+", key index "+k);
            }
        }
    }

    @Override protected String computeToString() {
        return "Triples{keys="+keysCount()+", path="+path+"]";
    }

    public long keysCount() { return offsCount-1; }

    public long triplesCount() {
        long bytes = readOff(offsCount-1) - readOff(0);
        return bytes>>(valShift+1);
    }

    /**
     * Whether the given {@code keyId} and {@code subKeyId} are mapped to {@code valueId}
     */
    public boolean contains(long keyId, long subKeyId, long valueId) {
        ValueIt it = values(keyId, subKeyId);
        if (!it.advance()) return false;
        if (it.valueId == valueId) return true;
        int step = valWidth<<1;
        long end;
        if (valueId < it.valueId)
            end = it.bottom + (step = -step);
        else
            end = it.top;
        for (long addr = it.midAddress; addr != end && readValue(addr) == subKeyId; addr += step) {
            if (readValue(addr         ) != subKeyId) return false;
            if (readValue(addr+valWidth) ==  valueId) return true;
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
        if (keyId < 0 || keyId >= offsCount-1) return noPairIt;
        return new PairIt(readOff(keyId), readOff(keyId+1));
    }

    public final class PairIt {
        private long address;
        private final long end;
        public long subKeyId, valueId;

        public PairIt(long address, long end) {
            this.address = address;
            this.end = end;
        }

        public long remaining() { return (end-address)>>(valShift+1); }

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
            subKeyId = readValue(address);
            valueId = readValue(address+valWidth);
            this.address += (long) valWidth <<1;
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
        if (keyId < 0 || keyId >= offsCount-1) return noValueIt;
        int pairShift = valShift + 1;
        long bottom = readOff(keyId), top = readOff(keyId+1);
        long lo = 0, hi = (top-1-bottom)>>pairShift;
        while (lo <= hi) {
            long mid = (lo+hi)>>>1;
            long midAddr = bottom + (mid<<pairShift);
            long diff = subKeyId - readValue(midAddr);
            if      (diff < 0) hi = mid-1;
            else if (diff > 0) lo = mid+1;
            else               return new ValueIt(bottom, top, midAddr, subKeyId);
        }
        return noValueIt;
    }

    public final class ValueIt {
        private long address;
        private final long midAddress, bottom, top;
        /** The {@code subKeyId} given to {@link #values(long, long)}. */
        public final long subKeyId;
        /** The current value id of the queried {@code keyId} and {@code subKeyId} after
         *  a {@code true} {@link #advance()}. */
        public long valueId;

        private ValueIt(long bottom, long top,
                        long midAddress, long subKeyId) {
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
            int pairWidth = valWidth<<1;
            if (address >= midAddress) {
                if (address < top && readValue(address) == subKeyId) {
                    valueId = readValue(address+valWidth);
                    this.address += pairWidth;
                    return true;
                }
                this.address = address = midAddress-pairWidth;
            }
            // address < midAddress
            if (address >= bottom && readValue(address) == subKeyId) {
                valueId = readValue(address+valWidth);
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
        if (keyId < 0 || keyId >= offsCount-1) return noSubKeyIt;
        long begin = readOff(keyId), end = readOff(keyId+1);
        return valShift == 2 ? new SubKeyIt4(begin, end, valueId)
                             : new SubKeyIt (begin, end, valueId);
    }

    public sealed class SubKeyIt {
        protected long address;
        protected final long valueId, end;
        /** The sub-key id fetched by the last {@code true} {@link #advance()} call. */
        public long subKeyId;

        private SubKeyIt(long address, long end, long valueId) {
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
            int pairWidth = valWidth<<1;
            while (address < end && readValue(address+valWidth) != valueId)
                address += pairWidth;
            if (address == end)
                return false;
            subKeyId = readValue(address);
            return true;
        }
    }

    public final class SubKeyIt4 extends SubKeyIt {
        private SubKeyIt4(long address, long end, long valueId) {
            super(address, end, valueId);
        }

        private static final boolean[] ODD = {
                false, true, false, true, false, true, false, true,
                false, true, false, true, false, true, false, true,
                false, true, false, true, false, true, false, true};
        @Override public boolean advance() {
            if (address == end)
                return false;
            long i = address, ve = i;
            if (end-i >= I_LEN) {
                var expected = zero(I_SP).blend(valueId, VectorMask.fromArray(I_SP, ODD, 0));
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
                while (i < end && readValue(i+valWidth) != valueId)
                    i += 8;
                if (i >= end) {
                    address = i;
                    return false;
                }
            }
            subKeyId = readValue(i);
            address = i+8;
            return true;
        }
    }

}
