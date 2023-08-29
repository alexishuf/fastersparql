package com.github.alexishuf.fastersparql.store.index.triples;

import com.github.alexishuf.fastersparql.store.index.OffsetMappedLEValues;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import jdk.incubator.vector.VectorMask;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;

import static com.github.alexishuf.fastersparql.util.LowLevelHelper.I_LEN;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.I_SP;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.IntVector.fromMemorySegment;
import static jdk.incubator.vector.IntVector.zero;

public class Triples extends OffsetMappedLEValues implements AutoCloseable {
    public static final long NKEYS_MASK = 0x00ffffffffffffffL;
    public static final long OFF_W_MASK = 0x0100000000000000L;
    public static final long  ID_W_MASK = 0x0200000000000000L;
    public static final int KEYS_AND_FLAGS_OFF = 0;
    public static final int      FIRST_KEY_OFF = 8;
    public static final int           OFFS_OFF = 16;

    /** Base-1 id of the first key in this index.*/
    private final long firstKey;

    private final ValueIt noValueIt;
    private final PairIt noPairIt;
    private final SubKeyIt noSubKeyIt;

    public Triples(Path path) throws IOException {
        super(path, Arena.openShared());
        firstKey = seg.get(JAVA_LONG, FIRST_KEY_OFF);
        noValueIt = new ValueIt(0, 0, 0);
        noPairIt = new PairIt(0, 0);
        noSubKeyIt = new SubKeyIt(0, 0, 0);
        quickValidateOffsets(-1);
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
                long sk = readValueUnsafe(begin), v = readValueUnsafe(begin+valWidth);
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

    /**
     * The lowest key stored in this index. Keys in the {@code [1, firstKey())}
     * have no pairs stored.
     *
     * <p>This should not be used to query the index with {@link #values(long, long)},
     * {@link #pairs(long)} and similar methods. Instead, it should be used if iterating over
     * all stored keys:</p>
     *
     * <pre>{@code
     * for (long k = firstKey(), end = k+keysCount(); i < end; ++k)
     *     // ... do something with key k
     * }</pre>
     */
    public long firstKey() { return firstKey; }
    public long keysCount() { return offsCount-1; }

    public long triplesCount() {
        long bytes = readOff(offsCount-1) - readOff(0);
        return bytes>>(valShift+1);
    }

    /**
     * Get a iterator over all triples. This should be avoided as a strategy to answer queries.
     *
     * @return a iterator positioned before the first triple, requiring
     *         {@link ScanIt#advance()} to be called.
     */
    public ScanIt scan() { return new ScanIt(); }

    public void scan(ScanIt it) {
        it.keyId = firstKey-1;
        it.pairAddress = 0;
        it.pairsEnd = 0;
    }

    public final class ScanIt {
        private final long endKeyId;
        private long pairAddress, pairsEnd;
        public long keyId, subKeyId, valueId;

        public ScanIt() {
            keyId = firstKey-1;
            endKeyId = firstKey+keysCount();
        }

        /**
         * Moves this iterator to the next triple, changing {@link #keyId}, {@link #subKeyId}
         * and {@link #valueId}.
         *
         * <p>This class is used instead of an {@link Iterator} to avoid spamming instances of
         * a {@code Pair} class. This method is analogous to the following:</p>
         *
         * <pre>{@code
         *     if (hasNext()) {
         *         Pair p = next();
         *         keyId = p.keyId;
         *         subKeyId = p.subKeyId;
         *         valueId = p.valueId;
         *         return true;
         *     }
         *     return false;
         * }</pre>
         *
         * @return {@code true} iff there is a new triple in this iterator {@code *Id} fields,
         *         {@code false} if the iterator reached the end.
         */
        public boolean advance() {
            while (pairAddress >= pairsEnd && ++keyId < endKeyId) {
                pairAddress = readOffUnsafe(  keyId-firstKey);
                pairsEnd    = readOffUnsafe(1+keyId-firstKey);
            }
            if (pairAddress < pairsEnd) {
                subKeyId = readValueUnsafe(pairAddress);
                valueId = readValueUnsafe(pairAddress + valWidth);
                pairAddress += (long) valWidth << 1;
                return true;
            }
            return false;
        }
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
        return new PairIt(readOffUnsafe(keyId), readOffUnsafe(keyId+1));
    }

    /**
     * Equivalent to {@link #pairs(long)} but modifies {@code it} instead of returning a new
     * iterator.
     */
    public void pairs(long keyId, PairIt it) {
        if (it == noPairIt) throw new IllegalArgumentException();
        keyId -= firstKey;
        if (keyId < 0 || keyId >= offsCount-1)
            return;
        it.address = readOffUnsafe(keyId);
        it.end     = readOffUnsafe(keyId+1);
    }

    /** Create a new {@link PairIt} for use with {@link #pairs(long, PairIt)}. */
    public PairIt makePairIt() {
        return new PairIt(0, 0);
    }

    /**
     * The number of results in {@link #pairs(long)} for the given {@code keyId}. This method
     * is accurate and fast.
     */
    public long estimatePairs(long keyId) {
        keyId -= firstKey;
        if (keyId < 0 || keyId >= offsCount-1) return 0;
        return (readOffUnsafe(keyId+1) - readOffUnsafe(keyId))>>>(valShift+1);
    }

    public final class PairIt {
        private long address;
        private long end;
        public long subKeyId, valueId;

        public PairIt(long address, long end) {
            this.address = address;
            this.end = end;
        }

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
            if (address >= end) return false;
            subKeyId = readValueUnsafe(address);
            valueId = readValueUnsafe(address+valWidth);
            this.address += (long) valWidth <<1;
            return true;
        }
    }

    /**
     * Whether the given {@code keyId} and {@code subKeyId} are mapped to {@code valueId}
     */
    public boolean contains(long keyId, long subKeyId, long valueId) {
        keyId -= firstKey;
        if (keyId < 0 || keyId >= offsCount-1)
            return false;
        int pairWidth = valWidth<<1;
        long bottom = readOffUnsafe(keyId), top = readOffUnsafe(keyId+1);
        long address = subKeyLowerBound(subKeyId, bottom, top);
        while (address < top) {
            long pairValue = readValueUnsafe(address+valWidth);
            if (readValueUnsafe(address) != subKeyId) return false; // scanned all pairs for subKeyId
            if (pairValue == valueId) return  true; // found
            if (pairValue >  valueId) return false; // pairs are ordered, thus valueId is missing
            address += pairWidth;
        }
        return false; //reached top before meeting (subKeyId, valueId) pair
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
        if (keyId < 0 || keyId >= offsCount-1)
            return noValueIt;
        long bottom = readOffUnsafe(keyId), top = readOffUnsafe(keyId+1);
        long pair = subKeyLowerBound(subKeyId, bottom, top);
        if (pair < top && readValueUnsafe(pair) == subKeyId)
            return new ValueIt(pair, top, subKeyId);
        return noValueIt;
    }

    /**
     * Equivalent to {@link #values(long, long)} but modifies {@code it} instead of returning
     * a new iterator.
     */
    public void values(long keyId, long subKeyId, ValueIt it) {
        if (it == noValueIt) throw new IllegalArgumentException();
        keyId -= firstKey;
        if (keyId < 0 || keyId >= offsCount-1)
            return;
        long bottom = readOffUnsafe(keyId), top = readOffUnsafe(keyId+1);
        it.address = subKeyLowerBound(subKeyId, bottom, top);
        it.end = top;
        it.subKeyId = subKeyId;
    }

    /** Create a {@link ValueIt} for use with {@link #values(long, long, ValueIt)} */
    public ValueIt makeValuesIt() {
        return new ValueIt(0, 0, 0);
    }

    /**
     * Approximate number of results that {@link #values(long, long)} would yield for
     * {@code keyId} and {@code subKeyId}.
     *
     * <p><strong>Accuracy</strong>: Estimates of zero are always accurate. For all other values,
     * the estimate may be an overestimation, but never an underestimation.</p>
     *
     * @param keyId analogous to {@link #values(long, long)}
     * @param subKeyId analogous to {@link #values(long, long)}
     * @return an estimate of the number of results in the iterator returned by {@link #values(long, long)}
     */
    public long estimateValues(long keyId, long subKeyId) {
        keyId -= firstKey;
        if (keyId < 0 || keyId >= offsCount-1)
            return 0;
        long bottom = readOffUnsafe(keyId), top = readOffUnsafe(keyId+1);
        long lo = subKeyLowerBound(subKeyId, bottom, top);
        return (subKeyLowerBound(subKeyId+1, lo, top) - lo)>>(valShift+1);
    }

    private long subKeyLowerBound(long subKeyId, long bottom, long top) {
        int pairShift = valShift + 1;
        long lo = 0, hi = (top-1-bottom)>>pairShift;
        while (lo <= hi) {
            long mid = (lo+hi)>>>1;
            long midAddr = bottom + (mid<<pairShift);
            long diff = subKeyId - readValueUnsafe(midAddr);
            if (diff <= 0) hi = mid-1;
            else           lo = mid+1;
        }
        return bottom + (lo<<pairShift);
    }

    public final class ValueIt {
        private long address;
        private long end;
        /** The {@code subKeyId} given to {@link #values(long, long)}. */
        public long subKeyId;
        /** The current value id of the queried {@code keyId} and {@code subKeyId} after
         *  a {@code true} {@link #advance()}. */
        public long valueId;

        private ValueIt(long begin, long end, long subKeyId) {
            this.address = begin;
            this.end = end;
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
            final long end = this.end, subKeyId = this.subKeyId;
            long address = this.address;
            if (address >= end)
                return false;
            if (readValueUnsafe(address) != subKeyId) {
                this.address = end;
                return false;
            }
            valueId = readValueUnsafe(address +valWidth);
            this.address = address + ((long)valWidth<<1);
            return true;
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
        if (keyId < 0 || keyId >= offsCount-1)
            return noSubKeyIt;
        long begin = readOffUnsafe(keyId), end = readOffUnsafe(keyId+1);
        return valShift == 2 ? new SubKeyIt4(begin, end, valueId)
                             : new SubKeyIt (begin, end, valueId);
    }

    /** Equivalent to {@link #subKeys(long, long)} but modifies {@code it} instead of
     *  returning a new iterator.*/
    public void subKeys(long keyId, long valueId, SubKeyIt it) {
        if (it == noSubKeyIt) throw new IllegalArgumentException();
        keyId -= firstKey;
        if (keyId < 0 || keyId >= offsCount-1)
            return;
        it.address = readOffUnsafe(keyId);
        it.end     = readOffUnsafe(keyId+1);
        it.valueId = valueId;
    }

    /** Creates a {@link SubKeyIt} for use with {@link #subKeys(long, long, SubKeyIt)}. */
    public SubKeyIt makeSubKeyIt() {
        return valShift == 2 ? new SubKeyIt4(0, 0, 0)
                             : new SubKeyIt(0, 0, 0);
    }

    public sealed class SubKeyIt {
        protected long address, valueId, end;
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
            final long end = this.end, valueId = this.valueId;
            if (address == end)
                return false;
            int pairWidth = valWidth<<1;
            while (address < end && readValueUnsafe(address+valWidth) != valueId)
                address += pairWidth;
            if (address == end)
                return false;
            subKeyId = readValueUnsafe(address);
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
            if (LowLevelHelper.ENABLE_VEC && end-i >= I_LEN) {
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
                while (i < end && readValueUnsafe(i+valWidth) != valueId)
                    i += 8;
                if (i >= end) {
                    address = i;
                    return false;
                }
            }
            subKeyId = readValueUnsafe(i);
            address = i+8;
            return true;
        }
    }

}
