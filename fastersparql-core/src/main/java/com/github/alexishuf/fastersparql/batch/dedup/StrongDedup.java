package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.util.Arrays.copyOf;

public final class StrongDedup<B extends Batch<B>> extends Dedup<B> {
    /** How many items are stored across all buckets */
    private int tableSize = 0;
    /** When new additions will have weak semantics (instead of re-hashing, another
     *  addition also made under weak semantics will be overwritten). */
    private final int weakenAt;
    /** When tableSize reaches this, perform a rehash */
    private int nextRehash;
    /** Capacity for new instantiations of Bucket */
    private int bucketCapacity = 16;
    /** Used to compute the bucket for a hash: {@code hash & bucketsMask */
    private int bucketsMask;
    /** Array of buckets. An empty bucket is represented by {@code null} */
    private @Nullable Bucket<B>[] buckets;
    /** Provides mutual exclusion for writes */
    private final ReentrantLock lock = new ReentrantLock();

    private StrongDedup(BatchType<B> bt, int initialCapacity, int weakenAt, int cols) {
        super(bt, cols);
        this.weakenAt  = weakenAt;
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Negative initialCapacity");
        rehash(initialCapacity);
    }

    public static <B extends Batch<B>>
    StrongDedup<B> strongUntil(BatchType<B> bt, int strongCapacity, int cols) {
        return new StrongDedup<>(bt, Math.max(8, strongCapacity>>4), strongCapacity, cols);
    }

    public static <B extends Batch<B>>
    StrongDedup<B> strongForever(BatchType<B> bt, int initialCapacity, int cols) {
        return new StrongDedup<>(bt, initialCapacity, MAX_VALUE, cols);
    }

    @Override public void clear(int cols) {
        tableSize = 0;
        this.cols = cols;
        for (Bucket<B> b : buckets) {
            if (b != null) b.clear(cols);
        }
    }

    @Override public int capacity() {
        long sum = 0;
        //noinspection rawtypes
        for (Bucket b : buckets)
            sum += b == null ? bucketCapacity : b.hashes.length;
        return (int)Math.min(MAX_VALUE, sum);
    }

    public int weakenAt() { return weakenAt; }

    @Override public boolean isWeak() { return tableSize >= weakenAt; }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void rehash(@NonNegative int initialCapacity) {
        int nBuckets;
        if (buckets == null) {
            nBuckets = initialCapacity < 8 ? 1
                        // 1 << ceil(log2(initialCapacity/4))
                        : -1 >>> numberOfLeadingZeros((initialCapacity >> 2) - 1);
        } else if (buckets.length < MAX_VALUE>>4) {
            nBuckets = buckets.length << 1; // double number of buckets
        } else {
            return; // stop growing buckets array
        }

        // allocate new buckets array, if we are growing, but save previous to rehash entries
        Bucket[] oldBuckets = buckets;
        buckets = new Bucket[nBuckets];
        bucketsMask = nBuckets - 1;
        nextRehash = 16*nBuckets - 4*nBuckets; // 75% of size with 16 items per bucket

        if (oldBuckets != null) {
            bucketCapacity = Math.max(16, tableSize>>numberOfLeadingZeros(nBuckets));
            // Scan old buckets and add their items to the new buckets array
            // allocate new Bucket instances on demand while releasing old Buckets
            for (int oldBucketIdx = 0; oldBucketIdx < oldBuckets.length; oldBucketIdx++) {
                Bucket<B> oldBucket = oldBuckets[oldBucketIdx];
                if (oldBucket == null)
                    continue;
                int[] hashes = oldBucket.hashes;
                var rows = oldBucket.rows;
                for (int i = 0, oldBucketSize = oldBucket.size; i < oldBucketSize; i++) {
                    int bIdx = hashes[i] & bucketsMask;
                    Bucket<B> newBucket = buckets[bIdx];
                    if (newBucket == null) // allocate on first use
                        buckets[bIdx] = newBucket = new Bucket(bt, bucketCapacity, cols);
                    newBucket.add(rows.batchOf(i), rows.batchRow(i), hashes[i]);
                }
                oldBuckets[oldBucketIdx] = null; // release memory
            }
        }
    }

    @Override public boolean isDuplicate(B batch, int row, int source) {
        if (debug) checkBatchType(batch);
        int hash = batch.hash(row);
        Bucket<B> bucket = buckets[hash & bucketsMask];
        // null bucket means nothing was added to it.
        if (bucket != null && bucket.contains(batch, row, hash))
            return true; // if contains() returns true, it is correct even with data races
        lock.lock();
        try {
            // once we reach MAX_VALUE, stop adding items and simply report anything not
            // contained as added without really adding. There is no need to re-check
            // weakContains() because no addition happened concurrently.
            if (tableSize == MAX_VALUE)
                return false;
            // test(row) might have been run by another thread between entry in this frame
            // and lock.lock(). row might have been added to bucket or the table could've been
            // rehashed.
            int bucketIdx = hash & bucketsMask;
            bucket = buckets[bucketIdx];
            if (bucket != null && bucket.contains(batch, row, hash))
                return true;
            // at this point, row must be added to the table
            if (tableSize == nextRehash) { // table requires rehash
                rehash(0);
                bucket = buckets[bucketIdx = hash & bucketsMask]; //rehash changes buckets, find new one
            }
            // if this item is the first under weak semantics, we must change all buckets
            // into weak mode
            if (tableSize == weakenAt) {
                for (Bucket<B> b : buckets) {
                    if (b != null) b.weaken();
                }
            }
            if (bucket == null) // allocate bucket on first use
                buckets[bucketIdx] = bucket = new Bucket<>(bt, bucketCapacity, cols);
            if (bucket.add(batch, row, hash)) // do not increment size if row overwrote an weak insertion
                ++tableSize;
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether {@code row} has been previously added by {@link Dedup#isDuplicate(B, int, int)}
     * or {@link StrongDedup#add(B, int)}
     */
    @Override public boolean contains(B batch, int row) {
        if (debug) checkBatchType(batch);
        int hash = batch.hash(row);
        Bucket<B> bucket = buckets[hash & bucketsMask];
        return bucket != null && bucket.contains(batch, row, hash);
    }

    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        for (Bucket<B> b : buckets) {
            if (b != null) b.forEach(consumer);
        }
    }

    @SuppressWarnings("unused") // used only when debugging
    public String dump() {
        var sb = new StringBuilder();
        forEach(b -> {
            for (int r = 0; r < b.rows; r++) sb.append(b.toString(r)).append('\n');
        });
        sb.setLength(Math.max(1, sb.length()-1));
        return sb.toString();
    }

    private static final class Bucket<B extends Batch<B>> {
        int size, weakBegin;
        int[] hashes;
        long bitset;
        RowBucket<B> rows;

        public Bucket(BatchType<B> bt, int capacity, int cols) {
            this.bitset = this.size = 0;
            this.hashes = new int[this.weakBegin = capacity];
            this.rows = bt.createBucket(capacity, cols);

        }

        void clear(int cols) {
            Arrays.fill(hashes, 0);
            bitset = 0;
            size = 0;
            weakBegin = hashes.length;
            rows.clear(hashes.length, cols);
        }

        void weaken() {
            int deficit = Math.max(8, hashes.length>>3) - (hashes.length-size);
            if (deficit > 0) {
                hashes = copyOf(hashes, hashes.length+deficit);
                rows.grow(deficit);
            }
            weakBegin = size;
        }

        boolean contains(B batch, int row, int hash) {
            if ((bitset & (1L << (hash & 63))) == 0)
                return false; // certainly not present (but vulnerable to data race)
            int weakBegin = this.weakBegin, capacity = hashes.length, i = 0;
            while (i < weakBegin && (hashes[i] != hash || !rows.equals(i, batch, row)))
                ++i;
            if (i == weakBegin) {
                if (weakBegin < capacity) {
                    i = weakBegin + ((hash&0x7fffffff) % (capacity - weakBegin));
                    return hashes[i] == hash && rows.equals(i, batch, row);
                }
                return false;
            }
            return true;
        }

        boolean add(B batch, int row, int hash) {
            bitset |= 1L << (hash & 63);
            int capacity = hashes.length, i;
            boolean incSize;
            if (weakBegin < capacity) {
               i = weakBegin + (hash&0x7fffffff) % (capacity - weakBegin);
                incSize = !rows.has(i);
            } else {
                incSize = true;
                if ((i = size) == capacity) {
                    int additional = Math.min(128, capacity>>1);
                    hashes = copyOf(hashes, capacity += additional);
                    rows.grow(additional);
                    weakBegin = capacity;
                }
            }
            hashes[i] = hash;
            rows.set(i, batch, row);
            if (incSize)
                ++size;
            return incSize;
        }

        <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
            for (B batch : rows)
                consumer.accept(batch);
        }
    }
}
