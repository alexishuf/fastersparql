package com.github.alexishuf.fastersparql.client.model.row.dedup;

import com.github.alexishuf.fastersparql.client.model.row.RowType;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.numberOfLeadingZeros;

public final class StrongDedup<R> extends Dedup<R> {
    /** Set of operations  */
    private final RowType<Object, ?> rt;
    /** How many items are stored across all buckets */
    private int tableSize = 0;
    /** Capacity for newly created buckets. */
    private int bucketCapacity = 8;
    /** When tableSize reaches this, perform a rehash */
    private int nextRehash;
    /** Used to compute the bucket for a hash: {@code hash & bucketsMask */
    private int bucketsMask;
    /** Array of buckets. An empty bucket is represented by {@code null} */
    private @Nullable Bucket[] buckets;
    /** Provides mutual exclusion for writes */
    private final ReentrantLock lock = new ReentrantLock();

    public StrongDedup(RowType<R, ?> rt, int capacity) {
        //noinspection unchecked
        this.rt = (RowType<Object, ?>) rt;
        if (capacity < 0)
            throw new IllegalArgumentException("Negative capacity");
        rehash(capacity);
    }

    private void rehash(@NonNegative int initialCapacity) {
        int bucketCount = buckets == null ? initialCapacity : buckets.length;
        bucketCount = Math.min(bucketCount, MAX_VALUE>>5);
        // mask = (1 << ceil(log2(bucketCount))) - 1
        int mask = -1 >>> numberOfLeadingZeros(bucketCount - 1);
        // if mask is -1 (log2(bucketCount) <= 1), add 2 (yielding 1 bucket), else add 1
        bucketCount = mask + 1 + (mask >>> 31);

        // allocate new buckets array but save previous to rehash entries
        Bucket[] oldBuckets = null;
        if (this.buckets == null || bucketCount > this.buckets.length) {
            oldBuckets = this.buckets;
            this.buckets = new Bucket[bucketCount];
            this.bucketsMask = bucketCount - 1;
        }

        // once buckets.length reaches 2^16 (64Ki), bucket and array pointers alone cost 1.75MiB.
        // From this point onwards, limit pointer overhead growth by doubling the bucket capacity.
        if ((bucketCapacity & 31) != 0) // do not grow bucketCapacity beyond 32
            bucketCapacity <<= ((bucketsMask >>> 15) & 1); //only double if bucketCount >= 2^16

        // rehash once tableSize reaches 75% of target capacity;
        int newCapacity = bucketCount * bucketCapacity;
        nextRehash = newCapacity - (newCapacity>>2);

        if (oldBuckets != null) {
            // Scan old buckets and add their items to the new buckets array
            // allocate new Bucket instances on demand while releasing old Buckets
            for (int oldBucketIdx = 0; oldBucketIdx < oldBuckets.length; oldBucketIdx++) {
                Bucket oldBucket = oldBuckets[oldBucketIdx];
                int[] hashes = oldBucket.hashes;
                Object[] rows = oldBucket.rows;
                for (int i = 0, oldBucketSize = oldBucket.size; i < oldBucketSize; i++) {
                    int bIdx = hashes[i] & bucketsMask;
                    Bucket newBucket = buckets[bIdx];
                    if (newBucket == null) // allocate on first use
                        buckets[bIdx] = newBucket = new Bucket(rt, bucketCapacity);
                    newBucket.add(rows[i], hashes[i]);
                }
                oldBuckets[oldBucketIdx] = null; // release memory
            }
        }
    }

    @Override public boolean isDuplicate(R row, int source) {
        int hash = rt.hash(row);
        Bucket bucket = buckets[hash & bucketsMask];
        // null bucket means nothing was added to it.
        if (bucket != null && bucket.contains(row, hash))
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
            if (bucket != null && bucket.contains(row, hash))
                return true;
            // at this point, row must be added to the table
            if (tableSize == nextRehash) { // table requires rehash
                rehash(0);
                bucket = buckets[bucketIdx = hash & bucketsMask]; //rehash changes buckets, find new one
            }
            if (bucket == null) // buckets are allocated on first use (here)
                buckets[bucketIdx] = bucket = new Bucket(rt, bucketCapacity);
            bucket.add(row, hash);
            ++tableSize;
            return false;
        } finally {
            lock.unlock();
        }
    }

    private static final class Bucket {
        final RowType<Object, ?> rt;
        int size;
        int[] hashes;
        long bitset;
        Object[] rows;

        public Bucket(RowType<Object, ?> rt, int capacity) {
            this.rt = rt;
            this.bitset = this.size = 0;
            this.hashes = new int[capacity];
            this.rows = new Object[capacity];
        }

        boolean contains(Object row, int hash) {
            if ((bitset & (1L << (hash & 63))) == 0)
                return false; // certainly not present (but vulnerable to data race)
            int sizeBefore = size, i = 0;
            while (i < sizeBefore && (hashes[i] != hash || !rt.equalsSameVars(row, rows[i])))
                ++i;
            return i < sizeBefore;
        }

        void add(Object row, int hash) {
            bitset |= 1L << (hash & 63);
            int capacity = hashes.length;
            if (size == capacity) {
                capacity = capacity < 256 ? capacity << 1 : capacity + (capacity>>1);
                hashes = Arrays.copyOf(hashes, capacity);
                rows = Arrays.copyOf(rows, capacity);
            }
            hashes[size] = hash;
            rows[size++] = row;
        }
    }
}
