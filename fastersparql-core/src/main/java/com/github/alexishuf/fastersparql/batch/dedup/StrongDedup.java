package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityLevelPool;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityPool;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Integer.*;
import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.onSpinWait;
import static java.util.Arrays.copyOf;

public final class StrongDedup<B extends Batch<B>> extends Dedup<B> {
    private static final Bucket<?>[] EMPTY_BUCKETS = new Bucket[0];
    private static final int MAX_BUCKETS      = 1024*1024/8; // 512KiB~1MiB per Bucket<B>[]
    private static final int RH_THREADS       = ForkJoinPool.commonPool().getParallelism();
    private static final int PAR_RH_THRESHOLD = RH_THREADS == 1 ? MAX_VALUE : RH_THREADS*64;


//    public static final VarHandle BA_CREATED, BA_REUSED, BA_RECYCLED, BA_ASYNC_RECYCLED, BA_LIVE;
//    static {
//        try {
//            BA_CREATED        = MethodHandles.lookup().findStaticVarHandle(StrongDedup.class, "plainBACreated",       int.class);
//            BA_REUSED         = MethodHandles.lookup().findStaticVarHandle(StrongDedup.class, "plainBAReused",        int.class);
//            BA_RECYCLED       = MethodHandles.lookup().findStaticVarHandle(StrongDedup.class, "plainBARecycled",      int.class);
//            BA_ASYNC_RECYCLED = MethodHandles.lookup().findStaticVarHandle(StrongDedup.class, "plainBAAsyncRecycled", int.class);
//            BA_LIVE           = MethodHandles.lookup().findStaticVarHandle(StrongDedup.class, "plainBALive", int.class);
//        } catch (NoSuchFieldException | IllegalAccessException e) {
//            throw new ExceptionInInitializerError(e);
//        }
//        FS.addShutdownHook(StrongDedup::reportStats);
//    }
//    private static int plainBACreated, plainBAReused, plainBARecycled, plainBAAsyncRecycled, plainBALive;
//    private static final ConcurrentHashMap<Bucket<?>[], Boolean> BA_LIVESET = new ConcurrentHashMap<>();
//    private static void reportStats() {
//        System.err.printf("""
//                StrongDedup#Bucket[] instances:
//                         created: %,6d
//                          reused: %,6d
//                        recycled: %,6d
//                  async recycled: %,6d
//                            live: %,6d
//                """, (int)BA_CREATED.getAcquire(), (int)BA_REUSED.getAcquire(),
//                (int)BA_RECYCLED.getAcquire(), (int)BA_ASYNC_RECYCLED.getAcquire(),
//                (int)BA_LIVE.getAcquire());
//        System.out.println("##");
//    }

    /** Counter that is incremented for every addition */
    private volatile int version;
    /** How many items are stored across all buckets */
    private int tableSize;
    /** Used to compute the bucket for a hash: {@code hash & bucketsMask */
    private int bucketsMask;
    /** Quick non-membership test. Bit {@code i} will be set if a row with
     * {@code hash&bitMask == i} was added.*/
    private long[] bitset = HashBitset.get();
    /** Array of buckets. An empty bucket is represented by {@code null} */
    private @Nullable Bucket<B>[] buckets;
    /** When tableSize reaches this, perform a rehash */
    private int nextRehash;
    /** Capacity for new instantiations of Bucket */
    private int bucketCapacity;
    /** When new additions will have weak semantics (instead of re-hashing, another
     *  addition also made under weak semantics will be overwritten). */
    private final int weakenAt;
    /** A pool for {@code Bucket<B>[]}s for {@code this.bt} */
    private final AffinityLevelPool<Bucket<B>[]> bucketArrayPool;
    /** A pool for {@link Bucket}s for {@code this.bt} */
    private final AffinityPool<Bucket<B>> bucketPool;

    private StrongDedup(BatchType<B> bt, int initialCapacity, int weakenAt, int cols) {
        super(bt, cols);
        this.bucketArrayPool = bucketArrayPool(bt);
        this.bucketPool = bucketPool(bt);
        this.weakenAt  = weakenAt;
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Negative initialCapacity");
        int nBuckets = initialCapacity <= 8 ? 1
                     : 1 << (32-numberOfLeadingZeros((initialCapacity>>3)-1));
        buckets        = bucketArray(nBuckets);
        bucketCapacity = 16;
        bucketsMask    = nBuckets-1;
        nextRehash     = nextRehash(nBuckets, bucketCapacity);
    }

    public static <B extends Batch<B>>
    StrongDedup<B> strongUntil(BatchType<B> bt, int strongCapacity, int cols) {
        return new StrongDedup<>(bt, Math.max(8, strongCapacity>>10), strongCapacity, cols);
    }

    public static <B extends Batch<B>>
    StrongDedup<B> strongForever(BatchType<B> bt, int initialCapacity, int cols) {
        return new StrongDedup<>(bt, initialCapacity, MAX_VALUE, cols);
    }

    @Override public void clear(int cols) {
        tableSize = 0;
        this.cols = cols;
    }

    @Override public void recycleInternals() {
        tableSize = 0;
        nextRehash = 0;
        bitset = HashBitset.recycle(bitset);
        var buckets = this.buckets;
        //noinspection unchecked
        this.buckets = (Bucket<B>[])EMPTY_BUCKETS;
//        BA_LIVE.getAndAddRelease(-1);
//        BA_LIVESET.remove(buckets);
//        BA_RECYCLED.getAndAddRelease(1);
        if (bucketArrayPool.offer(buckets, buckets.length) != null) {
            asyncRecycleInternals(buckets);
        }
    }

    private record BucketArrayGarbage<B extends Batch<B>>
            (Bucket<B>[] a, AffinityPool<Bucket<B>> pool) {
        public void recycle() {
            for (var b : a) {
                if (b != null && (b.rows.capacity() > 32 || pool.offer(b) != null))
                    b.recycleInternals();
            }
        }
    }

    private static final ConcurrentLinkedDeque<BucketArrayGarbage<?>> BUCKET_ARRAY_RECYCLER_QEUUE
            = new ConcurrentLinkedDeque<>();
    private static final Thread BUCKET_ARRAY_RECYCLER;
    static {
        BUCKET_ARRAY_RECYCLER = new Thread(() -> {
            while (!Thread.interrupted()) {
                var g = BUCKET_ARRAY_RECYCLER_QEUUE.poll();
                if (g == null) LockSupport.park(BUCKET_ARRAY_RECYCLER_QEUUE);
                else           g.recycle();
            }
        }, "StrongDedup-Bucket[]-recycler");
        BUCKET_ARRAY_RECYCLER.setDaemon(true);
        BUCKET_ARRAY_RECYCLER.start();
    }

    private void asyncRecycleInternals(Bucket<B>[] buckets) {
//        BA_ASYNC_RECYCLED.getAndAddRelease(1);
        BUCKET_ARRAY_RECYCLER_QEUUE.addLast(new BucketArrayGarbage<>(buckets, bucketPool));
        LockSupport.unpark(BUCKET_ARRAY_RECYCLER);
    }

    @Override public int capacity() {
        if (buckets == null)
            return 0;
        long sum = 0;
        for (var b : buckets)
            sum += b == null ? bucketCapacity : b.capacity();
        return (int)Math.min(MAX_VALUE, sum);
    }

    public int weakenAt() { return weakenAt; }

    @Override public boolean isWeak() { return tableSize >= weakenAt; }

    private static final class Rehash<B extends Batch<B>> extends RecursiveAction {
        private static final VarHandle LOCK;
        static {
            try {
                LOCK = MethodHandles.lookup().findVarHandle(Rehash.class, "plainLock", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final Bucket<B>[] newBuckets, oldBuckets;
        private final int bucketCapacity, chunkSize;
        @SuppressWarnings("unused") private int plainLock;
        private int tableSize;
        private final StrongDedup<B> parent;

        public Rehash(StrongDedup<B> parent, Bucket<B>[] newBuckets) {
            this.parent = parent;
            this.newBuckets = newBuckets;
            this.oldBuckets = parent.buckets;
            this.chunkSize = this.oldBuckets.length / RH_THREADS;
            int bucketCapacity = parent.tableSize >> numberOfTrailingZeros(newBuckets.length);
            this.bucketCapacity = Math.max(16, bucketCapacity);
        }

        @Override public void compute() {
            var tasks = new RecursiveAction[RH_THREADS];
            for (int i = 0; i < RH_THREADS; i++) tasks[i] = new SubTask(i);
            invokeAll(tasks);
            parent.buckets = newBuckets;
            int nBuckets = newBuckets.length;
            parent.bucketCapacity = bucketCapacity;
            parent.tableSize = tableSize;
            parent.bucketsMask = nBuckets-1;
            parent.nextRehash = nextRehash(nBuckets, bucketCapacity);
        }

        private void lock() {
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
        }

        private void unlock() { LOCK.setRelease(this, 0); }

        public final class SubTask extends RecursiveAction {
            private final int taskIdx;
            public SubTask(int taskIdx) {this.taskIdx = taskIdx;}

            @Override protected void compute() {
                var rh = Rehash.this;
                int weakenAt = parent.weakenAt/ RH_THREADS;
                int dstMask = newBuckets.length-1;
                int srcIdx = taskIdx*chunkSize;
                int srcEnd = taskIdx == RH_THREADS -1 ? oldBuckets.length : srcIdx+chunkSize;
                for (; srcIdx < srcEnd; ++srcIdx) {
                    var src = oldBuckets[srcIdx];
                    if (src == null) continue;
                    var srcRows = src.rows;
                    for (int r = 0, nRows = src.size; r < nRows; r++) {
                        int hash = enhanceHash(srcRows.hashCode(r)), dstIdx = hash & dstMask;
                        lock();
                        try {
                            var dst = newBuckets[dstIdx];
                            if (dst == null)
                                newBuckets[dstIdx] = dst = parent.createBucket(bucketCapacity);
                            if (dst.add(srcRows, r, hash, rh.tableSize >= weakenAt))
                                ++rh.tableSize;
                        } finally { unlock(); }
                    }
                }
            }
        }
    }

    private void singleThreadRehash(Bucket<B>[] dstBuckets) {
        assert Integer.bitCount(dstBuckets.length) == 1;
        int tableSize = 0;
        int bucketCapacity = Math.max(16, this.tableSize>>numberOfTrailingZeros(dstBuckets.length));
        var srcBuckets = buckets;
        int dstMask = dstBuckets.length-1;
        for (Bucket<B> src : srcBuckets) {
            if (src == null) continue;
            RowBucket<B> srcRows = src.rows;
            for (int r = 0, rows = src.size; r < rows; r++) {
                int hash = enhanceHash(srcRows.hashCode(r)), dstIdx = hash & dstMask;
                var dst = dstBuckets[dstIdx];
                if (dst == null)
                    dstBuckets[dstIdx] = dst = createBucket(bucketCapacity);
                if (dst.add(srcRows, r, hash, tableSize >= weakenAt))
                    ++tableSize;
            }
        }
        this.bucketCapacity = bucketCapacity;
        this.tableSize      = tableSize;
        this.buckets        = dstBuckets;
        this.bucketsMask    = dstBuckets.length-1;
        this.nextRehash     = nextRehash(dstBuckets.length, bucketCapacity);
    }

    private void rehash() {
        var oldBuckets = buckets;
        Bucket<B>[] newBuckets = bucketArray(Math.max(1, oldBuckets.length)<<1);
        if (oldBuckets.length < PAR_RH_THRESHOLD)
            singleThreadRehash(newBuckets);
        else
            new Rehash<>(this, newBuckets).compute();
        bucketArrayPool.offer(oldBuckets, oldBuckets.length);
    }

    /** 75% of total capacity of {@code nBuckets} each with {@code bucketCapacity} */
    private static int nextRehash(int nBuckets, int bucketCapacity) {
        return bucketCapacity*nBuckets - (bucketCapacity>>2)*nBuckets;
    }

    @Override public boolean isDuplicate(B batch, int row, int source) {
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        int readVersion = this.version;
        int bucketIdx = hash&bucketsMask;
        Bucket<B> bucket = buckets[bucketIdx];
        // null bucket means nothing was added to it.
        if (bucket != null && bucket.contains(batch, row, hash))
            return true; // if contains() returns true, it is correct even with data races
        lock();
        try {
            // once we reach MAX_VALUE, stop adding items and simply report anything not
            // contained as added without really adding.
            if (tableSize == MAX_VALUE)
                return false;
            int lockedVersion = this.version;
            if (lockedVersion != readVersion) {
                // there was an insertion between the false contains() and lock()
                bucket = buckets[bucketIdx = hash&bucketsMask];
                if (bucket != null && bucket.contains(batch, row, hash))
                    return true; // row as concurrently inserted before lock()
            }
            boolean imbalanced = bucket != null && bucket.size >= Math.max(32, tableSize>>3);
            // at this point, row must be added to the table
            if ((imbalanced || tableSize >= nextRehash) && buckets.length < MAX_BUCKETS) {
                rehash();
                bucket = buckets[bucketIdx = hash & bucketsMask]; //rehash changed this.buckets
            }
            if (bucket == null) // allocate bucket on first use
                buckets[bucketIdx] = bucket = createBucket(bucketCapacity);
            if (bucket.add(batch, row, hash, tableSize >= weakenAt))
                ++tableSize; // only increment size if bucket.add() did not overwrote another row
            this.version = lockedVersion+1; // notify unlocked checkers we added a row
            return false; // not a duplicate, we just added it
        } finally {
            unlock();
        }
    }

    /**
     * Whether {@code row} has been previously added by {@link Dedup#isDuplicate(B, int, int)}
     * or {@link StrongDedup#add(B, int)}
     */
    @Override public boolean contains(B batch, int row) {
        if (DEBUG) checkBatchType(batch);
        if (buckets == null) return false;
        int hash = enhanceHash(batch.hash(row));
        var bucket = buckets[hash & bucketsMask];
        return bucket != null && bucket.contains(batch, row, hash);
    }

    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        if (buckets != null) {
            for (var b : buckets) {
                if (b != null) b.forEach(consumer);
            }
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

    /* --- --- --- bucket & bucket pooling --- --- --- */

    @SuppressWarnings("unchecked")
    private static ArrayPool<Bucket<?>[]>[] BUCKET_ARRAY_POOLS = new ArrayPool[10];
    private static final int BUCKET_ARRAY_POOL_TINY_CAPACITY = 256;
    private static final int BUCKET_ARRAY_POOL_SMALL_CAPACITY = 1024;
    private static final int BUCKET_ARRAY_POOL_MEDIUM_CAPACITY = 64;
    private static final int BUCKET_ARRAY_POOL_LARGE_CAPACITY = 32;
    private static final int BUCKET_ARRAY_POOL_HUGE_CAPACITY = 8;
    static {
        bucketArrayPool(Batch.TERM);
        bucketArrayPool(Batch.COMPRESSED);
        bucketArrayPool(StoreBatch.TYPE);
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static <B extends Batch<B>> ArrayPool<Bucket<B>[]> bucketArrayPool(BatchType<B> bt) {
        int id = bt.id;
        var pools = BUCKET_ARRAY_POOLS;
        if (id >= pools.length)
            BUCKET_ARRAY_POOLS = pools = copyOf(pools, pools.length+(pools.length>>1));
        var p = (ArrayPool<Bucket<B>[]>)(Object)pools[id];
        if (p == null) {
            var cls = (Class<Bucket<B>[]>) (Object) Bucket[].class;
            LevelPool<Bucket<B>[]> shared = new LevelPool<>(cls,
                    BUCKET_ARRAY_POOL_TINY_CAPACITY,
                    BUCKET_ARRAY_POOL_SMALL_CAPACITY,
                    BUCKET_ARRAY_POOL_MEDIUM_CAPACITY,
                    BUCKET_ARRAY_POOL_LARGE_CAPACITY,
                    BUCKET_ARRAY_POOL_HUGE_CAPACITY);
            p = new ArrayPool<>(shared);
            pools[id] = (ArrayPool<Bucket<?>[]>)(Object)p;
        }
        return p;
    }

    @SuppressWarnings("unchecked") private Bucket<B>[] bucketArray(int nBuckets) {
        Bucket<B>[] arr = bucketArrayPool.getAtLeast(nBuckets);
//        BA_LIVE.getAndAddRelease(1);
        if (arr == null) {
//            BA_CREATED.getAndAddRelease(1);
            arr = new Bucket[nBuckets];
//            BA_LIVESET.put(arr, Boolean.TRUE);
            return arr;
        }
//        BA_LIVESET.put(arr, Boolean.TRUE);
//        BA_REUSED.getAndAddRelease(1);
        for (Bucket<B> bucket : arr) {
            if (bucket == null) continue;
            bucket.parent = this;
            bucket.clear();
        }
        return arr;
    }

    private static final int BUCKET_POOL_CAPACITY = 2048;
    private static final int BUCKET_POOL_THREADS = getRuntime().availableProcessors();
    @SuppressWarnings("unchecked")
    private static AffinityPool<Bucket<?>>[] BUCKET_POOLS = new AffinityPool[10];
    static {
        bucketPool(Batch.TERM);
        bucketPool(Batch.COMPRESSED);
        bucketPool(StoreBatch.TYPE);
    }

    @SuppressWarnings("unchecked")
    private static <B extends Batch<B>> AffinityPool<Bucket<B>> bucketPool(BatchType<B> bt) {
        int id = bt.id;
        var pools = BUCKET_POOLS;
        if (id >= pools.length)
            BUCKET_POOLS = pools = copyOf(pools, pools.length+(pools.length>>1));
        var p = (AffinityPool<Bucket<B>>)(Object)pools[id];
        if (p == null) {
            var cls = (Class<Bucket<B>>)(Object)Bucket.class;
            p = new AffinityPool<>(cls, BUCKET_POOL_CAPACITY, BUCKET_POOL_THREADS);
            pools[id] = (AffinityPool<Bucket<?>>)(Object)p;
        }
        return p;
    }

    private Bucket<B> createBucket(int capacity) {
        var b = bucketPool.get();
        if (b == null) {
            b = new Bucket<>(this, capacity);
        } else {
            b.parent = this;
            b.clear();
        }
        return b;
    }

    private static final class Bucket<B extends Batch<B>> {
        StrongDedup<B> parent;
        RowBucket<B> rows;
        int size;
        int[] hashes;
        long bitset;

        public Bucket(StrongDedup<B> parent, int capacity) {
            this.parent = parent;
            this.hashes = ArrayPool.intsAtLeast(capacity);
            this.rows = parent.bt.createBucket(hashes.length, parent.cols);
        }

        void clear() {
            bitset = 0;
            Arrays.fill(hashes, 0);
            size      = 0;
            if (parent.cols != rows.cols())
                rows.clear(hashes.length, parent.cols);
        }

        int capacity() { return hashes.length; }

        void recycleInternals() {
            hashes = ArrayPool.INT.offer(hashes, hashes.length);
            rows = parent.bt.recycleBucket(rows);
        }

        boolean contains(B batch, int row, int hash) {
            if ((bitset & (1L << hash)) == 0)
                return false; // not present
            int size = this.size, i = 0;
            var rows = this.rows;
            var hashes = this.hashes;
            while (i < size && (hashes[i] != hash || !rows.equals(i, batch, row))) ++i;
            return i < size;
        }

        private int add0(int hash, boolean weak) {
            bitset |= 1L << hash;
            int idx = size, cap = hashes.length;
            if (idx >= cap) {
                if (weak) {
                    idx = 0x80000000 | ((hash&0x7fffffff) % cap);
                } else {
                    hashes = ArrayPool.grow(hashes, cap<<1);
                    rows.grow(hashes.length-cap);
                    size = idx+1;
                }
            } else {
                size = idx+1;
            }
            hashes[idx&0x7fffffff] = hash;
            return idx;
        }

        boolean add(RowBucket<B> rb, int row, int hash, boolean weak) {
            int idx = add0(hash, weak);
            rows.set(idx&0x7fffffff, rb, row);
            return idx >= 0;
        }

        boolean add(B batch, int row, int hash, boolean weak) {
            int idx = add0(hash, weak);
            rows.set(idx&0x7fffffff, batch, row);
            return idx >= 0;
        }

        <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
            for (B batch : rows)
                consumer.accept(batch);
        }
    }
}
