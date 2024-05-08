package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.*;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.function.IntFunction;

import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.recycleIntsAndGetEmpty;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Integer.*;
import static java.lang.Thread.onSpinWait;
import static java.util.Arrays.copyOf;

public sealed abstract class StrongDedup<B extends Batch<B>> extends Dedup<B, StrongDedup<B>> {
    private static final boolean DEBUG = StrongDedup.class.desiredAssertionStatus();
    private static final Bucket<?>[] EMPTY_BUCKETS = new Bucket[0];
    @SuppressWarnings("unchecked") private static <B extends Batch<B>> Bucket<B>[] emptyBuckets() {
        return (Bucket<B>[]) EMPTY_BUCKETS;
    }

    private static final LevelAlloc.Capacities BUCKET_POOL_CAPACITIES = new LevelAlloc.Capacities()
            .set(4, 4, Alloc.THREADS*1024) // 16 rows
            .set(5, 5, Alloc.THREADS*256)  // 32 rows
            .set(6, 6, Alloc.THREADS*64)   // 64 rows
            .set(7, 7, Alloc.THREADS*8)    // 128 rows
            .set(8, 8, Alloc.THREADS*2)    // 256 rows
            .set(9, 10, 1);                // up to 1024 rows
    private static final int MAX_BUCKETS      = 1024*1024;
    private static final int RH_THREADS       = ForkJoinPool.commonPool().getParallelism();
    private static final int PAR_RH_THRESHOLD = RH_THREADS == 1 ? MAX_VALUE : RH_THREADS*64;

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
    private Bucket<B>[] buckets;
    /** When tableSize reaches this, perform a rehash */
    private int nextRehash;
    /** Capacity for new instantiations of Bucket */
    private int bucketCapacity;
    /** When new additions will have weak semantics (instead of re-hashing, another
     *  addition also made under weak semantics will be overwritten). */
    private final int weakenAt;
    /** A pool for {@link Bucket}s for {@code this.bt} */
    private final LevelAlloc<Bucket<B>> bucketAlloc;

    private StrongDedup(BatchType<B> bt, int initialCapacity, int weakenAt, int cols) {
        super(bt, cols);
        this.bucketAlloc      = bucketAlloc(bt);
        this.weakenAt         = weakenAt;
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Negative initialCapacity");
        int nBuckets = initialCapacity <= 8 ? 1
                : 1 << (32-numberOfLeadingZeros((initialCapacity>>3)-1));
        //noinspection unchecked
        buckets        = (Bucket<B>[])BUCKET_ARRAY.createAtLeast(nBuckets);
        bucketCapacity = 16;
        bucketsMask    = nBuckets-1;
        nextRehash     = nextRehash(nBuckets, bucketCapacity);
    }

    protected static final class Concrete<B extends Batch<B>> extends StrongDedup<B>
            implements Orphan<StrongDedup<B>> {
        public Concrete(BatchType<B> bt, int initialCapacity, int weakenAt, int cols) {
            super(bt, initialCapacity, weakenAt, cols);
        }
        @Override public StrongDedup<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable StrongDedup<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        // invoke recycleBuckets() asynchronously since it must visit each bucket
        BUCKET_ARRAY_RECYCLER.sched(buckets, buckets.length);
        buckets    = emptyBuckets();
        tableSize  = 0;
        nextRehash = 0;
        bitset     = HashBitset.recycle(bitset);
        return null;
    }

    private static final class BucketArrayRecycler
            extends LevelCleanerBackgroundTask<Bucket<?>[]>  {
        private int garbageBuckets;
        private int pooledBuckets;

        public BucketArrayRecycler() {
            super("StrongDedup-Bucket[]-recycler", Bucket[]::new);
            FS.addShutdownHook(this::dumpStats);
        }

        public void dumpStats() {
            sync();
            double nBuckets   = Math.max(1, pooledBuckets+garbageBuckets);
            double nArrays = Math.max(1, objsPooled+fullPoolGarbage);
            System.err.printf("""
                    StrongDedup.Buckets     pooled: %,9d (%5.3f%%)
                    StrongDedup.Buckets    garbage: %,9d (%5.3f%%)
                    StrongDedup.Bucket[]s   pooled: %,9d (%5.3f%%)
                    StrongDedup.Bucket[]s no space: %,9d (%5.3f%%)
                    """,
                    pooledBuckets,   100.0 *  pooledBuckets/nBuckets,
                    garbageBuckets,  100.0 * garbageBuckets/nBuckets,
                    objsPooled,      100.0 *      objsPooled/nArrays,
                    fullPoolGarbage, 100.0 * fullPoolGarbage/nArrays
            );
        }

        @Override protected void clear(Bucket<?>[] buckets) {
            for (int i = 0; i < buckets.length; i++) {
                Bucket<?> b = buckets[i];
                if (b != null) {
                    if (b.offerToPool(buckets)) {
                        ++pooledBuckets;
                    } else {
                        ++garbageBuckets;
                        b.markGarbage();
                    }
                    buckets[i] = null;
                }
            }
        }
    }
    private static final BucketArrayRecycler BUCKET_ARRAY_RECYCLER = new BucketArrayRecycler();

    @Override public void clear(int cols) {
        tableSize = 0;
        this.cols = cols;
        for (Bucket<B> b : buckets) {
            if (b != null)
                b.clear();
        }
    }

    @Override public int capacity() {
        long sum = 0;
        for (var b : buckets)
            sum += b == null ? bucketCapacity : b.capacity();
        return (int)Math.min(MAX_VALUE, sum);
    }

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
                            if (dst == null) {
                                dst = parent.createBucket(newBuckets, bucketCapacity);
                                newBuckets[dstIdx] = dst;
                            }
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
            RowBucket<B, ?> srcRows = src.rows;
            for (int r = 0, rows = src.size; r < rows; r++) {
                int hash = enhanceHash(srcRows.hashCode(r)), dstIdx = hash & dstMask;
                var dst = dstBuckets[dstIdx];
                if (dst == null)
                    dstBuckets[dstIdx] = dst = createBucket(dstBuckets, bucketCapacity);
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
        @SuppressWarnings("unchecked") var newBuckets = (Bucket<B>[])
                BUCKET_ARRAY.createAtLeast(Math.max(1, oldBuckets.length)<<1);
        if (oldBuckets.length < PAR_RH_THRESHOLD)
            singleThreadRehash(newBuckets);
        else
            new Rehash<>(this, newBuckets).compute();
        BUCKET_ARRAY_RECYCLER.sched(oldBuckets, oldBuckets.length);
    }

    /** 75% of total capacity of {@code nBuckets} each with {@code bucketCapacity} */
    private static int nextRehash(int nBuckets, int bucketCapacity) {
        return bucketCapacity*nBuckets - (bucketCapacity>>2)*nBuckets;
    }

    @Override public boolean isDuplicate(B batch, int row, int source) {
        requireAlive();
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
                buckets[bucketIdx] = bucket = createBucket(buckets, bucketCapacity);
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
        requireAlive();
        if (DEBUG) checkBatchType(batch);
        if (buckets == null) return false;
        int hash = enhanceHash(batch.hash(row));
        var bucket = buckets[hash & bucketsMask];
        return bucket != null && bucket.contains(batch, row, hash);
    }

    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        requireAlive();
        for (var b : buckets) {
            if (b != null) b.forEach(consumer);
        }
    }

    @SuppressWarnings("unused") // used only when debugging
    public String dump() {
        var sb = new StringBuilder();
        forEach(b -> {
            for (var n = b; n != null; n = n.next) {
                for (int r = 0; r < n.rows; r++) sb.append(n.toString(r)).append('\n');
            }
        });
        sb.setLength(Math.max(1, sb.length()-1));
        return sb.toString();
    }

    /* --- --- --- bucket & bucket pooling --- --- --- */

    private static final LevelAlloc<Bucket<?>[]> BUCKET_ARRAY;
    static {
        @SuppressWarnings({"unchecked", "RedundantCast"})
        var cls = (Class<Bucket<?>[]>)(Object)Bucket[].class;
        BUCKET_ARRAY = new LevelAlloc<>(cls, "StrongDedup.BUCKET_ARRAY",
                20, 4, BUCKET_ARRAY_RECYCLER.clearElseMake,
                new LevelAlloc.Capacities()
                        .set(0, 8, Alloc.THREADS*32)
                        .set(9, 15, Alloc.THREADS*8));
        BUCKET_ARRAY_RECYCLER.pool = BUCKET_ARRAY;
        Primer.INSTANCE.sched(() -> {
            for (int i = 0; i <= 8; i++)
                BUCKET_ARRAY.primeLevel(Bucket[]::new, i);
        });
    }

    @SuppressWarnings("unchecked")
    private static LevelAlloc<Bucket<?>>[] BUCKET_POOLS = new LevelAlloc[10];
    static {
        bucketAlloc(TermBatchType.TERM);
        bucketAlloc(CompressedBatchType.COMPRESSED);
        bucketAlloc(StoreBatchType.STORE);
    }

    private static final class BucketFactory<B extends Batch<B>> implements IntFunction<Bucket<B>> {
        private final BatchType<B> bt;
        private @MonotonicNonNull LevelAlloc<Bucket<B>> alloc;

        private BucketFactory(BatchType<B> bt) {
            this.bt = bt;
        }

        @Override public Bucket<B> apply(int capacity) {
            return new Bucket<>(bt, alloc, capacity, 8).takeOwnership(RECYCLED);
        }
    }

    @SuppressWarnings("unchecked")
    private static <B extends Batch<B>> LevelAlloc<Bucket<B>> bucketAlloc(BatchType<B> bt) {
        int id = bt.id;
        var old = id < BUCKET_POOLS.length
                ? (LevelAlloc<Bucket<B>>)(Object)BUCKET_POOLS[id] : null;
        if (old != null)
            return old;
        if (id >= BUCKET_POOLS.length)
            BUCKET_POOLS = copyOf(BUCKET_POOLS, BUCKET_POOLS.length<<1);
        var cls = (Class<Bucket<B>>)(Object)Bucket.class;
        int fixedCost = bt.bucketBytesCost(0, 0);
        int perRowCost = (bt.bucketBytesCost(16, 8) - fixedCost) / 16;

        BucketFactory<B> fac = new BucketFactory<>(bt);
        var p = new LevelAlloc<>(cls, "StrongDedup.Buckets("+bt+")", fixedCost, perRowCost,
                             fac, BUCKET_POOL_CAPACITIES);
        fac.alloc = p;
        BUCKET_POOLS[id] = (LevelAlloc<Bucket<?>>)(Object)p;
//        Primer.INSTANCE.sched(() -> {
//            for (int level = 4; level <= 8; level++)
//                p.primeLevelLocalAndShared(level);
//        });
        return p;
    }

    private Bucket<B> createBucket(Bucket<B>[] owner, int capacity) {
        requireAlive();
        var b = bucketAlloc.createAtLeast(capacity);
        b.thaw(owner, this);
        return b;
    }

    private static final class Bucket<B extends Batch<B>>
            extends AbstractOwned<Bucket<B>> implements Orphan<Bucket<B>> {
        private RowBucket<B, ?> rows;
        private int size;
        private int[] hashes;
        private long bitset;
        private final LevelAlloc<Bucket<B>> pool;

        public Bucket(BatchType<B> type, LevelAlloc<Bucket<B>> pool, int capacity, int cols) {
            this.pool   = pool;
            this.hashes = ArrayAlloc.intsAtLeast(capacity);
            this.rows   = type.createBucket(hashes.length, cols).takeOwnership(this);
        }

        @Override public Bucket<B> takeOwnership(Object o) {return takeOwnership0(o);}

        public void thaw(Bucket<B>[] owner, StrongDedup<B> parent) {
            releaseOwnership(RECYCLED).takeOwnership(owner);
            int expectedCols = parent.cols;
            if (expectedCols != rows.cols())
                rows.clear(parent.bucketCapacity, expectedCols);
        }

        @Override public @Nullable Bucket<B> recycle(Object currentOwner) {
            if (!offerToPool(currentOwner))
                markGarbage();
            return null;
        }

        boolean offerToPool(Object currentOwner) {
            clear();
            internalMarkRecycled(currentOwner);
            return hashes.length > 0 && pool.offer(this, hashes.length) == null;
        }

        void markGarbage() {
            internalMarkGarbage(RECYCLED);
            hashes = recycleIntsAndGetEmpty(hashes);
            rows   = rows.recycle(this);
        }

        void clear() {
            bitset = 0;
            size   = 0;
            Arrays.fill(hashes, 0);
            rows.clear(hashes.length, rows.cols());
        }

        int capacity() { return hashes.length; }

        boolean contains(B batch, int row, int hash) {
            if (DEBUG) requireAlive();
            if ((bitset & (1L << hash)) == 0)
                return false; // not present
            int size = this.size, i = 0;
            var rows = this.rows;
            var hashes = this.hashes;
            while (i < size && (hashes[i] != hash || !rows.equals(i, batch, row))) ++i;
            return i < size;
        }

        private int add0(int hash, boolean weak) {
            if (DEBUG) requireAlive();
            bitset |= 1L << hash;
            int idx = size, cap = hashes.length;
            if (idx >= cap) {
                if (weak) {
                    idx = 0x80000000 | ((hash&0x7fffffff) % cap);
                } else {
                    hashes = ArrayAlloc.grow(hashes, cap<<1);
                    rows.grow(hashes.length-cap);
                    size = idx+1;
                }
            } else {
                size = idx+1;
            }
            hashes[idx&0x7fffffff] = hash;
            return idx;
        }

        boolean add(RowBucket<B, ?> rb, int row, int hash, boolean weak) {
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
            requireAlive();
            for (B batch : rows)
                consumer.accept(batch);
        }
    }
}
