package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.PoolCleaner;
import com.github.alexishuf.fastersparql.util.owned.LeakDetector;
import jdk.jfr.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.arrayElementVarHandle;
import static java.lang.invoke.MethodType.methodType;

@SuppressWarnings("unused")
@Enabled
@Registered
@StackTrace
@Category({"FasterSparql", "Batch"})
public abstract class BatchEvent extends Event {
    private static final VarHandle POOLED, UNPOOLED, GARBAGE, CREATED, LEAKED, GROWN;
    @SuppressWarnings("unused") private static int plainPooled,  plainUnpooled, plainGarbage;
    @SuppressWarnings("unused") private static int plainCreated, plainLeaked,   plainGrown;
    static {
        try {
            POOLED   = MethodHandles.lookup().findStaticVarHandle(BatchEvent.class, "plainPooled",   int.class);
            UNPOOLED = MethodHandles.lookup().findStaticVarHandle(BatchEvent.class, "plainUnpooled", int.class);
            GARBAGE  = MethodHandles.lookup().findStaticVarHandle(BatchEvent.class, "plainGarbage",  int.class);
            CREATED  = MethodHandles.lookup().findStaticVarHandle(BatchEvent.class, "plainCreated",  int.class);
            LEAKED   = MethodHandles.lookup().findStaticVarHandle(BatchEvent.class, "plainLeaked",   int.class);
            GROWN    = MethodHandles.lookup().findStaticVarHandle(BatchEvent.class, "plainGrown",    int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    /** Whether the {@code record} methods should effectively create, fill and commit events. */
    public static final boolean RECORD = FSProperties.batchJFREnabled();

    @SuppressWarnings("unused") public static void resetCounters() {
        POOLED  .setOpaque(0);
        UNPOOLED.setOpaque(0);
        CREATED .setOpaque(0);
        LEAKED  .setOpaque(0);
        GARBAGE .setOpaque(0);
        GROWN   .setOpaque(0);
    }

    static {
        if (RECORD) {
            FS.addShutdownHook(BatchEvent::dumpStats);
        }
    }

    public static void dumpStats() {
        if (LeakDetector.ENABLED) {
            PoolCleaner.INSTANCE.sync();
            System.gc();
            Async.uninterruptibleSleep(500);
        }
        var leaked = LeakDetector.ENABLED ? format("%,9d", (int)LEAKED.getOpaque()) : " (off)";
        System.err.printf("""
                Batches   Pooled: %,9d
                Batches Unpooled: %,9d
                Batches  Garbage: %,9d
                Batches   Leaked: %s
                Batches  Created: %,9d
                Batches    Grown: %,9d
                """, (int)POOLED.getOpaque(), (int)UNPOOLED.getOpaque(), (int)GARBAGE.getOpaque(),
                     leaked,                  (int)CREATED.getOpaque(),  (int)GROWN.getOpaque());
    }

    @DataAmount @Label("Capacity (terms)")
    @Description("How many terms (rows*columns) the batch can hold after a clear() without " +
                 "requiring re-allocation of internal data structures")
    public int termsCapacity;

    @DataAmount @Label("Capacity (bytes)")
    @Description("How many bytes of storage the batch directly held. Note that a batch may " +
            "hold more bytes than required by its current number of rows and columns. Also " +
            "note that some batch implementations hold references to objects that hold the " +
            "actual string representation of RDF terms instead of holding the strings " +
            "directly.")
    public int bytesCapacity;

    protected void fillAndCommit(Batch<?> b) {
        termsCapacity = b.termsCapacity();
        bytesCapacity = b.totalBytesCapacity();
        commit();
    }

    private static final VarHandle POOL_HANDLE = arrayElementVarHandle(BatchEvent[].class);
    private static final int POOL_BUCKETS
            = 1 << (32-numberOfLeadingZeros(getRuntime().availableProcessors()*(128/4)-1));
    private static final int POOL_BUCKETS_MASK = POOL_BUCKETS-1;
    static {assert Integer.bitCount(POOL_BUCKETS) == 1;}

    @SuppressWarnings("unchecked")
    private static <T extends BatchEvent> T[] createPool(Class<T> cls) {
        T[] pool = (T[]) Array.newInstance(cls, POOL_BUCKETS);
        try {
            var c = MethodHandles.lookup().findConstructor(cls, methodType(void.class));
            for (int i = 0; i < pool.length; i++)
                pool[i] = (T)c.invoke();
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
        return pool;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean fillAndCommitPooled(BatchEvent[] pool, Batch<?> batch) {
        int bucket = (int)Thread.currentThread().threadId()&POOL_BUCKETS_MASK;
        BatchEvent e = (BatchEvent) POOL_HANDLE.getAndSetAcquire(pool, bucket, null);
        if (e != null) {
            e.fillAndCommit(batch);
            return true;
        }
        return false;
    }

    @Label("Batch pooled")
    @Name("com.github.alexishuf.fastersparql.batch.Pooled")
    @Description("A batch entered a pool")
    public static class Pooled extends BatchEvent {
        private static final Pooled[] EV_POOL = createPool(Pooled.class);

        /** Creates and {@link Event#commit()}s a {@link Pooled} event with given {@code capacity}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            POOLED.getAndAddRelease(1);
            if (!fillAndCommitPooled(EV_POOL, batch))
                new Pooled().fillAndCommit(batch);
        }
    }

    @Label("Batch unpooled")
    @Name("com.github.alexishuf.fastersparql.batch.Unpooled")
    @Description("The batch left a pool to be used")
    public static class Unpooled extends BatchEvent {
        private static final Unpooled[] EV_POOL = createPool(Unpooled.class);

        /** Creates and {@link #commit()}s a {@link Unpooled} event with given {@code capacity}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            UNPOOLED.getAndAddRelease(1);
            if (!BatchEvent.fillAndCommitPooled(EV_POOL, batch))
                new Unpooled().fillAndCommit(batch);
        }
    }

    @Label("Batch created")
    @Name("com.github.alexishuf.fastersparql.batch.Created")
    @Description("A new Batch instance was created instead of acquiring one from a pool")
    public static class Created extends BatchEvent {
        private static final Created[] EV_POOL = createPool(Created.class);

        /** Creates and {@link #commit()}s a {@link Created} event for the given {@code batch}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            CREATED.getAndAddRelease(1);
            if (!BatchEvent.fillAndCommitPooled(EV_POOL, batch))
                new Created().fillAndCommit(batch);
        }
    }

    @Label("Batch leaked")
    @Name("com.github.alexishuf.fastersparql.batch.Leaked")
    @Description("A batch is leaked if was created or unpooled but never offered back to a pool, " +
                 "which would cause it be marked as pooled or as (intentional) garbage.")
    @StackTrace(false)
    public static class Leaked extends BatchEvent {
        private static final Leaked[] EV_POOL = createPool(Leaked.class);

        public void fillAndCommit(int termsCapacity, int bytesCapacity) {
            LEAKED.getAndAddRelease(1);
            this.termsCapacity = termsCapacity;
            this.bytesCapacity = bytesCapacity;
            commit();
        }
    }

    @Label("Batch turned garbage")
    @Name("com.github.alexishuf.fastersparql.batch.Garbage")
    @Description("The batch was offered to a pool which was full and thus was left for collection")
    public static class Garbage extends BatchEvent {
        private static final Garbage[] EV_POOL = createPool(Garbage.class);

        /** Creates and {@link #commit()}s a {@link Garbage} event with given {@code capacity}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            GARBAGE.getAndAddRelease(1);
            if (!BatchEvent.fillAndCommitPooled(EV_POOL, batch))
                new Garbage().fillAndCommit(batch);
        }
    }

    @Label("Batch internal storage for local segments grown")
    @Name("com.github.alexishuf.fastersparql.batch.LocalsGrown")
    @Description("A batch had its internal storage for local segments reallocated. " +
                 "The capacity represents the capacity after the growth.")
    public static class LocalsGrown extends BatchEvent {
        private static final LocalsGrown[] EV_POOL = createPool(LocalsGrown.class);

        /**
         * Creates and {@link #commit()}s a {@link LocalsGrown} event.
         * @param batch The {@link Batch} after it has grown.
         */
        public static <B extends Batch<B>> void record(Batch<?> batch) {
            if (!RECORD) return;
            if (!BatchEvent.fillAndCommitPooled(EV_POOL, batch))
                new LocalsGrown().fillAndCommit(batch);
        }
    }

    @Label("Batch contents moved to a new bigger batch")
    @Name("com.github.alexishuf.fastersparql.batch.TermsGrown")
    @Description("A batch had its internal storage reallocate to hold more terms. " +
                 "The capacity represents the capacity after the growth.")
    public static class TermsGrown extends BatchEvent {
        private static final TermsGrown[] EV_POOL = createPool(TermsGrown.class);

        /**
         * Creates and {@link #commit()}s a {@link TermsGrown} event.
         * @param batch The {@link Batch} after it has grown.
         */
        public static <B extends Batch<B>> void record(Batch<?> batch) {
            if (!RECORD) return;
            if (!BatchEvent.fillAndCommitPooled(EV_POOL, batch))
                new TermsGrown().fillAndCommit(batch);
        }
    }
}
