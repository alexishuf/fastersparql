package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.concurrent.GlobalAffinityShallowPool;
import jdk.jfr.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

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
    /** Whether Batch.finalize() is implemented. Without finalize() leaks are not detected */
    private static final boolean REPORT_LEAKED;

    static {
        boolean hasFinalize = false;
        try {
            if (FSProperties.batchPooledMark()) { //noinspection JavaReflectionMemberAccess
                Batch.class.getDeclaredMethod("finalize");
                hasFinalize = true;
            }
        } catch (Throwable ignored) { }
        REPORT_LEAKED = hasFinalize;
    }

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
        String leaked = REPORT_LEAKED ? String.format("%,6d", (int)LEAKED.getOpaque()) : " (off)";
        System.err.printf("""
                  Pooled: %,6d
                Unpooled: %,6d
                 Garbage: %,6d
                  Leaked: %s
                 Created: %,6d
                   Grown: %,6d
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

    protected void fillCommitAndRecycle(Batch<?> b, int poolCol) {
        termsCapacity = b.termsCapacity();
        bytesCapacity = b.totalBytesCapacity();
        commit();
        GlobalAffinityShallowPool.offer(poolCol, this);
    }

    @Label("Batch pooled")
    @Name("com.github.alexishuf.fastersparql.batch.Pooled")
    @Description("A batch entered a pool")
    public static class Pooled extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /** Creates and {@link Event#commit()}s a {@link Pooled} event with given {@code capacity}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            POOLED.getAndAddRelease(1);
            Pooled e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new Pooled() : e).fillCommitAndRecycle(batch, POOL_COL);
        }
    }

    @Label("Batch unpooled")
    @Name("com.github.alexishuf.fastersparql.batch.Unpooled")
    @Description("The batch left a pool to be used")
    public static class Unpooled extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /** Creates and {@link #commit()}s a {@link Unpooled} event with given {@code capacity}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            UNPOOLED.getAndAddRelease(1);
            Unpooled e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new Unpooled() : e).fillCommitAndRecycle(batch, POOL_COL);
        }
    }

    @Label("Batch created")
    @Name("com.github.alexishuf.fastersparql.batch.Created")
    @Description("A new Batch instance was created instead of acquiring one from a pool")
    public static class Created extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /** Creates and {@link #commit()}s a {@link Created} event for the given {@code batch}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            CREATED.getAndAddRelease(1);
            Created e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new Created() : e).fillCommitAndRecycle(batch, POOL_COL);
        }
    }

    @SuppressWarnings("unused") // Batch.finalize() is commented-out
    @Label("Batch leaked")
    @Name("com.github.alexishuf.fastersparql.batch.Leaked")
    @Description("A batch is leaked if was created or unpooled but never offered back to a pool, " +
                 "which would cause it be marked as pooled or as (intentional) garbage.")
    @StackTrace(false)
    public static class Leaked extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /** Creates and {@link #commit()}s a {@link Leaked} event for the given {@code batch}. */
        public static void record(Batch<?> b) {
            if (!RECORD) return;
            LEAKED.getAndAddRelease(1);
            Leaked e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new Leaked() : e).fillCommitAndRecycle(b, POOL_COL);
        }
    }

    @Label("Batch turned garbage")
    @Name("com.github.alexishuf.fastersparql.batch.Garbage")
    @Description("The batch was offered to a pool which was full and thus was left for collection")
    public static class Garbage extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /** Creates and {@link #commit()}s a {@link Garbage} event with given {@code capacity}. */
        public static void record(Batch<?> batch) {
            if (!RECORD) return;
            GARBAGE.getAndAddRelease(1);
            Garbage e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new Garbage() : e).fillCommitAndRecycle(batch, POOL_COL);
        }
    }

    @Label("Batch internal storage for local segments grown")
    @Name("com.github.alexishuf.fastersparql.batch.LocalsGrown")
    @Description("A batch had its internal storage for local segments reallocated. " +
                 "The capacity represents the capacity after the growth.")
    public static class LocalsGrown extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /**
         * Creates and {@link #commit()}s a {@link LocalsGrown} event.
         * @param batch The {@link Batch} after it has grown.
         */
        public static <B extends Batch<B>> void record(Batch<?> batch) {
            if (!RECORD) return;
            LocalsGrown e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new LocalsGrown() : e).fillCommitAndRecycle(batch, POOL_COL);
        }
    }

    @Label("Batch contents moved to a new bigger batch")
    @Name("com.github.alexishuf.fastersparql.batch.TermsGrown")
    @Description("A batch had its internal storage reallocate to hold more terms. " +
                 "The capacity represents the capacity after the growth.")
    public static class TermsGrown extends BatchEvent {
        private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

        /**
         * Creates and {@link #commit()}s a {@link TermsGrown} event.
         * @param batch The {@link Batch} after it has grown.
         */
        public static <B extends Batch<B>> void record(Batch<?> batch) {
            if (!RECORD) return;
            TermsGrown e = GlobalAffinityShallowPool.get(POOL_COL);
            (e == null ? new TermsGrown() : e).fillCommitAndRecycle(batch, POOL_COL);
        }
    }
}
