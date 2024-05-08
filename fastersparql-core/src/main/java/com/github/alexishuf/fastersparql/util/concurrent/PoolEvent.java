package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.FSProperties;
import jdk.jfr.*;

@Enabled
@Registered
@Category({"FasterSparql"})
public abstract class PoolEvent extends Event {
    public static final boolean ENABLED = FSProperties.poolStats();

    @Label("Pool Name")
    @Description("pool.name(). This should be unique")
    public String poolName;

    @Label("Object length (items)")
    @DataAmount("items")
    @Description("Length of the object, in number of elements, not bytes. " +
                 "For non-level Alloc instances, this will be 1")
    public int objectLength;

    public void fillAndCommit(StatsPool pool, int objLen) {
        if (ENABLED) {
            poolName = pool.name();
            objectLength = objLen;
            commit();
        }
    }

    @Enabled
    @Label("Get an item from the pool")
    @Name("com.github.alexishuf.fastersparql.util.concurrent.Get")
    @Description("a call to poll*() or create*()")
    @StackTrace(false)
    public static class Get extends PoolEvent {
        public static boolean ENABLED = FSProperties.poolTransactionEvents();
        public static void record(StatsPool pool, int objLen) {
            if (ENABLED)
                new Get().fillAndCommit(pool, objLen);
        }
    }

    @Enabled()
    @Label("Offer an item back into the pool")
    @Name("com.github.alexishuf.fastersparql.util.concurrent.Offer")
    @Description("a call to offer*()")
    @StackTrace(false)
    public static class Offer extends PoolEvent {
        public static boolean ENABLED = FSProperties.poolTransactionEvents();
        public static void record(StatsPool pool, int objLen) {
            if (ENABLED)
                new Offer().fillAndCommit(pool, objLen);
        }
    }

    @Enabled
    @Label("Offer to local stacks")
    @Name("com.github.alexishuf.fastersparql.util.concurrent.OfferToLocals")
    @Description("This event happens when both the thread-affinity and the shared stacks " +
                 "have rejected the offer of this objects due to being full, causing it to be " +
                 "offered to all other thread-affinity stacks. This is undesirable as each" +
                 " offer will will require synchronizing with the threads that have affinity" +
                 " to that stack.")
    @StackTrace()
    public static class OfferToLocals extends PoolEvent {
        public static void record(StatsPool pool, int objLen) {
            if (ENABLED)
                new OfferToLocals().fillAndCommit(pool, objLen);
        }
    }

    @Enabled
    @Label("Offer rejected due to full pool")
    @Name("com.github.alexishuf.fastersparql.util.concurrent.FullPool")
    @Description("An object was offered but not taken by the pool since all stacks were full.")
    @StackTrace
    public static class FullPool extends PoolEvent {
        public static void record(StatsPool pool, int objLen) {
            if (ENABLED)
                new FullPool().fillAndCommit(pool, objLen);
        }
    }

    @Enabled
    @Label("Empty pool")
    @Name("com.github.alexishuf.fastersparql.util.concurrent.EmptyPool")
    @Description("The pool was empty, forcing a new object to be instantiated")
    @StackTrace
    public static class EmptyPool extends PoolEvent {
        public static void record(StatsPool pool, int objLen) {
            if (ENABLED)
                new EmptyPool().fillAndCommit(pool, objLen);
        }
    }
}
