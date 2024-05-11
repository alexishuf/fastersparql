package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.BackgroundTask;
import com.github.alexishuf.fastersparql.util.concurrent.BackgroundTasks;
import jdk.jfr.Event;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.sql.Ref;
import java.util.concurrent.CountDownLatch;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.render;

public class LeakDetector implements BackgroundTask {
    private static final Logger log = LoggerFactory.getLogger(LeakDetector.class);
    private static final LeakDetector INSTANCE = new LeakDetector();
    private static final boolean STACK_TRACE = FSProperties.ownedStackTrace();
    private static final boolean TRACE       = FSProperties.ownedTrace();
    private static final boolean PRINT       = FSProperties.ownedPrintLeaks();
    private static final boolean JFR         = FSProperties.ownedJFRLeaks();

    public static final boolean ENABLED = FSProperties.ownedDetectLeaks();

    private PrintStream out = System.err;
    private boolean notifiedIOException;
    private final Cleaner cleaner;

    private LeakDetector() {
        cleaner = Cleaner.create(r -> {
            Thread thread = new Thread(r, "LeakDetector");
            thread.setDaemon(true);
            thread.setPriority(Thread.MIN_PRIORITY);
            return thread;
        });
        BackgroundTasks.register(this);
    }

    public static void register(Owned<?> owned, LeakState leakState) {
        INSTANCE.cleaner.register(owned, leakState);
    }

    /**
     * Blocks the caller thread until the {@link LeakDetector} thread has cleared the queue of
     * unreachable (and finalized) references to {@link Owned} instances.
     */
    public void sync(CountDownLatch latch) {
        Thread.startVirtualThread(() -> {
            Async.uninterruptibleSleep(100);
            latch.countDown();
        });
    }

    /**
     * Report leaked {@link Owned} instances to {@code out}.
     *
     * <p>The {@link LeakDetector} thread will not close {@code out}. However, it an error
     * happens while writing to {@code out} (e.g., {@code out} being concurrently closed),
     * {@link LeakDetector} will silently revert to writing reports back into the default
     * destination: {@link System#err}.</p>
     *
     * @param out destination of leak reports
     */
    @SuppressWarnings("unused")
    public static void reportTo(@NotOwning @Nullable PrintStream out) {
        INSTANCE.out = out;
        INSTANCE.notifiedIOException = false;
    }

    private void reportPrintError() {
        if (out.checkError() && !notifiedIOException) {
            notifiedIOException = true;
            log.error("IOException on LeakDetector output {}{}", out,
                    out != System.err ? ", reverting output to System.err" : "");
            if (out != System.err) {
                out = System.err;
                notifiedIOException = false;
            }
        }
    }

    /**
     * A {@link WeakReference} that holds ownership information of a {@link Owned} instance
     * without holding a strong reference to the {@link Owned} object itself.
     *
     * <p>The mere existence of this reference object enables leak detection for the referent
     * (the {@link Owned} instance). When the referent is collected, this weak reference will be
     * enqueued by the garbage collector into a queue that is consumed by the
     * {@link LeakDetector}.</p>
     *
     * <p>This class may be extended to track additional information about the referent object.
     * However, care should be taken to not hold a (direct or indirect) strong reference to the
     * referent {@link Owned}. Having the referent strongly reachable from this
     * {@link WeakReference} will make it impossible for the Gc to ever mark and collect the
     * object, causing a memory leak.</p>
     */
    public static class LeakState implements Runnable {
        private boolean leak;
        private final @Nullable String ownedIfNoTrace;
        private final @Nullable OwnershipHistory history;
        private @Nullable String ownerIfNoTrace;
        private @Nullable String rootOwnerIfNoTrace;

        public LeakState(Owned<?> owned, @Nullable OwnershipHistory history) {
            this.ownedIfNoTrace = STACK_TRACE ? null : render(owned);
            this.history = history;
            this.leak = true;
            this.leak = owned != null && owned.isAliveAndMarking();
        }

        public final void update(@Nullable Object newOwner) {
            this.leak = !(newOwner instanceof LeakyOwner);
            if (!TRACE || history == null) {
                ownerIfNoTrace = render(newOwner);
                var root = newOwner instanceof Owned<?> o ? o.rootOwner() : newOwner;
                rootOwnerIfNoTrace = root == newOwner ? ownerIfNoTrace : render(root);
            }
        }

        @Override public void run() {
            if (leak) {
                if (PRINT) {
                    var out = LeakDetector.INSTANCE.out;
                    if (out != null) {
                        try {
                            singleThreadPrintLeak(out);
                        } catch (Throwable t) {
                            log.error("Could not print leak for {}", this, t);
                        }
                    }
                    INSTANCE.reportPrintError();
                }
                if (JFR) {
                    try {
                        singleThreadFillAndCommitJFR();
                    } catch (Throwable t) {
                        log.error("Could not record JFR leak event for {}", this, t);
                    }
                }
            }
        }

        /**
         * If leak detection and JFR reporting of leaks are enabled and the referent of this
         * {@link Ref} has leaked, this method will be called from the single {@link LeakDetector}
         * thread.
         *
         * <p>The default implementation fills and {@link Event#commit()}s a {@code OwnedLeak}
         * event. Subclasses may instead emit another JFR event.</p>
         *
         * <p>This method will only be called from the single {@link LeakDetector} thread.
         * Therefore it is safe to use a {@code private static final} JFR event object to avoid
         * allocations.</p>
         */
        protected void singleThreadFillAndCommitJFR() {
            leakEvent.commit();
        }
        private static final OwnedLeak leakEvent = new OwnedLeak();

        /**
         * If leak detection and printing of leak events are enabled and the {@link LeakDetector}
         * has deemed the referent of this {@link Ref} to be a leak, this method will be called
         * to print a description of the leaked object and its ownership history to {@code out}.
         *
         * <p>Like {@link #singleThreadFillAndCommitJFR()}, this will be called from a
         * single {@link LeakDetector} thread. Therefore it is safe to use
         * {@code private static} fields for working memory.</p>
         *
         * @param out destination where to print
         */
        protected void singleThreadPrintLeak(PrintStream out) {
            SB.setLength(0);
            SB.append("LEAK");
            if (ownedIfNoTrace != null)
                SB.append(' ').append(ownedIfNoTrace);
            appendShortProperties(SB);
            if (TRACE && history != null) {
                Object root = history.lastRootOwner(), last = history.lastOwner();
                if (root != last)
                    SB.append(" last root=").append(root);
                SB.append(" last owners: ");
                history.reverseHistory(SB);
            } else {
                SB.append(" owner=").append(ownerIfNoTrace);
                //noinspection StringEquality
                if (rootOwnerIfNoTrace != ownerIfNoTrace)
                    SB.append(" root=").append(rootOwnerIfNoTrace);
            }
            if (STACK_TRACE && history != null) {
                for (var e = history.lastEvent(); e != null && !e.isOmitted(); e = e.getCause()) {
                    SB.append(INDENT_1).append(e.getMessage());
                    for (var element : e.getStackTrace()) {
                        SB.append(INDENT_2).append(element);
                    }
                }
            }
            out.append(SB.append('\n'));
        }
        private static final StringBuilder SB = new StringBuilder();
        private static final String INDENT_1 = "\n  ";
        private static final String INDENT_2 = "\n    ";

        @SuppressWarnings("SameParameterValue")
        protected void appendShortProperties(StringBuilder out) {}
    }
}
