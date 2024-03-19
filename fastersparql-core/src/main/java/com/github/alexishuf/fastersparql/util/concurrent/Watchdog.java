package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.util.CloseShieldOutputStream;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.TeeOutputStream;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.WITH_STATE_AND_STATS;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.*;

@SuppressWarnings("UnusedReturnValue")
public class Watchdog implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Watchdog.class);
    private static final VarHandle DEADLINE;
    private static final MethodHandle ncdDumpAndReset;
    private static final MethodHandle ncdReset;

    static {
        try {
            DEADLINE = lookup().findVarHandle(Watchdog.class, "deadline", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
        MethodHandle dar = null, r = null;
        try {
            var ncd = Watchdog.class.getClassLoader().loadClass("com.github.alexishuf.fastersparql.client.netty.util.NettyChannelDebugger");
            try {
                dar = lookup().unreflect(ncd.getMethod("dumpAndReset", PrintStream.class));
                r   = lookup().unreflect(ncd.getMethod("reset"));
            } catch (NoSuchMethodException|IllegalAccessException|SecurityException e) {
                throw new ExceptionInInitializerError(e);
            }
        } catch (ClassNotFoundException ignored) {}
        ncdDumpAndReset = dar;
        ncdReset = r;
    }

    public static void reset() {
        ThreadJournal.resetJournals();
        ResultJournal.clear();
        try {
            if (ncdReset != null)
                ncdReset.invokeExact();
        } catch (Throwable e) {
            log.error("Failed to invoke NettyChannelDebugger.reset()", e);
        }
    }

    public static Spec spec(String name) { return new Spec(name); }

    @SuppressWarnings("unused")
    public static final class Spec implements Runnable {
        private static final int DST_STDOUT = 0x1;
        private static final int DST_STDERR = 0x2;
        private static final int DST_FILE   = 0x4;
        private static final File tmp;

        static {
            File dir = new File("/tmp");
            tmp = dir.isDirectory() ? dir : null;
        }

        private @Nullable File destDir = tmp;
        private int threadDst     = DST_FILE;
        private int resultsDst    = DST_FILE;
        private int ncdDst        = DST_FILE;
        private int emitterSvcDst = DST_FILE;
        private int threadCols;
        private @Nullable StreamNode streamNode;
        private @Nullable Plan plan;
        private CharSequence sparql;
        private final String name;

        public Spec(String name) {
            this.name = name;
        }

        public @This Spec dir(String path) { return dir(new File(path)); }
        public @This Spec dir(Path path) { return dir(path.toFile()); }
        public @This Spec dir(File dir) {
            this.destDir = dir;
            return this;
        }
        public @This Spec noDir() {
            this.destDir = null;
            return this;
        }

        public @This Spec plan(Plan plan) {
            this.plan = plan;
            return this;
        }

        public @This Spec sparql(CharSequence sparql) {
            this.sparql = sparql;
            return this;
        }

        public @This Spec threadStdOut(int cols) {
            threadDst = (threadDst&~DST_STDERR) | DST_STDOUT;
            threadCols = Math.max(threadCols, cols);
            return this;
        }
        public @This Spec threadStdErr(int cols) {
            threadDst = (threadDst&~DST_STDOUT) | DST_STDERR;
            threadCols = Math.max(threadCols, cols);
            return this;
        }
        public @This Spec thread(int cols) {
            threadCols = Math.max(threadCols, cols);
            return this;
        }

        public @This Spec streamNode(StreamNode streamNode) {
            this.streamNode = streamNode;
            return this;
        }

        public @This Spec resultsStdOut() {
            resultsDst = (resultsDst&DST_STDERR) | DST_STDOUT;
            return this;
        }
        public @This Spec resultsStdErr() {
            resultsDst = (resultsDst&DST_STDOUT) | DST_STDERR;
            return this;
        }

        public @This Spec nettyChannelDebuggerStdOut() {
            ncdDst = (ncdDst&DST_STDERR) | DST_STDOUT;
            return this;
        }
        public @This Spec nettyChannelDebuggerStdErr() {
            ncdDst = (ncdDst&DST_STDOUT) | DST_STDERR;
            return this;
        }

        public @This Spec emitterServiceStdOut() {
            emitterSvcDst = (emitterSvcDst&DST_STDERR) | DST_STDOUT;
            return this;
        }
        public @This Spec emitterServiceStdErr() {
            emitterSvcDst = (emitterSvcDst&DST_STDOUT) | DST_STDERR;
            return this;
        }

        private PrintStream std(int dst) {
            PrintStream orig;
            if      ((dst&DST_STDOUT) != 0) orig = System.out;
            else if ((dst&DST_STDERR) != 0) orig = System.err;
            else                            return null;
            return new PrintStream(new CloseShieldOutputStream(orig), true, UTF_8);
        }

        private @Nullable PrintStream out(int dst, String suffix) throws IOException {
            PrintStream std = std(dst);
            if ((dst&DST_FILE) == 0 || destDir == null) return std;

            if (!destDir.exists() && !destDir.mkdirs())
                throw new IOException("Could not mkdir "+destDir);
            if (destDir.isFile())
                throw new IOException("dir "+destDir+" exists as a file");
            var file = new FileOutputStream(new File(destDir, name+suffix));

            if (std == null)
                return new PrintStream(file, false, UTF_8);
            var tee = new TeeOutputStream();
            tee.add(std).add(file);
            return new PrintStream(tee);
        }

        @Override public void run() {
            log.info("Starting dump for watchdog {}...", name);
            try (var os = out(threadDst, ".journal")) {
                if (os != null)
                    ThreadJournal.dumpAndReset(os, threadCols == 0 ? 120 : threadCols);
            } catch (IOException e) {
                log.error("Failed to write journal: {}", e.getMessage());
            }
            try (var os = out(resultsDst, ".results")) {
                if (os != null)
                    ResultJournal.dump(os);
            } catch (IOException e) {
                log.error("Failed to write results journal: {}", e.getMessage());
            }
            if (ncdDumpAndReset != null) {
                try (var os = out(ncdDst, ".netty")) {
                    if (os != null)
                        ncdDumpAndReset.invokeExact(os);
                } catch (IOException e) {
                    log.error("Failed to write results journal: {}", e.getMessage());
                } catch (Throwable e) {
                    log.error("Failed to dump NettyChannelDebugger");
                }
            }
            if (destDir != null && streamNode != null) {
                File file = new File(destDir, name + ".svg");
                try {
                    streamNode.renderDOT(file, WITH_STATE_AND_STATS);
                } catch (IOException e) {
                    log.error("Failed to render dependency graph with dot for {} into {}: {}",
                              streamNode, file, e.getMessage());
                }
            }
            if (sparql != null && destDir != null) {
                File file = new File(destDir, name + ".sparql");
                try (var ps = new PrintStream(file, UTF_8)) {
                    ps.println(sparql);
                } catch (IOException e) {
                    log.error("Failed to write SPARQL to {}: {}", file, e.getMessage());
                }
            }
            if (plan != null && destDir != null) {
                File file = new File(destDir, name + ".plan");
                try (var ps = new PrintStream(file, UTF_8)) {
                    ps.println(plan);
                } catch (IOException e) {
                    log.error("Failed to write plan to {}: {}", file, e.getMessage());
                }
            }
            try (var ps = out(emitterSvcDst, ".emitterService")) {
                if (ps != null)
                    ps.println(EmitterService.EMITTER_SVC.dump());
            } catch (IOException e) {
                log.error("Failed to write EmitterService dump: {}", e.getMessage());
            }
            if (destDir == null)
                log.info("Dumped for watchdog {}", name);
            else
                log.info("Dumped into {} for watchdog {}", destDir, name);
            System.out.flush();
            System.err.flush();
        }

        public Watchdog create() { return new Watchdog(this); }

        public Watchdog start(long nanos) {
            Watchdog w = new Watchdog(this);
            w.start(nanos);
            return w;
        }

        public Watchdog   startMs(int   ms) { return start(MILLISECONDS.toNanos(ms)); }
        public Watchdog startSecs(int secs) { return start(SECONDS.toNanos(secs)); }
    }

    @SuppressWarnings("unused") // access through DEADLINE
    private long deadline;
    private volatile boolean triggered, shutdown;
    private final Semaphore semaphore = new Semaphore(0);
    private @MonotonicNonNull ConcurrentLinkedQueue<Runnable> lateActions = null;
    private final Runnable action;
    private final Thread thread;

    public Watchdog(Runnable action) {
        this.action = action;
        this.thread = Thread.startVirtualThread(() -> {
            semaphore.acquireUninterruptibly(); // wait until first start() call
            while (!shutdown) {
                long now = Timestamp.nanoTime(), deadline = (long)DEADLINE.getAcquire(this);
                if (now >= deadline) {
                    if (shutdown) { // re-check for racing close()
                        break;
                    } else if (!triggered) {
                        triggered = true; // only run action at most once per start() call
                        action.run();
                        if (lateActions != null) {
                            for (Runnable a : lateActions)
                                a.run();
                        }
                    }
                    if (!shutdown)
                        semaphore.acquireUninterruptibly(); //wake on start() or close()
                } else {
                    long delta = deadline - now;
                    try {
                        //noinspection ResultOfMethodCallIgnored
                        semaphore.tryAcquire(delta, NANOSECONDS); // wake at deadline/start()/close()
                    } catch (InterruptedException ignored) {}
                }
            }
        });
    }

    @SuppressWarnings("unused") public @This Watchdog andThen(Runnable action) {
        if (lateActions == null) lateActions = new ConcurrentLinkedQueue<>();
        lateActions.add(action);
        return this;
    }

    /**
     * Starts/resets the watchdog so that it will trigger and run the constructor-provided action
     * once {@code nanos} nanoseconds have elapsed since this call. The action will trigger at
     * most once per {@code start()} call (i.e., single-shot as opposed to periodic).
     *
     * <p>Future {@code start()} and {@link #stop()} calls cancel this call, i.e., the
     * watchdog will not trigger the action at the deadline implied by the first call if the
     * second call happens before the deadline is observed by the background thread.</p>
     *
     * @param nanos After how many nanoseconds (counting from this call) the action provided
     *              in the {@link Watchdog} constructor must run (once).
     * @return {@code this}
     * @throws IllegalStateException if {@link #close()} was previously called
     */
    public @This Watchdog start(long nanos) {
        if (shutdown) throw new IllegalStateException("close()ed");
        triggered = false;
        DEADLINE.setRelease(this, Timestamp.nanoTime()+nanos);
        semaphore.release();
        return this;
    }

    /**
     * Cancels a previous {@link #start(long)} call if it has not yet triggered.
     *
     * @return {@code this}
     */
    public @This Watchdog stop() {
        triggered = false;
        DEADLINE.setRelease(this, Long.MAX_VALUE);
        return this;
    }

    /**
     * Equivalent to {@link #stop()}, but if the watchdog was not previously
     * triggered by time, it will be intentionally triggered by this call.
     */
    public void stopAndTrigger() {
        DEADLINE.setRelease(this, Long.MAX_VALUE);
        if (!triggered)
            action.run();
        else
            triggered = false;
    }

    /**
     * Cancels the effect of any not-yet-triggered {@link #start(long)} and stops the
     * watchdog thread.
     *
     * <p>After this method is called, calling {@link #start(long)} is not allowed.</p>
     */
    @Override public void close() {
        shutdown = true;
        DEADLINE.setRelease(this, Long.MAX_VALUE);
        semaphore.release();
        try {
            if (!thread.join(Duration.ofSeconds(5))) {
                StringBuilder sb = new StringBuilder();
                for (StackTraceElement e : thread.getStackTrace())
                    sb.append("\n\tat ").append(e);
                log.warn("Watchdog thread blocked at{}", sb);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted, will not join thread", e);
        }
    }
}
