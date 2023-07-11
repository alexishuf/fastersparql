package com.github.alexishuf.fastersparql.emit.async;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.onSpinWait;

/**
 * Similar to a {@link java.util.concurrent.ExecutorService}, but executes {@link Task} objects
 * which:
 *
 * <ul>
 *     <li>Are non-blocking</li>
 *     <li>May reschedule themselves</li>
 *     <li>May be rescheduled from outside</li>
 *     <li>Have affinity to a specific worker thread</li>
 *     <li>Cooperate to avoid starvation (will reschedule themselves) to allow other tasks at
 *         the same worker thread to run)</li>
 * </ul>
 */
public class RecurringTaskRunner {
    private static final Logger log = LoggerFactory.getLogger(RecurringTaskRunner.class);
    private static final boolean IS_DEBUG = log.isDebugEnabled();

    public enum TaskResult {
        DONE,
        RESCHEDULE
    }

    /**
     * An arbitrary task ({@link #task()}) that can be repeatedly re-scheduled via {@link #awake()}.
     */
    public abstract static class Task {
        private static final VarHandle S;
        static {
            try {
                S = MethodHandles.lookup().findVarHandle(Task.class, "plainScheduled", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private int worker;
        private final RecurringTaskRunner runner;
        @SuppressWarnings("unused") private int plainScheduled;
        private boolean unloaded;

        public Task(RecurringTaskRunner runner) {
            this.runner = runner;
            this.worker = (int)MD.getAndAddAcquire(runner.runnerMd, RMD_WORKER, 1)
                        & runner.threadsMask;
            MD.getAndAddAcquire(runner.runnerMd, RMD_TASKS, 1);
        }

        /**
         * Notifies that this task will never be {@link #awake()}n again.
         */
        public final void unload() {
            if (unloaded) return;
            unloaded = true;
            MD.getAndAddAcquire(runner.runnerMd, RMD_TASKS, -1);
        }

        /**
         * Ensures that after this call, {@link #task()} will execute at least once. Such execution
         * may start and complete before the return of this call.
         */
        public final void awake() {
            if ((int)S.getAndAdd(this, 1) == 0)
                runner.add(this,  true);
        }

        /**
         * Arbitrary code that will be executed in a thread other than the one that called
         * {@link #awake()}. Implementations <strong>MUST NOT</strong> block.
         */
        protected abstract TaskResult task();

        protected final void run() {
            int old = (int) S.getVolatile(this);
            boolean resched = false;
            try {
                resched = task() == TaskResult.RESCHEDULE;
            } catch (Throwable t) {
                handleTaskException(t);
            }
            if (resched || (int)S.compareAndExchange(this, old, 0) != old) {
                S.setVolatile(this, 1);
                runner.add(this, false); // do not unpark() itself
            } // else: S == 0 and not enqueued, future awake() can enqueue
        }

        private void handleTaskException(Throwable t) {
            if (IS_DEBUG) log.error("Ignoring {} exception: ",  this, t);
            else          log.error("Ignoring {} thrown by {}", t,    this);
        }
    }

    private final class Worker extends Thread {
        private final int id;
        public Worker(ThreadGroup group, int i) {
            super(group, "RecurringTaskWorker-"+i);
            this.id = i;
            setDaemon(true);
            start();
        }

        @Override public void run() {
            var parent = RecurringTaskRunner.this;
            //noinspection InfiniteLoopStatement
            while (true) {
                Task task = pollOrSteal(id);
                if (task == null) {
                    LockSupport.park(parent);
                } else {
                    try {
                        task.run();
                    } catch (Throwable t) {
                        log.error("Task {} failed", task, t);
                    }
                }
            }
        }
    }

    private static final int MD_BITS       = Integer.numberOfTrailingZeros(128/4);
    private static final int MD_LOCK       = 0;
    private static final int MD_SIZE       = 1;
    private static final int MD_QUEUE_BASE = 2;
    private static final int MD_TAKE       = 3;
    private static final int MD_PUT        = 4;

    private static final int RMD_SHR_LOCK = 0;
    private static final int RMD_SHR_PRKD = 1;
    private static final int RMD_WORKER   = 2;
    private static final int RMD_TASKS    = 3;

    private static final int QUEUE_CAP = 128/4;
    private static final int QUEUE_IMBALANCE_CHECK = QUEUE_CAP>>3;
    private static final int QUEUE_MASK = QUEUE_CAP-1;

    static {
        assert Integer.bitCount(QUEUE_CAP) == 1;
        assert MD_PUT  < (1<<MD_BITS) && MD_TAKE < (1<<MD_BITS);
        assert (MD_LOCK|MD_SIZE|MD_QUEUE_BASE|MD_TAKE|MD_PUT) < (1<<MD_BITS);
    }

    private static final VarHandle MD = MethodHandles.arrayElementVarHandle(int[].class);
    private static final AtomicInteger nextServiceId = new AtomicInteger(1);
    public static final RecurringTaskRunner TASK_RUNNER
            = new RecurringTaskRunner(getRuntime().availableProcessors());

    private final Thread[] workers;
    private final int[] md;
    private final Task[] queues;
    private final ArrayDeque<Task> sharedQueue;
    private final Thread sharedScheduler;
    private final int threadsMask, threadMaskBits;
    private final int id;
    private final int[] runnerMd;

    public RecurringTaskRunner(int nThreads) {
        threadMaskBits = 32-Integer.numberOfLeadingZeros(nThreads-1);
        nThreads = 1<<threadMaskBits;
        threadsMask = nThreads-1;
        runnerMd = new int[4];
        md = new int[nThreads<<MD_BITS];
        queues = new Task[nThreads*QUEUE_CAP];
        for (int i = 0; i < nThreads; i++)
            md[(i<<MD_BITS) + MD_QUEUE_BASE] = i*QUEUE_CAP;
        workers = new Thread[nThreads];
        id = nextServiceId.getAndIncrement();
        var grp = new ThreadGroup("RecurringTaskRunner-"+id);
        for (int i = 0; i < nThreads; i++)
            workers[i] = new Worker(grp, i);
        sharedScheduler = Thread.ofPlatform().unstarted(this::sharedScheduler);
        sharedQueue = new ArrayDeque<>(nThreads*QUEUE_CAP);
        sharedScheduler.setName("RecurringTaskRunner-"+id+"-Scheduler");
        sharedScheduler.setDaemon(true);
        sharedScheduler.start();
    }

    @Override public String toString() {
        return "RecurringTaskRunner-"+id;
    }

    public String dump() {
        var sb = new StringBuilder().append("RecurringTaskRunner-").append(id).append('\n');
        sb.append("  shared queue: ").append(sharedQueue.size()).append(" items\n");
        sb.append(" loaded tasks: ").append(runnerMd[RMD_TASKS]).append('\n');
        for (int i = 0; i < workers.length; i++) {
            int mdb = i << MD_BITS;
            while ((int)MD.compareAndExchange(md, mdb+MD_LOCK, 0, 1) == 1) onSpinWait();
            try {
                int size = md[mdb + MD_SIZE];
                sb.append(" worker ")
                        .append(i).append(' ')
                        .append(workers[i].getState().name()).append(' ')
                        .append(size).append(" tasks queued: ");
                for (int j = 0; j < size; j++) {
                    var task = queues[md[mdb+MD_QUEUE_BASE] + ((md[mdb+MD_TAKE] + j)&QUEUE_MASK)];
                    sb.append(i == 0 ? "" : ", ").append(task);
                }
                sb.append('\n');
            } finally {
                MD.setRelease(md, mdb+MD_LOCK, 0);
            }
        }
        return sb.toString();
    }

    /**
     * Removes the first task from the queue whose metadata starts at {@code mdb}, assuming this
     * thread holds the spinlock for that queue.
     *
     * @param mdb {@code i << MD_BITS}  where i is the queue index in {@code [0, threadsMask]}
     * @return the first task of the queue, or {@code null} if the queue was empty.
     */
    private @Nullable Task localPollLocked(int mdb) {
        int size = md[mdb + MD_SIZE];
        if (size > 0) {
            md[mdb+MD_SIZE] = size-1;
            int takeIdx = md[mdb+MD_TAKE];
            md[mdb+MD_TAKE] = (takeIdx+1) & QUEUE_MASK;
            return queues[md[mdb+MD_QUEUE_BASE]+takeIdx];
        }
        return null;
    }

    /**
     * Gets and removes the first {@link Task} in any worker queue that is not {@code emptyQueue}.
     *
     * @param emptyQueue the worker queue from which stealing will not be attempted
     * @return A {@link Task} removed from the head of some queue or {@code null} if all quueues
     *         were empty or with their spinlocks locked.
     */
    private @Nullable Task localSteal(int emptyQueue) {
        for (int i = (emptyQueue+1)&threadsMask; i != emptyQueue; i = (i+1)&threadsMask) {
            int mdb = i<<MD_BITS;
            if ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) == 0) {
                Task task = localPollLocked(mdb);
                MD.setRelease(md, mdb+MD_LOCK, 0);
                if (task != null)
                    return task;
            }
        }
        return null;
    }

    /**
     * Gets the first task scheduled at the {@code queue}-th worker, or at the shared queue or
     * at any other worker queue, in this order.
     *
     * <p>For the {@code queue}-th worker queue and the shared queue there will be no spin limit
     * when acquiring their spinlock. Stealing from other worker queues will only be attempted if
     * this thread meets their spinlock unlocked.</p>
     *
     * @param queue the worker queue to be polled first
     * @return A {@link Task} removed from one of the queues or {@code null} if all queues were
     *         empty or with too much contention to allow stealing
     */
    private @Nullable Task pollOrSteal(int queue) {
        int mdb = queue << MD_BITS;
        while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
            onSpinWait();
        Task task = localPollLocked(mdb);
        MD.setRelease(md, mdb+MD_LOCK, 0);
        if (task == null) {
            if ((task = sharedPoll()) == null)
                task = localSteal(queue);
            else if (sharedQueue.size() > 0 && (int)MD.getOpaque(runnerMd, RMD_SHR_PRKD) == 1)
                LockSupport.unpark(sharedScheduler);
        }
        return task;
    }

    /**
     * Tries adding {@code task} to the end of the queue of the {@code task.worker}-th worker.
     *
     * @param task the non-null {@link Task}
     * @param canUnpark if true and adding to an empty queue, will
     *                  {@link LockSupport#unpark(Thread)} the worker thread. Should be
     *                  {@code true} unless the caller is the worker thread itself.
     * @return Whether {@code task} was added to the queue. Will return {@code false} if the
     *         queue was full or if there was too much contention on the spinlock.
     */
    private boolean tryAdd(Task task, boolean canUnpark) {
        int mdb = task.worker<<MD_BITS, lockIdx = mdb+MD_LOCK, size;
        boolean got = false;
        for (int i = 0; i < 16; i++) {
            got = (int) MD.compareAndExchangeAcquire(md, lockIdx, 0, 1) == 0;
            if (got || i == 7 && md[(((task.worker+1)&threadsMask) << MD_BITS) + MD_SIZE] == 0)
                break; // got spinlock or next worker has an empty queue
            onSpinWait();
        }
        if (!got)
            return false; // congested spinlock
        try {
            if ((size = md[mdb+MD_SIZE]) >= QUEUE_CAP)
                return false; // full queue
            md[mdb+MD_SIZE] = size+1;
            int putIdx;
            if (size == 0) {
                // pulling the queue down helps prevent false sharing: queue[0] is not
                // cache-aligned, and we can only rely on distance to prevent false sharing
                md[mdb+MD_PUT ] = 1;
                md[mdb+MD_TAKE] = putIdx = 0;
            } else {
                putIdx = md[mdb+MD_PUT];
                md[mdb+MD_PUT] = (putIdx+1) & QUEUE_MASK;
            }
            queues[md[mdb+MD_QUEUE_BASE]+putIdx] = task;
        } finally {
            MD.setRelease(md, lockIdx, 0);
        }
        if (size == 0 && canUnpark)
            LockSupport.unpark(workers[mdb>>MD_BITS]);
        return true;
    }

    /**
     * Tries adding {@code task} to the queue of the {@code task.worker}-th worker or to the shared
     * queue if the worker queue is full or under heavy contention.
     *
     * @param task the nonnull {@link Task} to add.
     * @param external whether this is called from anywhere other than {@link Task#run()}.
     *                 If {@code false} (calling from {@link Task#run()}, will not
     *                 {@link LockSupport#unpark(Thread)} the preferred worker since the
     *                 preferred worker thread is running this code
     */
    private void add(Task task, boolean external) {
        int size = md[(task.worker << MD_BITS) + MD_SIZE];
        int overloaded = size < QUEUE_IMBALANCE_CHECK ? QUEUE_IMBALANCE_CHECK
                                                      : 2+(runnerMd[RMD_TASKS]>>threadMaskBits);
        // break affinity if queue is >2 tasks above ideal average, full or under heavy contention
        if (size > overloaded || !tryAdd(task, external)) {
            // before unpark()ing sharedScheduler(), try the right-side worker which is the least
            // likely to steal from this queue
            boolean added = false;
            int nextWorker = (task.worker+1) & threadsMask;
            if (md[(nextWorker<<MD_BITS) + MD_SIZE] < overloaded) {
                task.worker = nextWorker;
                added = tryAdd(task, true);
            }
            if (!added)
                sharedAdd(task);
        }
    }

    private @Nullable Task sharedPoll() {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            return sharedQueue.poll();
        } finally { MD.setRelease(runnerMd, RMD_SHR_LOCK, 0); }
    }

    private void sharedAdd(Task task) {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            sharedQueue.add(task);
            if (sharedQueue.size() == 1)
                LockSupport.unpark(sharedScheduler);
        } finally {
            MD.setRelease(runnerMd, RMD_SHR_LOCK, 0);
        }
    }

    private void sharedPrepend(Task task) {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            sharedQueue.addFirst(task);
        } finally { MD.setRelease(runnerMd, RMD_SHR_LOCK, 0); }
    }

    private void sharedScheduler() {
        int w = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
            Task task = null;
            try {
                task = sharedPoll();
            } catch (Throwable t) {
                log.error("Exception from sharedPoll at sharedScheduler()", t);
            }
            try {
                if (task != null) {
                    boolean added = tryAdd(task, true);
                    int acceptable = 2 + ((int) MD.getOpaque(runnerMd, RMD_TASKS) >> threadMaskBits);
                    for (int e = w; !added && w <= e; w = (w + 1) & threadsMask) {
                        if ((int) MD.getOpaque(md, (w << MD_BITS) + MD_SIZE) > acceptable)
                            continue; // do not assign to queue above average load
                        task.worker = w;
                        added = tryAdd(task, true);
                    }
                    if (!added) {
                        sharedPrepend(task); // let workers steal it
                        task = null; // park
                    }
                }
                if (task == null) {
                    MD.setOpaque(runnerMd, RMD_SHR_PRKD, 1);
                    LockSupport.park(this);
                    MD.setOpaque(runnerMd, RMD_SHR_PRKD, 0);
                }
            } catch (Throwable t) {
                log.error("Ignoring exception on sharedScheduler", t);
                try { sharedAdd(task); } catch (Throwable ignored) {}
            }
        }
    }
}
