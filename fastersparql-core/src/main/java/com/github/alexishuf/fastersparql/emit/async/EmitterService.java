package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.MIN_PRIORITY;
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
public class EmitterService {
    private static final Logger log = LoggerFactory.getLogger(EmitterService.class);

    /**
     * An arbitrary task ({@link #task(int)}) that can be repeatedly re-scheduled via {@link #awake()}.
     */
    public abstract static class Task extends Stateful {
        private static final VarHandle SCHEDULED;
        static {
            try {
                SCHEDULED = MethodHandles.lookup().findVarHandle(Task.class, "plainScheduled", short.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        protected static final short RR_WORKER = Short.MIN_VALUE;
        private static final int IS_RUNNING  = 0x80000000;
        protected static final Stateful.Flags TASK_FLAGS = Stateful.Flags.DEFAULT.toBuilder()
                .flag(IS_RUNNING, "RUNNING").build();


        protected final EmitterService runner;
        protected short preferredWorker;
        @SuppressWarnings("unused") private short plainScheduled;

        /**
         * Create  a new {@link Task}, optionally  assigned to a preferred worker.
         *
         * @param svc the {@link EmitterService} where the task will run.
         * @param worker if {@code >= 0}, this will be treated as the 0-based index of the worker
         *               where this task will initially schedule itself on {@link #awake()}.
         *               If {@link #RR_WORKER}, a round-robin strategy will choose a worker.
         */
        protected Task(EmitterService svc, int worker, int initState, Flags flags) {
            super(initState, flags);
            assert flags.contains(TASK_FLAGS);
            this.runner = svc;
            int chosen;
            if (worker <  0)
                chosen = (int)MD.getAndAddRelease(svc.runnerMd, RMD_WORKER, 1);
            else
                chosen = worker;
            preferredWorker = (short)(chosen&svc.threadsMask);
            MD.getAndAddRelease(svc.runnerMd, RMD_TASKS, 1);
        }

        @Override protected void doRelease() {
            MD.getAndAddRelease(runner.runnerMd, RMD_TASKS, -1);
        }

        /**
         * Ensures that after this call, {@link #task(int)} will execute at least once. Such execution
         * may start and complete before the return of this call.
         */
        protected final void awake() {
            if ((short)SCHEDULED.getAndAddRelease(this, (short)1) == (short)0)
                runner.add(this);
        }

        /**
         * If this task and {@code task} share the same {@code preferredWorker}, change the
         * preferred worker of this task to the one that is least likely to steal from
         * (or to have tasks stolen by) the preferred worker of {@code task}.
         *
         * <p>If the task is already scheduled or being executed, the change enacted by this
         * method (if any) will only apply upon the next {@code awake}</p>
         *
         * @param task A Task for which this task should not be scheduled to a nearby worker
         */
        protected final void avoidWorker(Task task) {
            short other = task.preferredWorker;
            if (preferredWorker == other) {
                short mask = runner.threadsMask;
                preferredWorker = (short)( (other+1+(mask>>1)) & mask );
            }
        }


        /**
         * Arbitrary code that does whatever is the purpose of this task. Implementations
         * must not block and should {@link #awake()} and return instead of running loops. If
         * this runs in response to an {@link #awake()}, it will run in a worker thread of the
         * {@link EmitterService} set at construction. If running due to {@link #runNow()}, it
         * may be called from an external thread. Whatever the trigger, there will be no parallel
         * executions of this method for a single {@link Task} instance.
         *
         * @param threadId lower 32 bits of value of {@link Thread#threadId()} for
         *                 the {@link Thread#currentThread()}
         */
        protected abstract void task(int threadId);

        /**
         * Forbid execution of {@link #task(int)} after this method has returned and until
         * {@link #allowRun(short)} is called.
         *
         * <p>If {@link #task(int)} is currently being executed at some thread, this call will
         * block until it finishes executing. If the thread calling this method is a worker
         * thread, it will attempt executing another task instead of busy-waiting.</p>
         *
         * @return a counter value that must be passed to {@link #allowRun(short)}, so it can
         *         determine whether {@link #awake()} calls arrived while running was disallowed.
         */
        public short disallowRun() {
            short snapshot = (short)SCHEDULED.getAcquire(this);
            if (!compareAndSetFlagRelease(IS_RUNNING)) {
                beginSpin();
                for (int i=0; !compareAndSetFlagRelease(IS_RUNNING); ++i) {
                    if ((i&7) == 7) Thread.yield();
                    else            Thread.onSpinWait();
                    snapshot = (short) SCHEDULED.getAcquire(this);
                }
                endSpin();
            }
            return snapshot;
        }

        /**
         * Allows this {@link Task} to be scheduled upon {@link #awake()} after a call to
         * {@link #disallowRun()} made that impossible.
         *
         * <p>This call may itself trigger the effects of an {@link #awake()} if an
         * {@link #awake()}  arrived while running was disallowed.</p>
         *
         * @param scheduledBeforeDisallowRun the value returned by {@link #disallowRun()}
         * @return whether the task was enqueued for execution by this method call.
         */
        public boolean allowRun(short scheduledBeforeDisallowRun) {
            clearFlagsRelease(statePlain(), IS_RUNNING);
            short ac;
            ac = (short)SCHEDULED.compareAndExchange(this, scheduledBeforeDisallowRun, (short)0);
            if (ac != scheduledBeforeDisallowRun) {
                SCHEDULED.setRelease(this, (short)1);
                runner.add(this);
                return true;
            } // else: S = 0 and not enqueued, future awake() can enqueue
            return false;
        }

        /**
         * Execute this task <strong>now</strong>, unless it is already being executed by
         * another thread. Unlike a direct call to {@link #task(int)}, this will ensure there are
         * no concurrent {@link #task(int)} calls for the same {@link Task} object. If there is a
         * concurrent execution by another thread on {@link #run(int)} or {@code runNow()}, this
         * call will have no effect
         *
         * <p>This should be called to solve inversion-of-priority issues. A producer that finds
         * itself being blocked due to this task not running can call this method to have the
         * consumer task work instead of spinning or parking.</p>
         */
        @SuppressWarnings("unused") protected boolean runNow() {
            return run((int)Thread.currentThread().threadId());
        }

        @Async.Execute private boolean run(int threadId) {
            if (!compareAndSetFlagRelease(IS_RUNNING)) {
                // runNow() active on another thread. do runNow() will re-add() into
                // preferredWorker if an awake arrived since runNow() acquired IS_RUNNING, which
                // includes awake() calls from within task()
                return false;
            }
            short old = (short)SCHEDULED.getAcquire(this);
            try {
                task(threadId);
            } catch (Throwable t) {
                handleTaskException(t);
            } finally {
                clearFlagsRelease(statePlain(), IS_RUNNING);
            }
            if ((short)SCHEDULED.compareAndExchange(this, old, (short)0) != old) {
                SCHEDULED.setRelease(this, (short)1);
                runner.add(this);
            } // else: S = 0 and not enqueued, future awake() can enqueue
            return true; // task() was called
        }

        private void handleTaskException(Throwable t) {
            log.error("Ignoring {} thrown by {}:",  t.getClass().getSimpleName(), this, t);
        }
    }

//    private final class StarvedDetector extends Thread {
//        public StarvedDetector(ThreadGroup group) {
//            super(group, "EmitterService-StaleDetector");
//            setDaemon(true);
//            start();
//        }
//
//        @Override public void run() {
//            long lastDumpNs = Timestamp.nanoTime();
//            boolean[] starved = new boolean[workers.length];
//            //noinspection InfiniteLoopStatement
//            while (true) {
//                for (int i = 0; i < workers.length; i++) {
//                    int mdb = i << MD_BITS;
//                    while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
//                        Thread.onSpinWait();
//                    int n, msg = 0;
//                    try {
//                        if ((n=md[mdb+MD_SIZE]) > 0 && md[mdb+MD_PARKED] != 0) {
//                            if (!starved[i]) {
//                                starved[i] = true;
//                                msg = 1;
//                            }
//                        } else if (starved[i]) {
//                            starved[i] = false;
//                            msg = 2;
//                        }
//                    } finally {
//                        MD.setRelease(md, mdb+MD_LOCK, 0);
//                    }
//                    if (msg == 1)
//                        log.error("Worker {} has {} tasks starved", i, n);
//                    else if (msg == 2)
//                        log.error("Worker {} un-starved", i);
//                }
//                if (Timestamp.nanoTime()-lastDumpNs > 5_000_000_000L) {
//                    lastDumpNs = Timestamp.nanoTime();
//                    var sb = new StringBuilder();
//                    for (int i = 0; i < workers.length; i++) {
//                        int mdb = i<<MD_BITS;
//                        while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
//                            onSpinWait();
//                        try {
//                            sb.append(String.format("\nworker %02d %3d tasks, parked=%b",
//                                                    i, md[mdb+MD_SIZE], md[mdb+MD_PARKED]!=0));
//                        } finally {
//                            MD.setRelease(md, mdb+MD_LOCK, 0);
//                        }
//                    }
//                    log.error("Worker queues: {}", sb);
//                }
//                LockSupport.parkNanos(1_000_000_000L);
//            }
//        }
//    }

    private final class Worker extends Thread {
        private final short id;
        private final short threadId;
        private final EmitterService parent;

        public Worker(ThreadGroup group, short i) {
            super(group, "EmitterService-"+EmitterService.this.id+"-"+i);
            this.id = i;
            this.parent = EmitterService.this;
            setDaemon(true);
            threadId = (short)threadId();
            start();
        }

        @Override public void run() {
            var md = parent.md;
            int id = this.id, threadId = this.threadId;
            int mdParkedIdx = (id << MD_BITS) + MD_PARKED;
            //noinspection InfiniteLoopStatement
            while (true) {
                Task task = parent.pollOrSteal(id);
                if (task == null) {
                    if (id < 32) {
                        parkedBitset |= 1<<id;
                        if (stealer == id) stealer = -1;
                    }
                    LockSupport.park(parent);
                    md[mdParkedIdx] = 0;
                    if (id < 32) parkedBitset &= ~(1<<id);
                } else {
                    try {
                        task.run(threadId);
                    } catch (Throwable t) {
                        log.error("Dispatch failed for {}", task, t);
                    }
                }
            }
        }

//        private static final AtomicInteger YIELD_RAN           = new AtomicInteger();
//        private static final AtomicInteger YIELD_FAIL_RUNNING  = new AtomicInteger();
//        private static final AtomicInteger YIELD_FAIL_EMPTY    = new AtomicInteger();
//        private static final AtomicInteger YIELD_FAIL_YIELDING = new AtomicInteger();
//        static {
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                int ran      = YIELD_RAN          .getOpaque();
//                int running  = YIELD_FAIL_RUNNING .getOpaque();
//                int empty    = YIELD_FAIL_EMPTY   .getOpaque();
//                int yielding = YIELD_FAIL_YIELDING.getOpaque();
//                double calls = ran+running+empty+yielding;
//                System.err.printf("""
//                        Yield success:               %,9d (%5.2f%%)
//                        Yield fail:    task running: %,9d (%5.2f%%)
//                        Yield fail:     empty queue: %,9d (%5.2f%%)
//                        Yield fail: worker yielding: %,9d (%5.2f%%)
//                        """,
//                        ran,      100*(ran     /calls),
//                        running,  100*(running /calls),
//                        empty,    100*(empty   /calls),
//                        yielding, 100*(yielding/calls)
//                );
//            }));
//        }

        public void beginSpin() {
            short mask = threadsMask;
            short id = this.id, i = id, mdb = (short)(id<<MD_BITS);
            int[] md = EmitterService.this.md;
            MD.setRelease(md, mdb+MD_SPINNING, 1);
            if (md[mdb+MD_SIZE] <= 0)
                return; // no tasks to be stolen
            // scan for a non-spinning non-parked worker that has no scheduled tasks
            while ((i=(short)((i+1)&mask)) != id) {
                mdb = (short)(i<<MD_BITS);
                if (md[mdb+MD_SIZE] == 0 && (int)MD.getAcquire(md, mdb+MD_SPINNING) == 0
                                         && md[mdb+MD_SPINNING] == 0) {
                    return; // found a stealer, no need to unpark()
                }
            }
            // unpark at most one worker, staring from most likely stealer
            while ((i=(short)((i-1)&mask)) != id) {
                if (md[(i<<MD_BITS)+MD_PARKED] == 1) {
                    Unparker.unpark(workers[i]);
                    break;
                }
            }
        }

        public void endSpin() { MD.setRelease(md, (id<<MD_BITS)+MD_SPINNING, 0); }

//        public boolean yieldToTaskOnce() {
//            if (yielding)
//                return false;
//            int mdb = id << MD_BITS, size, queueIdx;
//            boolean locked = false;
//            short os = -1;
//            try {
//                yielding = true;
//                while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
//                    onSpinWait(); // spin until locked worker queue
//                locked = true;
//                if ((size = md[mdb+MD_SIZE]) > 0) {
//                    queueIdx = md[mdb+MD_TAKE];
//                    Task task = queues[md[mdb+MD_QUEUE_BASE]+queueIdx];
//                    if (task.compareAndSetFlagRelease(Task.IS_RUNNING)) { // blocked task.runNow()
//                        try {
//                            md[mdb+MD_SIZE] = size - 1;                 // remove from queue
//                            md[mdb+MD_TAKE] = (queueIdx+1)&QUEUE_MASK;  // bump take index
//                            MD.setRelease(md, mdb+MD_LOCK, 0);          // release worker queue
//                            locked = false;                             // do not release
//                            os = (short)Task.SCHEDULED.getOpaque(task); // pending awake()s
//                            task.task(threadId);
//                        } catch (Throwable t) {
//                            task.handleTaskException(t);
//                        } finally {
//                            task.clearFlagsRelease(task.statePlain(), Task.IS_RUNNING);
//                        }
//                        if ((short)Task.SCHEDULED.compareAndExchange(task, os, (short)0) != os) {
//                            // got awake() calls during task.task(), enqueue task again
//                            Task.SCHEDULED.setRelease(task, (short)1);
//                            add(task);
//                        }
//                    }
//                }
//            } finally {
//                yielding = false;
//                if (locked) // release worker queue if not already released
//                    MD.setRelease(md, mdb+MD_LOCK, 0);
//            }
//            return os != -1;
//        }
//
//        private void awakeStealer() {
//            if (md[(id<<MD_BITS)+MD_SIZE] == 0)
//                return; // no scheduled tasks to be stolen
//            int stealerId = (id-1)&threadsMask;
//            if (md[(stealerId<<MD_BITS)+MD_PARKED] == 1)
//                Unparker.unpark(workers[stealerId]);
//        }
    }

    private static final int MD_BITS       = numberOfTrailingZeros(128/4);
    private static final int MD_LOCK       = 0;
    private static final int MD_SIZE       = 1;
    private static final int MD_QUEUE_BASE = 2;
    private static final int MD_TAKE       = 3;
    private static final int MD_PUT        = 4;
    private static final int MD_PARKED     = 5;
    private static final int MD_SPINNING   = 6;
    static { assert MD_SPINNING < (1<<MD_BITS); }

    private static final int RMD_SHR_LOCK   = 0;
    private static final int RMD_SHR_PARKED = 1;
    private static final int RMD_WORKER     = 2;
    private static final int RMD_TASKS      = 3;

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
    public static final EmitterService EMITTER_SVC
            = new EmitterService(getRuntime().availableProcessors());

    private final int[] md;
    private final short threadsMask, threadMaskBits;
    private int parkedBitset;
    private int stealer;
    private final int[] runnerMd;
    private final Worker[] workers;
    private final ArrayDeque<Task> sharedQueue;
    private final Task[] queues;
    private final int id;
    private final Thread sharedScheduler;

    public EmitterService(int nWorkers) {
        threadMaskBits = (short) Math.min(15, 32-Integer.numberOfLeadingZeros(nWorkers-1));
        nWorkers = 1<<threadMaskBits;
        assert nWorkers <= 0xffff;
        threadsMask = (short)(nWorkers-1);
        runnerMd = new int[4];
        md = new int[nWorkers<<MD_BITS];
        queues = new Task[nWorkers*QUEUE_CAP];
        for (int i = 0; i < nWorkers; i++)
            md[(i<<MD_BITS) + MD_QUEUE_BASE] = i*QUEUE_CAP;
        workers = new Worker[nWorkers];
        id = nextServiceId.getAndIncrement();
        var grp = new ThreadGroup("EmitterService-"+id);
        for (short i = 0; i < nWorkers; i++)
            workers[i] = new Worker(grp, i);
        sharedScheduler = Thread.ofPlatform().unstarted(this::sharedScheduler);
        sharedQueue = new ArrayDeque<>(nWorkers*QUEUE_CAP);
        sharedScheduler.setName("EmitterService-"+id+"-Scheduler");
        sharedScheduler.setPriority(MIN_PRIORITY);
        sharedScheduler.setDaemon(true);
        sharedScheduler.start();
//        new StarvedDetector(grp);
    }

    @Override public String toString() {
        return "EmitterService-"+id;
    }

    @SuppressWarnings("unused") public String dump() {
        var sb = new StringBuilder().append("EmitterService-").append(id).append('\n');
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

//    /**
//     * Tries to execute a previously scheduled task in this {@link EmitterService}.
//     *
//     * <p>If this method is called from a worker thread, tasks scheduled in the worker will
//     * be preferred. If the calling thread is not a worker thread, a task will be stolen
//     * from an arbitrary worker chosen according to the calling Thread (i.e., the same
//     * external thread will always try stealing from the same worker thread).</p>
//     *
//     * @return Whether a task was executed. {@code false} will be returned if there is no
//     * scheduled task in the worker or if the selected task is already being executed
//     * (i.e., {@link Task#awake()} called from within {@link Task#task(int)}),
//     */
//    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
//    public boolean yieldToTaskOnce() {
//        if (Thread.currentThread() instanceof Worker w)
//            return w.yieldToTaskOnce();
//        return false;
//    }
//
//    /**
//     * If the current thread is a worker thread with a non-empty queue (beyond the
//     * currently executing task) and the worker that would try stealing first from
//     * this worker is parked, then {@link LockSupport#unpark(Thread)} said stealing
//     * worker.
//     *
//     * <p>This method should be called in scenarios where there is a chance of the
//     * current worker doing some potentially expensive computation or being blocked
//     * for a few microseconds (tasks should not block)</p>
//     */
//    public static void awakeStealer() {
//        if (Thread.currentThread() instanceof Worker w)
//            w.awakeStealer();
//    }

    /**
     * If {@link Thread#currentThread()} is a worker thread, mark it as {@code spinning}. If,
     * additionally, there is at least one scheduled task beyond the one that called
     * this method, ensure that there is a non-spinning worker thread in the running state if
     * no that can tasks scheduled on this worker.
     *
     * <p>If this method is called, {@link #endSpin()} <strong>MUST</strong> be called once
     * the spinning finishes.</p>
     */
    public static void beginSpin() {
        if (Thread.currentThread() instanceof Worker w)
            w.beginSpin();
    }

    /**
     * If the current thread is a worker thread, removes the {@code spinning} flag set by
     * {@link #beginSpin()}.
     */
    public static void endSpin() {
        if (Thread.currentThread() instanceof Worker w)
            w.endSpin();
    }

    public void requireStealer() {
        if (stealer == -1 && parkedBitset != 0) {
            int w = findStealer();
            if (w >= 0)
                Unparker.unpark(workers[w]);
        }
    }

    @SuppressWarnings("unused") public static short currentWorker() {
        if (Thread.currentThread() instanceof Worker w)
            return w.id;
        return Task.RR_WORKER;
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
            int b = i<<MD_BITS;
            if (md[b+MD_SIZE] > 0 && (int)MD.compareAndExchangeAcquire(md, b+MD_LOCK, 0, 1) == 0) {
                Task task = localPollLocked(b);
                MD.setRelease(md, b+MD_LOCK, 0);
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
     * empty or with too much contention to allow stealing
     */
    private @Nullable Task pollOrSteal(int queue) {
        int mdb = queue << MD_BITS;
        while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
            onSpinWait();
        Task task = localPollLocked(mdb);
        MD.setRelease(md, mdb+MD_LOCK, 0);
        if (task == null) {
            if (!sharedQueue.isEmpty())
                task = sharedSteal();
            if (task == null)
                task = localSteal(queue);
            if (task == null) {
                while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
                    onSpinWait();
                task = localPollLocked(mdb);
                if (task == null)
                    md[mdb+MD_PARKED] = 1;
                MD.setRelease(md, mdb+MD_LOCK, 0);
            }
        }
        return task;
    }

    /**
     * Tries adding {@code task} to the end of the queue of the {@code task.worker}-th worker.
     *
     * @param task the non-null {@link Task}
     * @return Whether {@code task} was added to the queue. Will return {@code false} if the
     *         queue was full or if there was too much contention on the spinlock.
     */
    private boolean tryAdd(Task task) {
        int mdb = task.preferredWorker <<MD_BITS, lockIdx = mdb+MD_LOCK, size;
        boolean got = false;
        byte unpark = -1;
        for (int i = 0; i < 16; i++) {
            got = (int) MD.compareAndExchangeAcquire(md, lockIdx, 0, 1) == 0;
            if (got || i == 7 && md[(((task.preferredWorker +1)&threadsMask) << MD_BITS) + MD_SIZE] == 0)
                break; // got spinlock or next worker has an empty queue
            onSpinWait();
        }
        if (!got)
            return false; // congested spinlock
        try {
            size = md[mdb+MD_SIZE];
            if (size >= QUEUE_CAP)
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
            if (md[mdb+MD_PARKED] == 1) {
                unpark = (byte)(mdb>>>MD_BITS);
                // since this thread will unpark(), subsequent tryAdd() calls will not need to
                // (unless the queue gets drained by the worker thread or a neighbour).
                md[mdb+MD_PARKED] = 0;
            }
        } finally {
            MD.setRelease(md, lockIdx, 0);
        }
        if (unpark >= 0) {
            if (size == 0 && stealer == unpark)
                stealer = -1;
            Unparker.unpark(workers[unpark]); // unpark target worker or stealer
        }
        return true;
    }

    private int findStealer() {
        int w = numberOfTrailingZeros(parkedBitset);
        if (w < 32 && stealer == -1) {
            stealer = w;
            md[(w<<MD_BITS) + MD_PARKED] = 0;
            return w;
        }
        return -1;
    }


    /**
     * Tries adding {@code task} to the queue of the {@code task.worker}-th worker or to the shared
     * queue if the worker queue is full or under heavy contention.
     *
     * @param task the nonnull {@link Task} to add.
     */
    private void add(@Async.Schedule Task task) {
        int size = md[(task.preferredWorker << MD_BITS) + MD_SIZE];
        int overloaded = size < QUEUE_IMBALANCE_CHECK ? QUEUE_IMBALANCE_CHECK
                                                      : 2+(runnerMd[RMD_TASKS]>>threadMaskBits);
        // break affinity if queue is >2 tasks above ideal average, full or under heavy contention
        if (size > overloaded || !tryAdd(task)) {
            // before unpark()ing sharedScheduler(), try the right-side worker which is the least
            // likely to steal from this queue
            boolean added = false;
            int nextWorker = (task.preferredWorker+1) & threadsMask;
            if (md[(nextWorker<<MD_BITS) + MD_SIZE] < overloaded) {
                task.preferredWorker = (short)nextWorker;
                added = tryAdd(task);
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

    private @Nullable Task sharedSteal() {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            var task = sharedQueue.poll();
            if (!sharedQueue.isEmpty() && runnerMd[RMD_SHR_PARKED] != 0)
                Unparker.unpark(sharedScheduler);
            return task;
        } finally { MD.setRelease(runnerMd, RMD_SHR_LOCK, 0); }
    }

    private void sharedAdd(Task task) {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            boolean unpark = sharedQueue.isEmpty();
            sharedQueue.add(task);
            if (unpark)
                Unparker.unpark(sharedScheduler);
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
                    boolean added = tryAdd(task);
                    int acceptable = 2 + ((int) MD.getOpaque(runnerMd, RMD_TASKS) >> threadMaskBits);
                    for (int e = w; !added && w <= e; w = (w + 1) & threadsMask) {
                        if ((int) MD.getOpaque(md, (w << MD_BITS) + MD_SIZE) > acceptable)
                            continue; // do not assign to queue above average load
                        task.preferredWorker = (short)w;
                        added = tryAdd(task);
                    }
                    if (!added) {
                        sharedPrepend(task); // let workers steal it
                        task = null; // park
                    }
                }
                if (task == null) {
                    MD.setOpaque(runnerMd, RMD_SHR_PARKED, 1);
                    LockSupport.park(this);
                    MD.setOpaque(runnerMd, RMD_SHR_PARKED, 0);
                }
            } catch (Throwable t) {
                log.error("Ignoring exception on sharedScheduler", t);
                try { sharedAdd(task); } catch (Throwable ignored) {}
            }
        }
    }
}
