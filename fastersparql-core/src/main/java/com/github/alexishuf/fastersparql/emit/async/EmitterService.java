package com.github.alexishuf.fastersparql.emit.async;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.Async;
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
public class EmitterService {
    private static final Logger log = LoggerFactory.getLogger(EmitterService.class);

    /**
     * An arbitrary task ({@link #task()}) that can be repeatedly re-scheduled via {@link #awake()}.
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
        protected static final int RR_WORKER = -1;
        private static final int IS_RUNNING  = 0x40000000;
        protected static final Stateful.Flags TASK_FLAGS = Stateful.Flags.DEFAULT.toBuilder()
                .flag(IS_RUNNING, "RUNNING").build();


        protected final EmitterService runner;
        protected short preferredWorker;
        @SuppressWarnings("unused") private short plainScheduled;

        /**
         * Create  a new {@link Task}, optionally  assigned to a preferred worker.
         *
         * @param runner the {@link EmitterService} where the task will run.
         * @param worker if {@code >= 0}, this will be treated as the 0-based index of the worker
         *               where this task will initially schedule itself on {@link #awake()}. If
         *               {@code < 0}, a round-robin strategy will select a preferred worker.
         */
        protected Task(EmitterService runner, int worker, int initState, Flags flags) {
            super(initState, flags);
            assert flags.contains(TASK_FLAGS);
            this.runner = runner;
            int unbound = worker >= 0
                        ? worker : (int) MD.getAndAddRelease(runner.runnerMd, RMD_WORKER, 1);
            preferredWorker = (short)(unbound&runner.threadsMask);
            MD.getAndAddRelease(runner.runnerMd, RMD_TASKS, 1);
        }

        @Override protected void doRelease() {
            MD.getAndAddRelease(runner.runnerMd, RMD_TASKS, -1);
        }

        /**
         * Ensures that after this call, {@link #task()} will execute at least once. Such execution
         * may start and complete before the return of this call.
         */
        protected final void awake() {
            if ((short)SCHEDULED.getAndAddRelease(this, (short)1) == (short)0)
                runner.add(this,  true);
        }

        /**
         * Arbitrary code that does whatever is the purpose of this task. Implementations
         * must not block and should {@link #awake()} and return instead of running loops. If
         * this runs in response to an {@link #awake()}, it will run in a worker thread of the
         * {@link EmitterService} set at construction. If running due to {@link #runNow()}, it
         * may be called from an external thread. Whatever the trigger, there will be no parallel
         * executions of this method for a single {@link Task} instance.
         */
        protected abstract void task();

        /**
         * Execute this task <strong>now</strong>, unless it is already being executed by
         * another thread. Unlike a direct call to {@link #task()}, this will ensure there are
         * no concurrent {@link #task()} calls for the same {@link Task} object. If there is a
         * concurrent execution by another thread on {@link #run()} or {@code runNow()}, this
         * call will have no effect
         *
         * <p>This should be called to solve inversion-of-priority issues. A producer that finds
         * itself being blocked due to this task not running can call this method to have the
         * consumer task work instead of spinning or parking.</p>
         */
        protected boolean runNow() {
            if (!compareAndSetFlagRelease(IS_RUNNING))
                return false; // running elsewhere
            try {
                task();
                return true;
            } catch (Throwable t) {
                handleTaskException(t);
            } finally {
                clearFlagsRelease(statePlain(), IS_RUNNING);
            }
            return false;
        }

        @Async.Execute private void run() {
            if (!compareAndSetFlagRelease(IS_RUNNING)) {
                // runNow() active on another thread, return to queue
                runner.add(this, false);
                return;
            }
            short old = (short)SCHEDULED.getAcquire(this);
            try {
                task();
            } catch (Throwable t) {
                handleTaskException(t);
            } finally {
                clearFlagsRelease(statePlain(), IS_RUNNING);
            }
            if ((short)SCHEDULED.compareAndExchange(this, old, (short)0) != old) {
                SCHEDULED.setRelease(this, (short)1);
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
        private boolean yielding;

        public Worker(ThreadGroup group, int i) {
            super(group, "EmitterService-"+EmitterService.this.id+"-"+i);
            this.id = i;
            setDaemon(true);
            start();
        }

        @Override public void run() {
            int mdParkedIdx = (id << MD_BITS) + MD_PARKED;
            var parent = EmitterService.this;
            //noinspection InfiniteLoopStatement
            while (true) {
                Task task = pollOrSteal(id);
                if (task == null) {
                    LockSupport.park(parent);
                    md[mdParkedIdx] = 0;
                } else {
                    try {
                        task.run();
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

        public boolean yieldToTaskOnce() {
            if (yielding)
                return false;
            int mdb = id << MD_BITS, size, queueIdx;
            boolean ran = false, locked = false;
            short os = 0;
            try {
                yielding = true;
                while ((int)MD.compareAndExchangeAcquire(md, mdb+MD_LOCK, 0, 1) != 0)
                    onSpinWait(); // spin until locked worker queue
                locked = true;
                if ((size = md[mdb+MD_SIZE]) > 0) {
                    queueIdx = md[mdb+MD_TAKE];
                    Task task = queues[md[mdb+MD_QUEUE_BASE]+queueIdx];
                    if (task.compareAndSetFlagRelease(Task.IS_RUNNING)) { // blocked task.runNow()
                        try {
                            md[mdb+MD_SIZE] = size-1;                   // remove task from queue
                            md[mdb+MD_TAKE] = (queueIdx+1)&QUEUE_MASK;  // bump take index
                            MD.setRelease(md, mdb+MD_LOCK, 0);          // release worker queue
                            locked = false;                             // do not release
                            os = (short)Task.SCHEDULED.getOpaque(task); // pending awake()s
                            task.task();
                            ran = true;
                        } catch (Throwable t) {
                            task.handleTaskException(t);
                        } finally {
                            task.clearFlagsRelease(task.statePlain(), Task.IS_RUNNING);
                        }
                        if ((short)Task.SCHEDULED.compareAndExchange(task, os, (short)0) != os) {
                            // got awake() calls during task.task(), enqueue task again
                            Task.SCHEDULED.setRelease(task, (short)1);
                            add(task, task.preferredWorker == id);
                        }
                    }
                }
            } finally {
                yielding = false;
                if (locked) // release worker queue if not already released
                    MD.setRelease(md, mdb+MD_LOCK, 0);
            }
            return ran;
        }
    }

    private static final int MD_BITS       = Integer.numberOfTrailingZeros(128/4);
    private static final int MD_LOCK       = 0;
    private static final int MD_SIZE       = 1;
    private static final int MD_QUEUE_BASE = 2;
    private static final int MD_TAKE       = 3;
    private static final int MD_PUT        = 4;
    private static final int MD_PARKED     = 5;
    static { assert MD_PARKED < (1<<MD_BITS); }

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
    private final int[] runnerMd;
    private final Worker[] workers;
    private final ArrayDeque<Task> sharedQueue;
    private final Task[] queues;
    private final int id;
    private final Thread sharedScheduler;

    public EmitterService(int nWorkers) {
        threadMaskBits = (short) Math.min(15, 32-Integer.numberOfLeadingZeros(nWorkers-1));
        nWorkers = 1<<threadMaskBits;
        threadsMask = (short)(nWorkers-1);
        runnerMd = new int[4];
        md = new int[nWorkers<<MD_BITS];
        queues = new Task[nWorkers*QUEUE_CAP];
        for (int i = 0; i < nWorkers; i++)
            md[(i<<MD_BITS) + MD_QUEUE_BASE] = i*QUEUE_CAP;
        workers = new Worker[nWorkers];
        id = nextServiceId.getAndIncrement();
        var grp = new ThreadGroup("EmitterService-"+id);
        for (int i = 0; i < nWorkers; i++)
            workers[i] = new Worker(grp, i);
        sharedScheduler = Thread.ofPlatform().unstarted(this::sharedScheduler);
        sharedQueue = new ArrayDeque<>(nWorkers*QUEUE_CAP);
        sharedScheduler.setName("EmitterService-"+id+"-Scheduler");
        sharedScheduler.setDaemon(true);
        sharedScheduler.start();
    }

    @Override public String toString() {
        return "RecurringTaskRunner-"+id;
    }

    @SuppressWarnings("unused") public String dump() {
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
     * Tries to execute a previously scheduled task in this {@link EmitterService}.
     *
     * <p>If this method is called from a worker thread, tasks scheduled in the worker will
     * be preferred. If the calling thread is not a worker thread, a task will be stolen
     * from an arbitrary worker chosen according to the calling Thread (i.e., the same
     * external thread will always try stealing from the same worker thread).</p>
     *
     * @return Whether a task was executed. {@code false} will be returned if there is no
     * scheduled task in the worker or if the selected task is already being executed
     * (i.e., {@link Task#awake()} called from within {@link Task#task()}),
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean yieldToTaskOnce() {
        if (Thread.currentThread() instanceof Worker w)
            return w.yieldToTaskOnce();
        return false;
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
     * @param canUnpark if true and adding to an empty queue, will
     *                  {@link LockSupport#unpark(Thread)} the worker thread. Should be
     *                  {@code true} unless the caller is the worker thread itself.
     * @return Whether {@code task} was added to the queue. Will return {@code false} if the
     *         queue was full or if there was too much contention on the spinlock.
     */
    private boolean tryAdd(Task task, boolean canUnpark) {
        int mdb = task.preferredWorker <<MD_BITS, lockIdx = mdb+MD_LOCK;
        boolean got = false, unpark;
        for (int i = 0; i < 16; i++) {
            got = (int) MD.compareAndExchangeAcquire(md, lockIdx, 0, 1) == 0;
            if (got || i == 7 && md[(((task.preferredWorker +1)&threadsMask) << MD_BITS) + MD_SIZE] == 0)
                break; // got spinlock or next worker has an empty queue
            onSpinWait();
        }
        if (!got)
            return false; // congested spinlock
        try {
            int size = md[mdb+MD_SIZE];
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
            unpark = canUnpark && md[mdb+MD_PARKED] == 1;
        } finally {
            MD.setRelease(md, lockIdx, 0);
        }
        if (unpark)
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
    private void add(@Async.Schedule Task task, boolean external) {
        int size = md[(task.preferredWorker << MD_BITS) + MD_SIZE];
        int overloaded = size < QUEUE_IMBALANCE_CHECK ? QUEUE_IMBALANCE_CHECK
                                                      : 2+(runnerMd[RMD_TASKS]>>threadMaskBits);
        // break affinity if queue is >2 tasks above ideal average, full or under heavy contention
        if (size > overloaded || !tryAdd(task, external)) {
            // before unpark()ing sharedScheduler(), try the right-side worker which is the least
            // likely to steal from this queue
            boolean added = false;
            int nextWorker = (task.preferredWorker +1) & threadsMask;
            if (md[(nextWorker<<MD_BITS) + MD_SIZE] < overloaded) {
                task.preferredWorker = (short)nextWorker;
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

    private @Nullable Task sharedSteal() {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            var task = sharedQueue.poll();
            if (!sharedQueue.isEmpty() && runnerMd[RMD_SHR_PARKED] != 0)
                LockSupport.unpark(sharedScheduler);
            return task;
        } finally { MD.setRelease(runnerMd, RMD_SHR_LOCK, 0); }
    }

    private void sharedAdd(Task task) {
        while ((int)MD.compareAndExchangeAcquire(runnerMd, RMD_SHR_LOCK, 0, 1) != 0) onSpinWait();
        try {
            boolean unpark = sharedQueue.isEmpty();
            sharedQueue.add(task);
            if (unpark)
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
                        task.preferredWorker = (short)w;
                        added = tryAdd(task, true);
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
