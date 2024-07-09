package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import net.openhft.affinity.Affinity;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Thread.currentThread;

/**
 * Similar to a {@link ExecutorService}, but executes {@link Task} objects
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
public final class EmitterService {
    static {
        ThreadPoolsPartitioner.registerPartition(EmitterService.class.getSimpleName());
    }
    private static final Logger log = LoggerFactory.getLogger(EmitterService.class);

    /**
     * An arbitrary task ({@link #task(Worker, int)}) that can be repeatedly re-scheduled via {@link #awakeSameWorker()}.
     */
    public abstract static class Task<T extends Task<T>> extends Stateful<T> {
        private static final VarHandle SCHEDULED;
        static {
            try {
                SCHEDULED = MethodHandles.lookup().findVarHandle(Task.class, "plainScheduled", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        protected static final int IS_RUNNING  = 0x80000000;
        protected static final Flags TASK_FLAGS = Flags.DEFAULT.toBuilder()
                .flag(IS_RUNNING, "RUNNING").build();


        protected final EmitterService emitterSvc;
        @SuppressWarnings("unused") private int plainScheduled;

        /**
         * Create  a new {@link Task}, optionally  assigned to a preferred worker.
         *
         * @param initState see {@link Stateful#Stateful(int, Flags)}
         * @param flags see {@link Stateful#Stateful(int, Flags)}
         */
        protected Task(int initState, Flags flags) {
            super(initState, flags);
            assert flags.contains(TASK_FLAGS);
            this.emitterSvc = service();
        }

        /**
         * Ensures that after this call, {@link #task(Worker, int)} will execute at least once.
         *
         * <p>{@link #task(Worker, int)} will not be called from within this call, But it may
         * be called in parallel from another worker thread before this call returns.</p>
         *
         * <p>This method will prefer enqueuing {@code this} into a worker-private queue if
         * the calling thread is a worker thread. Such queue is not shared with other worker
         * and thus does not suffer from contention. Tasks only move out of the worker-private
         * queue if the worker thread evicts them due to imbalance or due to a
         * {@link #yieldWorker()} call.</p>
         */
        protected final void awakeSameWorker() {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) != 0)
                return; // already queued
            Thread thread = currentThread();
            int workerId = thread instanceof Worker w ? w.workerId : (int)thread.threadId();
            emitterSvc.queue.put(this, workerId);
        }

        /**
         * Equivalent to {@link #awakeSameWorker()} but assumes {@link Thread#currentThread()}
         * is {@code currentWorker}. This should be used when a {@link #task(Worker, int)}
         * wishes to continue processing later and is returning now only to be polite and
         * allow other tasks to execute.
         *
         * <p><strong>Attention:</strong>{@code currentWorker} MUST BE
         * {@link Thread#currentThread()}. If not, {@code this} or other tasks might be silently
         * dropped from the queue and will thus starve for all eternity.</p>
         */
        protected final void awakeSameWorker(Worker currentWorker) {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) == 0)
                emitterSvc.queue.put(this, currentWorker.workerId);
        }

        /**
         * Ensures that after this call, {@link #task(Worker, int)} will execute at least once.
         *
         * <p>{@link #task(Worker, int)} will not be called from within this call, But it may
         * be called in parallel from another worker thread before this call returns.</p>
         *
         * <p>This method will always enqueue {@code this} into a queue that is shared with
         * all worker threads, even if called from a worker thread. This reduces the average
         * latency until {@link #task(Worker, int)} is called, at the expense of increased
         * contention at the queue. Use this method when the intent of the {@link Task} is
         * to offload computation, not simply "do this when you can".</p>
         */
        protected final void awakeParallel() {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) == 0)
                emitterSvc.queue.put(this, (int)Thread.currentThread().threadId());
        }

        /**
         * Arbitrary code that does whatever is the purpose of this task. Implementations
         * must not block and should {@link #awakeSameWorker()} and return instead of running
         * unbounded or long loops.
         * This method runs in response to a previous {@code awake*Worker()} call. This method
         * will always be called from withing the given {@code worker} thread and will
         * never be executed in parallel (for the same {@link Task} instance).
         *
         * @param worker {@link Worker} thread that is calling this method
         * @param threadId result of {@code (int)}{@link Thread#threadId()} for {@link Thread#currentThread()}
         */
        protected abstract void task(Worker worker, int threadId);


        @Async.Execute private void run(Worker worker, int threadId) {
            if (!compareAndSetFlagAcquire(IS_RUNNING))
                return; // run() active on another thread
            int old = (int)SCHEDULED.getAcquire(this);
            try {
                if ((statePlain()&RELEASED_MASK) == 0)
                    task(worker, threadId);
                else
                    journal("skip run of released", this);
            } catch (Throwable t) {
                handleTaskException(t);
            } finally {
                clearFlagsRelease(IS_RUNNING);
            }
            if ((int)SCHEDULED.compareAndExchangeRelease(this, old, 0) != old) {
                SCHEDULED.setRelease(this, (short)1);
                emitterSvc.queue.put(this, worker.workerId);
            } // else: S = 0 and not enqueued, future awake() can enqueue
        }

        private void handleTaskException(Throwable t) {
            log.error("Ignoring {} thrown by {}:",  t.getClass().getSimpleName(), this, t);
        }
    }

    public abstract static class LowPriorityTask<T extends LowPriorityTask<T>> extends Task<T> {
        /** See {@link Task#Task(int, Flags)} */
        protected LowPriorityTask(int initState, Flags flags) {super(initState, flags);}
    }

    private static abstract class Worker_0 extends Thread {
        protected final int threadId, workerId;
        protected final EmitterService svc;

        public Worker_0(@Nullable ThreadGroup group, EmitterService svc, int id) {
            super(group, svc+"-"+id);
            this.workerId = id;
            this.threadId = (int)threadId();
            this.svc = svc;
            setDaemon(true);
            setUncaughtExceptionHandler((w, err)
                    -> log.error("Worker {} failed, Deadlock/starvation imminent", w, err));
        }
    }
    @SuppressWarnings("unused")
    private static abstract class Worker_1 extends Worker_0 {
        private long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        private long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
        public Worker_1(@Nullable ThreadGroup group, EmitterService svc, int id) {
            super(group, svc, id);
        }
    }
    private static abstract class Worker_2 extends Worker_1 {
        protected static final VarHandle PARKED, UNPARKED;
        static {
            try {
                PARKED = MethodHandles.lookup().findVarHandle(Worker_2.class, "plainParked", int.class);
                UNPARKED = MethodHandles.lookup().findVarHandle(Worker_2.class, "plainUnparked", int.class);
            } catch (NoSuchFieldException|IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        protected int plainParked, plainUnparked;

        public Worker_2(@Nullable ThreadGroup group, EmitterService svc, int id) {
            super(group, svc, id);
        }
    }

    @SuppressWarnings("unused")
    public static final class Worker extends Worker_2 {
        private long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        private long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;

        private Worker(ThreadGroup group, EmitterService svc, int id) {
            super(group, svc, id);
        }

        boolean unparkNow() {
            if ((int)PARKED.getOpaque(this) == 0)
                return false;
            plainUnparked = 0;
            plainParked   = 0;
            LockSupport.unpark(this);
            return true;
        }

        boolean tryUnpark() {
            if ((int)PARKED.getOpaque(this) != 0 && (int)UNPARKED.getOpaque(this) == 0) {
                plainUnparked = 1; // will be read by UnparkWorkers.run()
                return true;
            }
            return false;
        }

        void unpark() {
            plainUnparked = 1; // will be read by UnparkWorkers.run()
        }

        void park() {
            if ((int)PARKED.getOpaque(this) == 0) {
                plainParked   = 1;    // notify intent to park
                plainUnparked = 0;  // not parking yet, do not unpark me
            } else {
                LockSupport.park();
                plainParked = 0;
            }
        }

        @Override public void run() {
            if (currentThread() != this)
                throw new IllegalStateException("wrong thread");
            Affinity.setAffinity(svc.cpuAffinity);
            //noinspection InfiniteLoopStatement
            while (true) {
                Task<?> task = svc.queue.take(this);
                try {
                    task.run(this, threadId);
                } catch (Throwable t) {
                    log.error("Dispatch failed for task={}", task, t);
                }
            }
        }
    }

    private static final VarHandle SVC_INIT_LOCK;
    @SuppressWarnings("unused") private static int plainSvcInitLock;
    static {
        try {
            SVC_INIT_LOCK = MethodHandles.lookup().findStaticVarHandle(EmitterService.class, "plainSvcInitLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static EmitterService SVC;

    /**  Get the single global {@link EmitterService} instance */
    public static EmitterService service() {
        if (SVC == null)
            initService(); // should only be called once
        return SVC;
    }
    private static void initService() {
        while ((int)SVC_INIT_LOCK.getAndSetAcquire(1) != 0) Thread.yield();
        try {
            if (SVC == null)
                SVC = new EmitterService();
        } finally { SVC_INIT_LOCK.setRelease(0); }
    }

    @SuppressWarnings("FieldCanBeLocal")
    private final EmitterService.Worker[] workers;
    private final BitSet cpuAffinity;
    private final TaskQueue queue;

    private EmitterService() {
        int nWorkers = ThreadPoolsPartitioner.partitionSize();
        cpuAffinity  = ThreadPoolsPartitioner.nextLogicalCoreSet();
        workers      = new EmitterService.Worker[nWorkers];
        var grp = new ThreadGroup("EmitterService");
        for (int i = 0; i < workers.length; i++)
            workers[i] = new Worker(grp, this, i);
        queue = new TaskQueue(workers);
        for (Worker w : workers)
            w.start();
        Timestamp.onTick(0xd1454270, new UnparkWorkers(this));
    }

    private record UnparkWorkers(EmitterService svc) implements Runnable {
        @Override public void run() { svc.unparkWorkers(); }
        @Override public String toString() {return "UnparkWorkers";}
    }

    private boolean unparkWorkers() {
        boolean unparked = false;
        for (var w : workers) {
            if ((int)Worker.UNPARKED.getOpaque(w) != 0) {
                w.plainUnparked = 0; // do not unpark before a new Worker.unpark()
                if ((int)Worker.PARKED.getOpaque(w) != 0) {
                    w.plainParked = 0; // suppress w.unparkNow() until Worker.park()
                    LockSupport.unpark(w);
                    unparked = true;
                }
            }
        }
        return unparked;
    }

    @Override public String toString() {return "EmitterService";}

    @SuppressWarnings("unused") public String dump() {
        var sb = new StringBuilder().append("EmitterService-").append('\n');
        return queue.dump(sb.append('\n')).toString();
    }

    public static void yieldWorker() {
        if (!SVC.unparkWorkers())
            Thread.yield();
    }

}
