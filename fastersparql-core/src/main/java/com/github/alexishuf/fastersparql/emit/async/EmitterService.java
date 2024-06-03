package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import net.openhft.affinity.Affinity;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.onSpinWait;

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
public final class EmitterService extends EmitterService_3 {
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
         * be called in paralle from another worker thread before this call returns.</p>
         *
         * <p>This method will prefer enqueuing {@code this} into a worker-private queue if
         * the calling thread is a worker thread. Such queue is not shared with other worker
         * and thus does not suffer from contention. Tasks only move out of the worker-private
         * queue if the worker thread evicts them due to imbalance or due to a
         * {@link #yieldWorker(Thread)} call.</p>
         */
        protected final void awakeSameWorker() {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) != 0)
                return; // already queued
            if (currentThread() instanceof Worker w && w.offerTaskLocal(this))
                return; // queued into local list
            emitterSvc.putTaskShared(this);
        }

        /**
         * Equivalent to {@link #awakeSameWorker()} but assumes {@link Thread#currentThread()}
         * is {@code currentWorker}. This should be used when a {@link #task(Worker, int)}
         * whishes to continue processing later and is returning now only to be polite and
         * allow other tasks to execute.
         *
         * <p><strong>Attention:</strong>{@code currentWorker} MUST BE
         * {@link Thread#currentThread()}. If not, {@code this} or other tasks might be silently
         * dropped from the queue and will thus starve for all eternity.</p>
         */
        protected final void awakeSameWorker(Worker currentWorker) {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) == 0) {
                if (!currentWorker.offerTaskLocal(this))
                    emitterSvc.putTaskShared(this);
            }
        }

        /**
         * Ensures that after this call, {@link #task(Worker, int)} will execute at least once.
         *
         * <p>{@link #task(Worker, int)} will not be called from within this call, But it may
         * be called in paralle from another worker thread before this call returns.</p>
         *
         * <p>This method will always enqueue {@code this} into a queue that is shared with
         * all worker threads, even if called from a worker thread. This reduces the average
         * latency until {@link #task(Worker, int)} is called, at the expense of increased
         * contention at the queue. Use this method when the intent of the {@link Task} is
         * to offload computation, not simply "do this when you can".</p>
         */
        protected final void awakeParallel() {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) == 0)
                emitterSvc.putTaskShared(this);
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
                if (worker == null || !worker.offerTaskLocal(this))
                    emitterSvc.putTaskShared(this);
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

    @SuppressWarnings("unused")
    private static class PaddedWorker extends Worker {
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7; // 64 bytes
        private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7; // 64 bytes

        public PaddedWorker(ThreadGroup group, EmitterService svc, int id,
                            Task<?>[] queue, int queueBegin) {
            super(group, svc, id, queue, queueBegin);
        }
    }

    private static final int LOCAL_QUEUE_CAPACITY = 8;
    private static final int LOCAL_QUEUE_MASK     = LOCAL_QUEUE_CAPACITY-1;
    private static final int LOCAL_QUEUE_WIDTH    = LOCAL_QUEUE_CAPACITY*2 + (128/4);
    static {assert Integer.bitCount(LOCAL_QUEUE_CAPACITY) == 1;}

    public static abstract class Worker extends Thread {
        public final int threadId, workerId;
        private final EmitterService svc;
        private final Task<?>[] queue;
        private final int queueBegin;
        private int queueHead, queueSize;
        private int lpQueueHead, lpQueueSize;
        private @Nullable Task<?> current;

        protected Worker(ThreadGroup group, EmitterService svc, int id, Task<?>[] queue, int queueBegin) {
            super(group, svc+"-"+id);
            this.workerId   = id;
            this.threadId   = (int)threadId();
            this.svc        = svc;
            this.queue      = queue;
            this.queueBegin = queueBegin;
            setDaemon(true);
            setUncaughtExceptionHandler((w, err) -> {
                log.error("Worker {} failed. Deadlock/starvation imminent", w, err);
                try {
                    for (Task<?> task; (task=pollTaskLocal()) != null; ) {
                        try {
                            svc.putTaskShared(task);
                        } catch (Throwable t) {
                            log.error("putTaskShared({}): {}", task, t.toString());
                        }
                    }
                } catch (Throwable t) {
                    log.error("Draining local tasks from {} failed: {}", w, t.toString());
                }
            });
        }

        @Override public void run() {
            if (currentThread() != this)
                throw new IllegalStateException("wrong thread");
            Affinity.setAffinity(svc.cpuAffinity);
            int skipOffload = workerId, offloadSkipPeriod = svc.workers.length;
            //noinspection InfiniteLoopStatement
            while (true) {
                Task<?> task = pollTaskLocal();
                if (task == null) {
                    current = null;
                    task = svc.takeTaskShared(workerId); // will spin/park
                } else if (task == current) {
                    // if task() returned, it is because it wants/needs other task to execute
                    // not doing this can lead to big slowdowns when all workers are
                    // executing tasks that "do some work until cancelled" and the cancelling
                    // tasks are stuck in sharedQueue
                    var other = pollTaskLocal();
                    if (other == null)
                        other = svc.pollTaskShared();
                    if (other != null) {
                        if (!offerTaskLocal(task))
                            svc.putTaskShared(task);
                        task = other;
                    }
                } else if (skipOffload-- <= 0 && queueSize > 1)  {
                    tryExpelLastTask();
                    skipOffload = offloadSkipPeriod;
                }
                current = task;
                try {
                    task.run(this, threadId);
                } catch (Throwable t) {
                    log.error("Dispatch failed for task={}", current, t);
                }
            }
        }

        private void tryExpelLastTask() {
            int last = queueSize-1;
            if (last < 0)
                return;
            int idx = queueBegin + ((queueHead+last)&LOCAL_QUEUE_MASK);
            boolean expelled = svc.offerTaskSharedIfEmpty(queue[idx]);
            if (expelled)
                queueSize = last;
        }

        private @Nullable Task<?> pollTaskLocal() {
            int size = this.queueSize, idx;
            if (size <= 0) {
                size = lpQueueSize;
                if (size <= 0)
                    return null;
                lpQueueSize = size-1;
                idx         = queueBegin+LOCAL_QUEUE_CAPACITY+lpQueueHead;
                lpQueueHead = (lpQueueHead+1)&LOCAL_QUEUE_MASK;
            } else {
                queueSize = size-1;
                idx       = queueBegin + queueHead;
                queueHead = (queueHead+1)&LOCAL_QUEUE_MASK;
            }
            return queue[idx];
        }

        protected boolean offerTaskLocal(Task<?> task) {
            if (task instanceof LowPriorityTask<?>)
                return offerTaskLocalLowPriority(task);
            int size = queueSize;
            if (size >= LOCAL_QUEUE_CAPACITY)
                return false; // no free space
            queue[queueBegin+((queueHead+queueSize++)&LOCAL_QUEUE_MASK)] = task;
            return true; // task taken
        }

        private boolean offerTaskLocalLowPriority(Task<?> task) {
            int size = lpQueueSize;
            if (size >= LOCAL_QUEUE_CAPACITY)
                return false; // no free space
            queue[queueBegin + LOCAL_QUEUE_CAPACITY
                             + ((queueHead+lpQueueSize++)&LOCAL_QUEUE_MASK)] = task;
            return true; // task taken
        }

        private @Nullable Task<?> peekLocal(int i) {
            return queue[queueBegin+(queueHead+i)&LOCAL_QUEUE_MASK];
        }

        private void doYieldWorker() {
            Task<?> task = pollTaskLocal();
            if (task != null) {
                svc.putTaskShared(task);
            } else if (!Unparker.volunteer()) {
                if (svc.plainSize <= 0 && !svc.parked.unparkAny(svc.workers))
                    Thread.yield();
            }
        }
    }

    private record OnTick(EmitterService svc) implements Runnable {
        @Override public void run() {
            var parked  = svc.parked;
            var workers = svc.workers;
            for (int unparked = 1; parked.unparkAny(workers) && unparked < svc.plainSize; )
                ++unparked;
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

    private static final int LOCKED = -1;
    private static final VarHandle SIZE;
    static {
        try {
            SIZE = MethodHandles.lookup().findVarHandle(EmitterService.class, "plainSize", int.class);
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private EmitterService() {
        var workerQueue = new EmitterService.Task[(workers.length+2)*LOCAL_QUEUE_WIDTH];
        var grp = new ThreadGroup("EmitterService");
        for (int i = 0; i < workers.length; i++) {
            int workerQueueBegin = LOCAL_QUEUE_WIDTH*(i+1);
            workers[i] = new PaddedWorker(grp, this, i, workerQueue, workerQueueBegin);
        }
        for (Worker w : workers)
            w.start();
        Timestamp.onTick(0xd1454270, new OnTick(this));
    }

    @Override public String toString() {return "EmitterService";}

    @SuppressWarnings("unused") public String dump() {
        var sb = new StringBuilder().append("EmitterService-").append('\n');
        sb.append("  shared queue: ").append(plainSize).append(" items\n");
        sb.append("parkedBS: ").append(parked);
        sb.append('\n');
        for (int i = 0; i < workers.length; i++) {
            var w = workers[i];
            sb.append(" worker ")
                    .append(i).append(' ')
                    .append(w.getState().name()).append(' ')
                    .append(w.queueSize).append(" tasks queued: ");
            for (int j = 0; j < w.queueSize; j++) {
                var task = w.peekLocal(j);
                if (task == null)
                    break;
                sb.append(i == 0 ? "" : ", ").append(task);
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    public static void yieldWorker(Thread currentThread) {
        if (currentThread instanceof Worker w)
            w.doYieldWorker();
        else if (!Unparker.volunteer())
            Thread.yield();
    }

    private Task<?> pollTaskShared() {
        int size;
        while ((size=(int)SIZE.getAndSetAcquire(this, LOCKED)) == LOCKED)
            onSpinWait();
        if (size > 0) {
            var task  = tasks[tasksHead];
            tasksHead = (tasksHead+1)&tasksMask;
            SIZE.setRelease(this, size-1);
            return task;
        } else {
            SIZE.setRelease(this, 0);
            return null;
        }
    }

    private Task<?> takeTaskShared(int workerId) {
        int size;
        while (true) {
            while ((size=(int)SIZE.getAndSetAcquire(this, LOCKED)) == LOCKED)
                onSpinWait();
            if (size > 0) { // has task in queue
                Task<?> task = tasks[tasksHead];
                tasksHead = size == 1 ? 0 : (tasksHead+1)&tasksMask;
                SIZE.setRelease(this, size-1); // release lock
                return task;
            } else {
                SIZE.setRelease(this, 0);
                parked.park(workerId);
            }
        }
    }

    private boolean offerTaskSharedIfEmpty(Task<?> task) {
        boolean lockedEmpty = SIZE.weakCompareAndSetAcquire(this, 0, LOCKED);
        if (lockedEmpty) {
            tasks[(tasksHead)&tasksMask] = task;
            SIZE.setRelease(this, 1);
        }
        return lockedEmpty;
    }


    private void putTaskShared(Task<?> task) {
        int size;
        while ((size=(int)SIZE.getAndSetAcquire(this, LOCKED)) == LOCKED)
            onSpinWait();
        if (size == tasksMask)
            growTasks();
        tasks[(tasksHead+size)&tasksMask] = task;
        SIZE.setRelease(this, size+1);
        if (size == 0)
            parked.unparkAnyIfAllParked(workers);
    }

    private void growTasks() {
        tasks = Arrays.copyOf(tasks, tasks.length<<1);
        tasksMask = tasks.length-1;
    }
}
class EmitterService_0 {
    protected final EmitterService.Worker[] workers;
    protected final ParkedSet parked;
    protected final BitSet cpuAffinity;
    protected       EmitterService.Task<?>[] tasks;
    protected       int tasksMask;

    public EmitterService_0() {
        int nWorkers = ThreadPoolsPartitioner.partitionSize();
        cpuAffinity  = ThreadPoolsPartitioner.nextLogicalCoreSet();
        workers      = new EmitterService.Worker[nWorkers];
        parked       = new ParkedSet(nWorkers);
        tasksMask    = 0x1fff;
        tasks        = new EmitterService.Task[tasksMask+1];
    }
}
@SuppressWarnings("unused")
class EmitterService_1 extends EmitterService_0 {
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
    private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
    public EmitterService_1() {super();}
}
class EmitterService_2 extends EmitterService_0 {
    @SuppressWarnings("unused") protected int plainSize;
    protected int tasksHead;
    public EmitterService_2() {super();}
}
@SuppressWarnings("unused")
class EmitterService_3 extends EmitterService_2 {
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
    private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
    public EmitterService_3() {super();}
}
