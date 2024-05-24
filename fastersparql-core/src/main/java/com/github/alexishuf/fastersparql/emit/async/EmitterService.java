package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Runtime.getRuntime;
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
public final class EmitterService {
    private static final Logger log = LoggerFactory.getLogger(EmitterService.class);

    /**
     * An arbitrary task ({@link #task(Worker, int)}) that can be repeatedly re-scheduled via {@link #awake(boolean)}.
     */
    public abstract static class Task<S extends Stateful<S>> extends Stateful<S> {
        private static final VarHandle SCHEDULED;
        static {
            try {
                SCHEDULED = MethodHandles.lookup().findVarHandle(Task.class, "plainScheduled", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        protected static final int IS_RUNNING  = 0x80000000;
        protected static final Stateful.Flags TASK_FLAGS = Stateful.Flags.DEFAULT.toBuilder()
                .flag(IS_RUNNING, "RUNNING").build();


        protected final EmitterService emitterSvc;
        @SuppressWarnings("unused") private int plainScheduled;

        /**
         * Create  a new {@link Task}, optionally  assigned to a preferred worker.
         *
         * @param emitterSvc the {@link EmitterService} where the task will run.
         */
        protected Task(EmitterService emitterSvc, int initState, Flags flags) {
            super(initState, flags);
            assert flags.contains(TASK_FLAGS);
            this.emitterSvc = emitterSvc;
        }

        /**
         * Ensures that after this call, {@link #task(Worker, int)} will execute at least once. Such execution
         * may start and complete before the return of this call.
         *
         * @param onCurrentWorker if {@code true} and this method is called from within a
         *                        worker thread (even if not during execution of
         *                        {@code this.}{@link #task(Worker, int)}), then will try to enqueue
         *                        {@code this} task into the worker-local task queue, from which
         *                        other worker cannot steal from. If {@code this} is also the task
         *                        being currently executed, the local queue will be used even
         *                        if this parameter is {@code false}. If the worker-local queue
         *                        is full, {@code this} will be offered to a shared queue,
         *                        enabling other worker threads to execute it.
         */
        protected final void awake(boolean onCurrentWorker) {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) != 0)
                return; // already queued
            if (currentThread() instanceof Worker w && (onCurrentWorker || this == w.current)) {
                if (w.offerTaskLocal(this))
                    return;
            }
            emitterSvc.putTaskShared(this);
        }

        /**
         * Similar to {@link #awake(boolean)} with {@code onCurrentWorker=false}, but even
         * if this method is called from a {@link Worker} currently executing
         * {@code this.}{@link #task(Worker, int)}, {@code this} will be enqueued on a queue
         * subject to work stealing.
         */
        protected final void awakeParallel() {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) == 0)
                emitterSvc.putTaskShared(this);
        }

        /**
         * Equivalent to {@link #awake(boolean)} with {@code onCurrentWorker=true}.
         * @param currentWorker The {@link Thread#currentThread()}, which is also given as
         *                      an argument to {@link Task#task(Worker, int)}.
         */
        protected final void awake(Worker currentWorker) {
            if ((int)SCHEDULED.getAndAddRelease(this, 1) == 0) {
                if (!currentWorker.offerTaskLocal(this))
                    emitterSvc.putTaskShared(this);
            }
        }

        /**
         * Arbitrary code that does whatever is the purpose of this task. Implementations
         * must not block and should {@link #awake(boolean)} and return instead of running loops. If
         * this runs in response to an {@link #awake(boolean)}, it will run in a worker thread of the
         * {@link EmitterService} set at construction. If running due to {@link #runNow()}, it
         * may be called from an external thread. Whatever the trigger, there will be no parallel
         * executions of this method for a single {@link Task} instance.
         *
         * @param worker {@link Worker} thread that is calling this method, if this is being
         *               called from a worker and not from {@link #runNow()} from a
         *               non-worker thread.
         * @param threadId result of {@code (int)}{@link Thread#threadId()} for {@link Thread#currentThread()}
         */
        protected abstract void task(@Nullable Worker worker, int threadId);

        /**
         * Execute this task <strong>now</strong>, unless it is already being executed by
         * another thread. Unlike a direct call to {@link #task(Worker, int)}, this will ensure there are
         * no concurrent {@link #task(Worker, int)} calls for the same {@link Task} object. If there is a
         * concurrent execution by another thread on {@link #run(Worker, int)} or {@code runNow()}, this
         * call will have no effect
         *
         * <p>This should be called to solve inversion-of-priority issues. A producer that finds
         * itself being blocked due to this task not running can call this method to have the
         * consumer task work instead of spinning or parking.</p>
         */
        @SuppressWarnings("unused") protected boolean runNow() {
            var currentThread = currentThread();
            var worker = currentThread instanceof Worker w ? w : null;
            return run(worker, (int)currentThread.threadId());
        }

        @Async.Execute private boolean run(@Nullable Worker worker, int threadId) {
            if (!compareAndSetFlagAcquire(IS_RUNNING))
                return false; // run() active on another thread
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
            return true; // task() was called
        }

        private void handleTaskException(Throwable t) {
            log.error("Ignoring {} thrown by {}:",  t.getClass().getSimpleName(), this, t);
        }
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

    private static final int LOCAL_QUEUE_WIDTH     = 128/4;
    private static final int LOCAL_QUEUE_SIZE_MASK = 7;

    public static abstract class Worker extends Thread {
        public final int threadId, workerId;
        private final EmitterService svc;
        private final Task<?>[] queue;
        private final int queueBegin;
        private int queueHead, queueSize;
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
            //noinspection InfiniteLoopStatement
            while (true) {
                Task<?> task = pollTaskLocal();
                if (task == null) {
                    task = svc.takeTaskShared(workerId); // will spin/park
                } else if (task == current) {
                    // if task() returned, it is because it wants/needs other task to execute
                    // not doing this can lead to big slowdowns when all workers are
                    // executing tasks that "do some work until cancelled" and the cancelling
                    // tasks are stuck in sharedQueue
                    var other = svc.pollTaskShared();
                    if (other != null) {
                        if (!offerTaskLocal(task))
                            svc.putTaskShared(task);
                        task = other;
                    }
                }
                current = task;
                try {
                    task.run(this, threadId);
                } catch (Throwable t) {
                    log.error("Dispatch failed for task={}", current, t);
                }
                current = null;
            }
        }

        /**
         * If {@code other} is in the task queue that is private to this Worker, remove it
         * and insert it into the shared queue so that another worker can execute
         * it concurrently. Implementations of this method are allowed to not remove
         * {@code other} even if it is present in the queue.
         *
         * @param other another task that should be executed concurrently.
         */
        public void expelRelaxed(Task<?> other) {
            assert currentThread() == this : "wrong thread";
            final int head = queueHead, size = this.queueSize;
            int i = 0;
            while (i < size && queue[queueBegin+((head+i)&LOCAL_QUEUE_SIZE_MASK)] != other)
                ++i;
            if (i >= size)
                return; // not found
            int idx0 = queueBegin+((head+i)&LOCAL_QUEUE_SIZE_MASK);
            for (int idx1; i+1 < size; ++i, idx0=idx1)
                queue[idx0] = queue[idx1 = queueBegin+((head+i+1)&LOCAL_QUEUE_SIZE_MASK)];
            queueSize = size-1;
            svc.putTaskShared(other);
        }

        private @Nullable Task<?> pollTaskLocal() {
            int size = this.queueSize;
            if (size <= 0)
                return null;
            queueSize = size-1;
            int idx   = queueBegin + queueHead;
            queueHead = (queueHead+1)&LOCAL_QUEUE_SIZE_MASK;
            return queue[idx];
        }

        protected boolean offerTaskLocal(Task<?> task) {
            int size = queueSize;
            if (size >= LOCAL_QUEUE_SIZE_MASK)
                return false; // no free space
            queue[queueBegin+((queueHead+queueSize++)&LOCAL_QUEUE_SIZE_MASK)] = task;
            return true; // task taken
        }

        private @Nullable Task<?> peekLocal(int i) {
            return queue[queueBegin+(queueHead+i)&LOCAL_QUEUE_SIZE_MASK];
        }

        private void doYieldWorker() {
            Task<?> task = pollTaskLocal();
            if (task != null) {
                svc.putTaskShared(task);
            } else if (!Unparker.volunteer()) {
                if (!svc.unparkStealer())
                    Thread.yield();
            }
        }
    }

    private static final AtomicInteger nextServiceId = new AtomicInteger(1);
    public static final EmitterService EMITTER_SVC
            = new EmitterService(getRuntime().availableProcessors());

    private static final int LOCKED = -1;
    private static final VarHandle SIZE;
    static {
        try {
            SIZE = MethodHandles.lookup().findVarHandle(EmitterService.class, "plainSize", int.class);
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final EmitterService.Worker[] workers;
    private final int id;
    @SuppressWarnings("unused") private int plainSize;
    private int tasksHead;
    private int tasksMask;
    private EmitterService.Task<?>[] tasks;
    private int parkedBS;

    public EmitterService(int nWorkers) {
        nWorkers = 1 << Math.max(1, 32-Integer.numberOfLeadingZeros(nWorkers-1));
        assert nWorkers <= 0xffff;
        id           = nextServiceId.getAndIncrement();
        workers      = new EmitterService.Worker[nWorkers];
        tasksMask    = 0x1fff;
        tasks        = new EmitterService.Task[tasksMask+1];
        var workerQueue = new EmitterService.Task[(nWorkers+2)*LOCAL_QUEUE_WIDTH];
        var grp = new ThreadGroup("EmitterService-"+id);
        for (int i = 0; i < workers.length; i++) {
            int workerQueueBegin = LOCAL_QUEUE_WIDTH*(i+1);
            workers[i] = new PaddedWorker(grp, this, i, workerQueue, workerQueueBegin);
        }
        for (Worker w : workers)
            w.start();
    }

    @Override public String toString() {return "EmitterService-"+id;}

    @SuppressWarnings("unused") public String dump() {
        var sb = new StringBuilder().append("EmitterService-").append(id).append('\n');
        sb.append("  shared queue: ").append(plainSize).append(" items\n");
        sb.append("parkedBS: ").append(Integer.toHexString(parkedBS));
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

    /**
     * If there are parked workers, at least one is elected as <i>stealer</i> and will park for
     * a timeout instead of until unparked. This method un-parks the stealer <strong>NOW</strong>,
     * using a {@link LockSupport#unpark(Thread)} instead of {@link Unparker#unpark(Thread)}.
     */
    public boolean unparkStealer() {
        int id = Integer.numberOfTrailingZeros(parkedBS);
        if (id <= workers.length) {
            LockSupport.unpark(workers[id]);
            return true;
        }
        return false;
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
        int size, parkedMask = workerId < 32 ? 1<<workerId : 0, next = 0;
        while (true) {
            while ((size=(int)SIZE.getAndSetAcquire(this, LOCKED)) == LOCKED)
                onSpinWait();
            if (size > 0) { // has task in queue
                Task<?> task = tasks[tasksHead];
                // unset this thread and another thread as parked
                int bs = parkedBS&~parkedMask;
                if (size == 1) {
                    tasksHead = 0;
                } else {
                    tasksHead = (tasksHead+1)&tasksMask;
                    bs &= ~(next=Integer.lowestOneBit(bs));
                }
                parkedBS = bs;
                SIZE.setRelease(this, size-1); // release lock
                // if there was another parked thread, cheaply unpark it
                if (next != 0)
                    Unparker.unpark(workers[Integer.numberOfTrailingZeros(next)]);
                return task;
            } else {
                parkedBS |= parkedMask;
                SIZE.setRelease(this, 0);
                LockSupport.park(this);
            }
        }
    }

    private void putTaskShared(Task<?> task) {
        int size;
        while ((size=(int)SIZE.getAndSetAcquire(this, LOCKED)) == LOCKED)
            onSpinWait();
        if (size == tasksMask)
            growTasks();
        tasks[(tasksHead+size)&tasksMask] = task;
        int bs = parkedBS, next = bs & -bs;
        parkedBS = bs & ~next;
        SIZE.setRelease(this, size+1);
        if (next != 0)
            Unparker.unpark(workers[Integer.numberOfTrailingZeros(next)]);
    }

    private void growTasks() {
        tasks = Arrays.copyOf(tasks, tasks.length<<1);
        tasksMask = tasks.length-1;
    }
}
