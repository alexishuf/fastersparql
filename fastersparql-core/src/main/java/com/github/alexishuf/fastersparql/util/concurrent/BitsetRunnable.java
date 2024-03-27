package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.onSpinWait;

public abstract class BitsetRunnable<R> implements Runnable, LongRenderer {
    private static final int CHG_EXECUTOR = 0x00000001;
    private static final int SKIP         = 0x00000002;
    private static final int QUEUED       = 0x80000000;
    private static final int SPECIAL_METHODS = CHG_EXECUTOR|SKIP|QUEUED;
    private static final int FIRST_IDX       = numberOfTrailingZeros(SKIP);
    private static final int LAST_IDX        = numberOfTrailingZeros(QUEUED);
    private static final Logger log = LoggerFactory.getLogger(BitsetRunnable.class);
    private static final VarHandle A, THREAD, SET_EXECUTOR_LOCK;
    static {
        try {
            A                 = MethodHandles.lookup().findVarHandle(BitsetRunnable.class, "plainActions", int.class);
            THREAD            = MethodHandles.lookup().findVarHandle(BitsetRunnable.class, "plainThread", Thread.class);
            SET_EXECUTOR_LOCK = MethodHandles.lookup().findVarHandle(BitsetRunnable.class, "plainSetExecutorLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private Executor executor;
    @SuppressWarnings("unused") private int plainActions;
    private final R receiver;
    private final MethodHandle[] methods;
    @SuppressWarnings("unused") private @Nullable Thread plainThread;
    @SuppressWarnings("unused") private int plainSetExecutorLock;
    private final int journalBitset;
    private final String[] methodNames;

    public static class Spec {
        private static final MethodType mthType = MethodType.methodType(void.class);
        private final Class<?> receiver;
        private final MethodHandles.Lookup lookup;
        private int journalBitset;
        private int size = FIRST_IDX+1;
        private final MethodHandle[] methods = new MethodHandle[32];
        private final String[] methodNames = new String[32];

        public Spec(MethodHandles.Lookup lookup) {
            this.lookup = lookup;
            this.receiver = lookup.lookupClass();
            Arrays.fill(methodNames, "UNREGISTERED");
            methodNames[numberOfTrailingZeros(CHG_EXECUTOR)] = "CHG_EXECUTOR";
            methodNames[numberOfTrailingZeros(SKIP)]         = "SKIP";
            methodNames[numberOfTrailingZeros(QUEUED)]       = "QUEUED";
        }

        public int add(String name, boolean journal) {
            if (size >= LAST_IDX)
                throw new IllegalStateException("too many methods");
            try {
                methods[size] = lookup.findVirtual(receiver, name, mthType);
            } catch (NoSuchMethodException|IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
            methodNames[size] = name;
            int mask = 1 << size;
            if (journal)
                journalBitset |= mask;
            ++size;
            return mask;
        }

        public int add(String name) { return add(name, false); }
        /** Sets a method handle that will be invoked everytime {@link #run()}
         *  has executed all other actions and is preparing to exit. */
        public int setLast(String name) {
            try {
                lookup.findVirtual(receiver, name, mthType);
            } catch (NoSuchMethodException|IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
            methodNames[size] = "LAST|"+name;
            return QUEUED;
        }
    }

    public BitsetRunnable(R receiver, Spec spec) { this(null, receiver, spec); }
    public BitsetRunnable(Executor executor, R receiver, Spec spec) {
        if (executor == null)
            executor = ForkJoinPool.commonPool();
        if (!spec.receiver.isInstance(receiver))
            throw new IllegalArgumentException(receiver+" is not a "+spec.receiver);
        this.executor = executor;
        this.receiver = receiver;
        this.methods = spec.methods;
        this.journalBitset = spec.journalBitset;
        this.methodNames = spec.methodNames;
    }

    /**
     * Sets the executor where {@code this} will be scheduled via
     * {@link Executor#execute(Runnable)} to invoke methods scheduled via {@link #sched(int)}.
     *
     * @param executor the new {@link Executor}
     */
    public void executor(Executor executor) {
        while ((int)SET_EXECUTOR_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.yield();
        try {
            if (this.executor == executor)
                return; // no-op
            this.executor = executor;

            // set CHG_EXECUTOR, to make a previously scheduled or concurrent run()
            // re-schedule on the new executor
            int e = (int)A.getAcquire(this), a;
            while ((a=(int)A.compareAndExchangeRelease(this, e, e|CHG_EXECUTOR)) != e)
                e = a;
            a |= CHG_EXECUTOR;

            // If there is no scheduled run(), unset CHG_EXECUTOR because a future sched()
            // will already see the updated executor. This must be done after setting CHG_EXECUTOR
            // because sched(int) races with executor(Executor)
            if ((a&QUEUED) == 0 && (int)A.compareAndExchangeRelease(this, a, a&~CHG_EXECUTOR) == a)
                return; // done: no run() scheduled or scheduled on the new executor

            Thread running = (Thread) THREAD.getAcquire(this);
            // if doRun() indirectly called this method, waiting is a deadlock.
            // else, if web observe doRun() not running, we can safely exit because:
            //   1. doRun() was not executed yet: when it does, it will enqueue on the new executor
            //   2. doRun() already executed and exited, then:
            //      1. it enqueued this on the old executor: then it did not consume CHG_EXECUTOR,
            //         which will be consumed when it runs again
            //      2. it enqueued this on the new executor, which was the goal
            //      3. it did not enqueued this anywhere: then it saw no scheduled actions and
            //         a future sched() will use the new executor. This is very rare and
            //         should not happen in x86. On Arm it may happen because memory ordering
            if (running == Thread.currentThread() || running == null)
                return;

            // doRun() appears to be concurrently running, it is not safe to return
            // control to our caller
            waitConcurrentDoRunChgExecutor();
        } finally {
            SET_EXECUTOR_LOCK.setRelease(this, 0);
        }
    }

    private void waitConcurrentDoRunChgExecutor() {
        journal("concurrent doRun, set exec rcv=", receiver);
        // this is scheduled on the old executor and a thread from the old executor has
        // entered doRun(). Wait until:
        //   1. concurrent doRun() has unset CHG_EXECUTOR: it will re-schdule this on the
        //      new executor which is already visible to it.
        //   2. concurrent doRun() exits before it saw CHG_EXECUTOR. This is rare but may
        //      happen because A and THREAD are independently synchronized. The old executor
        //      might have scheduled itself on the old executor, on the new executor or in
        //      no executor at all:
        //      - If it rescheduled on the old executor, a future doRun() it will process
        //        CHG_EXECUTOR before any method and re-schedule on the new executor
        //      - If it rescheduled on the new executor, all is good
        //      - If it did not reschedule, it there were no actions left and it cleared QUEUED
        //        a future sched() will enqueue this on the new executor.
        Thread running = (Thread)THREAD.getAcquire(this);
        int a = (int)A.getAcquire(this);
        for (int i=0; (a&CHG_EXECUTOR) != 0 && running != null; ++i) {
            if ((i&0xf) == 0xf) Thread.yield();
            else                Thread.onSpinWait();
            a = (int)A.getAcquire(this);
            running = (Thread)THREAD.getAcquire(this);
        }
        journal("waited concurrent doRun, set exec rcv=", receiver);
    }

    private void journalSched(int actions) {
        var msg = (plainActions& QUEUED) == 0 ? "sched (first)" : "sched";
        for (int rem = actions, a, i; rem != 0; rem &= ~a) {
            a = 1<<(i=numberOfTrailingZeros(rem));
            if ((journalBitset&a) != 0)
                journal(msg, methodNames[i], "on", receiver);
        }
    }

    /**
     * Schedules a future execution of all methods in {@code methods}.
     *
     * <p>If {@link #executor(Executor)} has not been set yet, once it is set, methods queued
     * by this call will be scheduled and eventually executed.</p>
     *
     * @param methods bitset of methods to execute
     * @return {@code true} iff all methods have been executed or will certainly execute
     *         even if there is no future {@link #executor(Executor)} call
     * @throws RejectedExecutionException if the executor is set, {@code this} is not scheduled
     *                                    and the executor rejects new tasks.
     */
    public void sched(int methods) {
        methods &= ~QUEUED;
        if (ThreadJournal.ENABLED && (journalBitset&methods) != 0)
            journalSched(methods);
        int queue = plainActions, witness;
        while ((witness=(int)A.compareAndExchange(this, queue, queue|methods)) != queue)
            queue = witness;
        // if witness had QUEUED, run() will see methods or will call enqueue()
        if ((queue&QUEUED) == 0)
            enqueueIfNot(queue|methods);
    }

    private void enqueueIfNot(int queue) {
        for (int witness; (queue&QUEUED) == 0; queue = witness) {
            if ((witness=(int)A.compareAndExchange(this, queue, queue|QUEUED)) == queue) {
                enqueueUnchecked();
                break;
            }
        }
    }

    private void enqueueUnchecked() {
        try {
            executor.execute(this);
        } catch (RejectedExecutionException re) {
            handleRejection();
        }
    }

    private void handleRejection() {
        executor = ForkJoinPool.commonPool();
        try {
            executor.execute(this);
        } catch (RejectedExecutionException re2) {
            log.error("ForkJoin.commonPool() rejected {}", this, re2);
        }
    }

    /** Whether the current thread is executing {@link #run()} or {@link #runNow()}. */
    public boolean inRun() {
        return plainThread == currentThread();
    }

    /**
     * Attempt to run queued actions from within  this call if there is no concurrent
     * {@link #run()}.
     */
    public void runNow() {
        int ex = (int)A.getAcquire(this), ac;
        if ((ex&~SPECIAL_METHODS) == 0)
            return; // no work
        if ((ex&SKIP) != 0)
            throw new IllegalStateException("concurrent runNow()");
        Thread self = currentThread();
        if (plainThread == self)
            return; // do not run recursively
        // set SKIP to make a previously scheduled or concurrent run() exit
        while ((ac=(int)A.compareAndExchangeRelease(this, ex&~SKIP, ex|SKIP)) != ex)
            ex = ac;
        try {
            // wait until concurrent run() exits
            while ((Thread)THREAD.compareAndExchangeRelease(this, null, self) != null)
                onSpinWait();
            // invoke queued methods
            doRun(0);
        } finally {
            // unset SKIP
            ex = (int)A.getAcquire(this);
            while ((ac=(int)A.compareAndExchangeRelease(this, ex, ex&~SKIP)) != ex)
                ex = ac;
        }
    }

    @Override public void run() { doRun(SKIP); }

    private void doRun(int stopWhen) {
        Thread self = currentThread();
        try {
            THREAD.setRelease(this, self);
            int continueMask = stopWhen|CHG_EXECUTOR|QUEUED, queue, done = 0;
            while (((queue=(int)A.getAcquire(this))&continueMask) == QUEUED) {
                int mthMask = Integer.lowestOneBit(queue), ex = queue;
                while ((queue=(int)A.compareAndExchangeRelease(this, ex, ex&~mthMask)) != ex)
                    ex = queue;
                invoke(mthMask);
                if ((done&mthMask) != 0)
                    continueMask |= mthMask; // break before running same method for 3rd time
                else
                    done |= mthMask;
            }
            if ((queue&CHG_EXECUTOR) != 0)
                doChgExecutor();
            else if ((queue&QUEUED) != 0 && (queue&stopWhen) == 0)
                enqueueUnchecked(); // stopped due to method spam 2 invocations scheduled
            else if (queue != 0)
                enqueueIfNot(queue); // stopWhen or consumed QUEUED concurrently with sched()
        } catch (Throwable t) {
            journal(t, "on BitsetRunnable recv=", receiver);
            log.error("{} on {}.run()", t.getClass().getSimpleName(), this, t);
        } finally {
            THREAD.compareAndExchangeRelease(this, self, null);
        }
    }

    /**
     * This will be called if a method previously scheduled raises a {@link Throwable} from within {@link #run()}.
     * @param methodName the method name given in {@link Spec#add(String, boolean)}
     * @param t the {@link Throwable} thrown by the method
     */
    protected void onMethodError(String methodName, Throwable t) {
        journal(t.getClass().getSimpleName()+":"+t.getMessage()+"while running action=",
                methodName, "on", receiver);
        log.error("{} thrown during invocation of {} on {}", t.getClass().getSimpleName(),
                  methodName, receiver, t);
    }

    private void doChgExecutor() {
        // clear CHG_EXECUTOR bit
        int e = (int)A.getAcquire(this), a;
        while ((a=(int)A.compareAndExchangeRelease(this, e, e&~CHG_EXECUTOR)) != e)
            e = a;
        if ((e&QUEUED) != 0)  // QUEUED makes enqueue() a no-op, enqueue now
            executor.execute(this);
    }

    /** Safely invoke a method and handle any exception */
    private void invoke(int mthMask) {
        int idx = numberOfTrailingZeros(mthMask);
        try {
            if (ThreadJournal.ENABLED && (journalBitset&mthMask) != 0)
                journal("running", methodNames[idx], "on", receiver);
            var handle = methods[idx];
            if (handle == null) {
                if ((mthMask&SPECIAL_METHODS) != 0)
                    return;
                throw new IllegalArgumentException("No method registered");
            }
            handle.invoke((R)receiver);
        } catch (Throwable t) {
            onMethodError(methodNames[idx], t);
        }
    }

    @Override public String render(long methods) {
        methods &= 0xffffffffL;
        int bits = Long.bitCount(methods);
        if (bits == 0) return "[]";
        else if (bits == 1) return methodNames[Long.numberOfTrailingZeros(methods)];
        var sb = new StringBuilder().append('[');
        for (int i = 0; (i+=Long.numberOfTrailingZeros(methods>>>i)) < 64; i++)
            sb.append(methodNames[i]).append(',');
        sb.setLength(sb.length()-1);
        return sb.append(']').toString();
    }

    @Override public String toString() {
        return "BitsetRunnable("+receiver+")";
    }
}
