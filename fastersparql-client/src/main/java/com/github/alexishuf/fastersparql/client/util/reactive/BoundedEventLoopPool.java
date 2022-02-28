package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A lazy-initialized of {@link Thread}s with an upper bound designed to execute short-lived,
 * mostly non-blocking tasks always in the same thread.
 *
 * There are two reasons for using this instead of {@link Async#async(Runnable)}:
 * <ol>
 *     <li>The number of threads is bound</li>
 *     <li>The code to execute does not perform IO and is thus CPU-bound (short blocks on
 *         synchronization primitives are OK, so long no deadlock/starvation arises</li>
 * </ol>
 *
 * In comparison to a {@link Executors#newFixedThreadPool(int)}, the rationale for this class
 * is that it strongly binds repeated execution of the same task on the same {@link Thread}
 * (which preserves affinity of that task to a CPU, if the OS can retain thread-CPU affinity).
 *
 */
public class BoundedEventLoopPool implements AutoCloseable {
    private final String name;
    private final int maxThreads;
    private int nextExecutor = 0;
    private boolean closed = false;
    private final List<LoopExecutor> executors;

    public static BoundedEventLoopPool get() {
        return SHARED;
    }

    public BoundedEventLoopPool(String name, int size) {
        this.name = name;
        this.maxThreads = size;
        this.executors = new ArrayList<>(size);
    }

    /**
     * A {@link Executor} that always execute on the same thread (not the caller thread).
     */
    public final class LoopExecutor implements Executor {
        private final int idx;
        @SuppressWarnings("FieldCanBeLocal") private final Thread thread;
        private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        private final AtomicInteger tasks = new AtomicInteger(0);

        private LoopExecutor(int idx) {
            this.idx = idx;
            thread = new Thread(() -> runTasks(this));
            thread.setName(name+"-"+idx);
            thread.setDaemon(true);
            thread.start();
        }

        public boolean isFree() {
            return tasks.get() > 0;
        }

        @Override public void execute(@NonNull Runnable task) {
            if (closed)
                throw new RejectedExecutionException(BoundedEventLoopPool.this+" is close()ed");
            queue.add(task);
            tasks.incrementAndGet();
        }

        @Override public String toString() {
            return name+".LoopExecutor["+idx+"]";
        }
    }

    /**
     * Get a {@link LoopExecutor} in a round-robin fashion. This may create a new
     * {@link LoopExecutor} and associated {@link Thread} if the maximum number of threads
     * defined in the constructor has not yet been reached.
     *
     * Users of this method should save the {@link LoopExecutor} instance and always using that
     * instance to submit the same {@link Runnable} (or {@link Runnable}s that run the same code
     * / access the same data).
     *
     * @return a {@link LoopExecutor} where to submit {@link Runnable}s for execution.
     * @throws IllegalStateException if {@link BoundedEventLoopPool#close()} has been
     *                               previously called
     */
    public synchronized LoopExecutor chooseExecutor() {
        log.trace("{}.chooseSubmitter() nextQueue={}, target={}", this, nextExecutor, maxThreads);
        assert nextExecutor < maxThreads;
        if (closed)
            throw new IllegalStateException(this+" is close()d");
        LoopExecutor loopExecutor;
        if (nextExecutor >= executors.size()) {
            loopExecutor = new LoopExecutor(nextExecutor);
            executors.add(loopExecutor);
        } else {
            loopExecutor = executors.get(nextExecutor);
        }
        nextExecutor = (nextExecutor + 1) % maxThreads;
        return loopExecutor;
    }

    /**
     * Starts the process of shutting down all threads held by this instance.
     *
     * This is asynchronous. Immediately, {@link BoundedEventLoopPool#chooseExecutor()} and
     * {@link LoopExecutor#execute(Runnable)} will fail with {@link IllegalStateException} and
     * {@link RejectedExecutionException}, but already queued tasks will be executed until
     * completion without being interrupted.
     */
    @Override public synchronized void close() {
        log.trace("{}.close()", this);
        for (LoopExecutor queue : executors)
            queue.execute(TerminationRunnable.INSTANCE);
        if (!closed)
            closed = true;
    }

    @Override public String toString() {
        return name;
    }

    /* --- --- --- implementation details --- --- --- */

    private static final Logger log = LoggerFactory.getLogger(BoundedEventLoopPool.class);
    private static final int SHARED_THREADS
            = Math.max(4, (int)Math.ceil(Runtime.getRuntime().availableProcessors()*1.5));
    private static final BoundedEventLoopPool SHARED
            = new BoundedEventLoopPool("SharedReactiveEventLoopPool", SHARED_THREADS);

    private static final class  TerminationRunnable implements Runnable {
        public static final TerminationRunnable INSTANCE = new TerminationRunnable();
        @Override public void run() {
            log.error("Termination runnable executed");
        }
    }

    private void runTasks(LoopExecutor loopExecutor) {
        BlockingQueue<Runnable> queue = loopExecutor.queue;
        while (true) {
            try {
                Runnable task = queue.take();
                if (task instanceof TerminationRunnable) {
                    if (!queue.isEmpty()) {
                        queue.add(task);
                        log.error("worker thread {} met TerminationRunnable, but there are " +
                                  "{} tasks pending. Delayed termination",
                                  Thread.currentThread().getName(), queue.size());
                        continue;
                    }
                    break; //terminate
                }
                loopExecutor.tasks.decrementAndGet();
                task.run();
            } catch (InterruptedException e) {
                log.info("runTask thread {} interrupted, will ignore it", Thread.currentThread());
            }
        }
    }
}
