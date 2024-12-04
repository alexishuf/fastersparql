package com.github.alexishuf.fastersparql.util.concurrent;

import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Primer
        extends SingleThreadBackgroundTask<Runnable, MpscUnboundedAtomicArrayQueue<Runnable>> {
    private static final Logger log = LoggerFactory.getLogger(Primer.class);
    public  static final Primer INSTANCE = new Primer();

    private final List<Runnable> tasks = new ArrayList<>();
    private final Runnable all = () -> {
        for (Runnable task : tasks) {
            try {
                task.run();
            } catch (Throwable error) {
                log.error("primer task {} failed", task, error);
            }
        }
    };

    public Primer() {
        super("Pool primer", new MpscUnboundedAtomicArrayQueue<>(PREFERED_QUEUE_CHUNK));
    }

    public static void primeAll() {
        INSTANCE.sched(INSTANCE.all);
        INSTANCE.sync();
    }

    public void schedOnce(Runnable runnable) {
        sched(new RunOnce(runnable));
    }
    private record RunOnce(Runnable runnable) implements Runnable {
        @Override public void run() {runnable.run();}
    }

    @Override public void sched(Runnable item) {
        if (item == null)
            return;
        if (Thread.currentThread() == INSTANCE) {
            item.run();
        } else {
            while (!work.offer(item))
                Thread.yield(); // unbounded queue, will never run
            afterSched();
        }
    }

    @Override protected void handle(Runnable work) {
        work.run();
        if (!(work instanceof RunOnce) && work != all)
            tasks.add(work);
    }
}
