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

    @Override public void sched(Runnable item) {
        if (Thread.currentThread() == INSTANCE)
            item.run();
        super.sched(item);
    }

    @Override protected void handle(Runnable work) {
        work.run();
        if (work != all)
            tasks.add(work);
    }
}
