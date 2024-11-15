package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.util.OOMHandler;

public class CpuAffinityRunnable implements Runnable {
    private final Runnable delegate;
    private final int workerId;

    public CpuAffinityRunnable(Runnable delegate, int workerId) {
        this.delegate = delegate;
        this.workerId = workerId;
    }

    @Override public void run() {
        try {
            AffinityHelper.setCurrentThreadPhysicalCoreAffinity(workerId);
            delegate.run();
        } catch (OutOfMemoryError e) {
            OOMHandler.notifyOOM(e);
            throw e;
        }
    }
}
