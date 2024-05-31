package com.github.alexishuf.fastersparql.emit.async;

import net.openhft.affinity.Affinity;

import java.util.BitSet;

public class CpuAffinityRunnable implements Runnable {
    private final Runnable delegate;
    private final BitSet affinityMask;

    public CpuAffinityRunnable(Runnable delegate, BitSet affinityMask) {
        this.delegate = delegate;
        this.affinityMask = affinityMask;
    }

    @Override public void run() {
        Affinity.setAffinity(affinityMask);
        delegate.run();
    }
}
