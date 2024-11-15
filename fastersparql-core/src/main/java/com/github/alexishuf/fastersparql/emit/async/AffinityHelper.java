package com.github.alexishuf.fastersparql.emit.async;

import net.openhft.affinity.Affinity;
import oshi.SystemInfo;

import java.util.BitSet;

import static java.lang.Thread.currentThread;

public class AffinityHelper {
    private static final BitSet[] worker2Affinity;
    static {
        var processor = new SystemInfo().getHardware().getProcessor();
        var logical   = processor.getLogicalProcessors();
        var p2l       = new BitSet[processor.getPhysicalProcessorCount()];
        for (var p : logical) {
            int physId = p.getPhysicalProcessorNumber();
            var logicalProcessors = p2l[physId];
            if (logicalProcessors == null)
                p2l[physId] = logicalProcessors = new BitSet();
            logicalProcessors.set(p.getProcessorNumber());
        }
        var w2a = new BitSet[logical.size()];
        for (int worker = 0, phys = 0; worker < w2a.length; ) {
            // get logical cores contained within 4 physical cores
            var bs = new BitSet(logical.size());
            for (int physEnd = phys+4; phys < physEnd; ++phys)
                bs.or(p2l[phys%p2l.length]);
            int n = bs.cardinality();
            // map n workers to the n logical cores within the 4 physical cores
            for (int workerEnd = Math.min(w2a.length, worker+n); worker < workerEnd; ++worker)
                w2a[worker] = bs;
        }
        worker2Affinity = w2a;
    }

    /**
     * Set the CPU affinity mask of the current platform thread to a set of physical CPU cores
     * (including all logical cores hosted on the physical cores).
     * The set of physical cores is determined such that neighboring {@code workerId} values
     * are likely to map to the same affinity set.
     *
     * <p>In some platforms (Linux, but potentially others as well) affinity is not a mere
     * recommendation: The thread will only ever be assigned for execution at a CPU logical
     * core that is in its affinity mask.</p>
     *
     * @param workerId A sequential identifier of the worker thread. Neighboring {@code workerIds}
     *                 will be mapped to the same set of logical cores or to neighboring sets.
     */
    public static void setCurrentThreadPhysicalCoreAffinity(int workerId) {
        if (currentThread().isVirtual())
            throw new IllegalStateException("Called from a virtual thread");
        if (workerId < 0 || workerId >= worker2Affinity.length)
            workerId = (workerId&Integer.MAX_VALUE) % worker2Affinity.length;
        Affinity.setAffinity(worker2Affinity[workerId]);
    }
}
