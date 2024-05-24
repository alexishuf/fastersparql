package com.github.alexishuf.fastersparql.emit.async;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("unused")
class ParkedSet extends ParkedSet1 {
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
    private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;

    public ParkedSet(int nWorkers) {
        super(nWorkers);
    }
}

class ParkedSet1 extends ParkedSet0 {
    private static final VarHandle BS;
    static {
        try {
            BS = MethodHandles.lookup().findVarHandle(ParkedSet1.class, "plainBitset", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused") private long plainBitset;
    private final long all;

    public ParkedSet1(int nWorkers) {
        nWorkers = 1 << (32-Integer.numberOfLeadingZeros(nWorkers-1));
        nWorkers = Integer.min(64, nWorkers);
        all = (1L<<nWorkers)-1;
    }

    public boolean unparkAny(EmitterService.Worker[] workers) {
        long bs = plainBitset;
        for (long mask; (mask=bs&-bs) != 0; ) {
            if (((bs=(long)BS.getAndBitwiseAndRelease(this, ~mask))&mask) != 0) {
                LockSupport.unpark(workers[Long.numberOfTrailingZeros(mask)]);
                return true; // unparked a worker
            }
        }
        return false; // no parked worker
    }

    public void unparkAnyIfAllParked(EmitterService.Worker[] workers) {
        if (plainBitset == all && (long)BS.compareAndExchangeRelease(this, all, all &~0x1) == all)
            LockSupport.unpark(workers[0]);
    }

    public void park(int workerId) {
        long mask = 1L << workerId;
        BS.getAndBitwiseOrRelease(this, mask);
        LockSupport.park();
        BS.getAndBitwiseAndAcquire(this, ~mask);
    }


    @Override public String toString() {return Long.toHexString(plainBitset);}
}

@SuppressWarnings("unused") class ParkedSet0 {
    private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
    private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
}
