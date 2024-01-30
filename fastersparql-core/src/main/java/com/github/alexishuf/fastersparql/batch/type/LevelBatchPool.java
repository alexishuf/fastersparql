package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.LeakyPool;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.lang.Thread.onSpinWait;

public class LevelBatchPool<B extends Batch<B>> implements LeakyPool {
    private static final VarHandle I = MethodHandles.arrayElementVarHandle(int[].class);

    private static final int CTRL_WIDTH      = 16;
    private static final int SMALL_CTRL_BASE   = 0;
    private static final int LARGE_CTRL_BASE   =   CTRL_WIDTH;
    private static final int HUGE_CTRL_BASE    = 2*CTRL_WIDTH;

    private static final int SMALL_CAP   = 1024;
    private static final int LARGE_CAP   = 512;
    private static final int HUGE_CAP    = 128;

    private static final int FREE_OFF  = 1;
    private static final int BEGIN_OFF = 2;
    private static final int END_OFF   = 3;

    public interface Factory<B extends Batch<B>> { B create(int terms); }

    private final int[] ctrl;
    private final B[] pool;
    private final short generalHi, smallHi;
    private final int largeHi, hugeHi;
    private final Factory<B> factory;
    private final BatchPool<B> generalPool;

    @SuppressWarnings("unchecked")
    public LevelBatchPool(Factory<B> factory, BatchPool<B> generalPool, short generalPoolCapacity) {
        this.generalHi   = generalPoolCapacity;
        this.smallHi     = (short)Math.min(Short.MAX_VALUE, generalPoolCapacity<<1);
        this.largeHi     = generalPoolCapacity<<2;
        this.hugeHi      = generalPoolCapacity<<4;
        this.factory     = factory;
        this.generalPool = generalPool;
        this.ctrl = new int[CTRL_WIDTH*3];
        this.pool        = (B[])new Batch[SMALL_CAP + LARGE_CAP + HUGE_CAP];

        ctrl[SMALL_CTRL_BASE+BEGIN_OFF] = ctrl[SMALL_CTRL_BASE+FREE_OFF] = 0;
        ctrl[LARGE_CTRL_BASE+BEGIN_OFF] = ctrl[LARGE_CTRL_BASE+FREE_OFF] = SMALL_CAP;
        ctrl[HUGE_CTRL_BASE +BEGIN_OFF] = ctrl[HUGE_CTRL_BASE +FREE_OFF] = SMALL_CAP+LARGE_CAP;
        ctrl[SMALL_CTRL_BASE+END_OFF  ] = ctrl[SMALL_CTRL_BASE+BEGIN_OFF]+SMALL_CAP;
        ctrl[LARGE_CTRL_BASE+END_OFF  ] = ctrl[LARGE_CTRL_BASE+BEGIN_OFF]+LARGE_CAP;
        ctrl[HUGE_CTRL_BASE +END_OFF  ] = ctrl[HUGE_CTRL_BASE +BEGIN_OFF]+HUGE_CAP;
    }

    public B get(int terms) {
        B b = null;
        if (terms <= generalHi) {
            return generalPool.get();
        } else if (terms <= hugeHi) {
            int ctrlBase;
            if      (terms <=   smallHi) ctrlBase = SMALL_CTRL_BASE;
            else if (terms <=   largeHi) ctrlBase = LARGE_CTRL_BASE;
            else                         ctrlBase = HUGE_CTRL_BASE;
            b = getFromStack(ctrlBase);
        }
        return b == null ? factory.create(terms) : b;
    }

    private B getFromStack(int ctrlBase) {
        B b = null;
        int beginIdx = ctrl[ctrlBase+BEGIN_OFF], freeIdx;
        while ((int)I.compareAndExchangeAcquire(ctrl, ctrlBase, 0, 1) != 0) onSpinWait();
        try {
            freeIdx = ctrl[ctrlBase+FREE_OFF]-1;
            if (freeIdx >= beginIdx) {
                b = pool[freeIdx];
                ctrl[ctrlBase+FREE_OFF] = freeIdx;
            }
        } finally {
            I.setRelease(ctrl, ctrlBase, 0);
        }
        return b;
    }

    public @Nullable B offer(B b) {
        int ctrlBase, cap = b.termsCapacity();
        if      (cap == generalHi) return generalPool.offer(b);
        else if (cap ==   smallHi) ctrlBase = SMALL_CTRL_BASE;
        else if (cap ==   largeHi) ctrlBase = LARGE_CTRL_BASE;
        else if (cap ==    hugeHi) ctrlBase = HUGE_CTRL_BASE;
        else                       return b;
        return offerToStack(ctrlBase, b);
    }

    private @Nullable B offerToStack(int ctrlBase, B b) {
        int endIdx = ctrl[ctrlBase+END_OFF], freeIdx;
        while ((int)I.compareAndExchangeAcquire(ctrl, ctrlBase, 0, 1) != 0) onSpinWait();
        try {
            freeIdx = ctrl[ctrlBase+FREE_OFF];
            if (freeIdx < endIdx) {
                pool[freeIdx] = b;
                b = null;
                ctrl[ctrlBase+FREE_OFF] = freeIdx+1;
            }
        } finally {
            I.setRelease(ctrl, ctrlBase, 0);
        }
        return b;
    }

    @Override public void cleanLeakyRefs() {
        for (int ctrlBase = 0; ctrlBase < ctrl.length; ctrlBase += CTRL_WIDTH) {
            int mid = ctrl[ctrlBase+FREE_OFF], end = ctrl[ctrlBase+END_OFF];
            while (mid < end && pool[mid] != null) ++mid;
            while ((int)I.compareAndExchangeAcquire(ctrl, ctrlBase, 0, 1) != 0) onSpinWait();
            try {
                for (int i = ctrl[ctrlBase+FREE_OFF] ; i < mid; ++i)
                    pool[i] = null;
            } finally {
                I.setRelease(ctrl, ctrlBase, 0);
            }
        }
    }
}
