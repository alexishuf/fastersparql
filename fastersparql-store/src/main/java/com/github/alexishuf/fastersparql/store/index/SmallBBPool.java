package com.github.alexishuf.fastersparql.store.index;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

public class SmallBBPool {
    private static final int SMALL_SIZE = 16;
    private static final int CHUNK_SIZE = 2048;
    private static final VarHandle S0, S1, S2, S3, C0, C1;
    static {
        try {
            S0 = MethodHandles.lookup().findStaticVarHandle(SmallBBPool.class, "plainSmall0", ByteBuffer.class);
            S1 = MethodHandles.lookup().findStaticVarHandle(SmallBBPool.class, "plainSmall1", ByteBuffer.class);
            S2 = MethodHandles.lookup().findStaticVarHandle(SmallBBPool.class, "plainSmall2", ByteBuffer.class);
            S3 = MethodHandles.lookup().findStaticVarHandle(SmallBBPool.class, "plainSmall3", ByteBuffer.class);
            C0 = MethodHandles.lookup().findStaticVarHandle(SmallBBPool.class, "plainChunk0", ByteBuffer.class);
            C1 = MethodHandles.lookup().findStaticVarHandle(SmallBBPool.class, "plainChunk1", ByteBuffer.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private static ByteBuffer plainSmall0, plainSmall1, plainSmall2, plainSmall3;
    @SuppressWarnings("unused") private static ByteBuffer plainChunk0, plainChunk1;

    public static ByteBuffer smallDirectBB() {
        ByteBuffer bb;
        if ((bb = (ByteBuffer) S0.getAndSetAcquire((ByteBuffer)null)) == null) {
            if ((bb = (ByteBuffer) S1.getAndSetAcquire((ByteBuffer)null)) == null) {
                if ((bb = (ByteBuffer) S2.getAndSetAcquire((ByteBuffer)null)) == null) {
                    if ((bb = (ByteBuffer) S3.getAndSetAcquire((ByteBuffer)null)) == null)
                        bb = ByteBuffer.allocateDirect(SMALL_SIZE);
                }
            }
        }
        return bb.clear();
    }

    public static void releaseSmallDirectBB(ByteBuffer bb) {
        if (S0.compareAndExchangeRelease(null, bb) == null) return;
        if (S1.compareAndExchangeRelease(null, bb) == null) return;
        if (S2.compareAndExchangeRelease(null, bb) == null) return;
        S3.compareAndExchangeRelease(null, bb);
    }

    public static ByteBuffer chunkDirectBB() {
        var bb = (ByteBuffer)C0.getAndSetAcquire((ByteBuffer)null);
        if (bb != null) return bb;
        bb = (ByteBuffer) C1.getAndSetAcquire((ByteBuffer)null);
        return bb == null ? ByteBuffer.allocateDirect(CHUNK_SIZE) : bb;
    }

    public static void releaseChunkDirectBB(ByteBuffer bb) {
        if (C0.compareAndExchangeRelease(null, bb) != null)
            C1.compareAndExchangeRelease(null, bb);
    }
}
