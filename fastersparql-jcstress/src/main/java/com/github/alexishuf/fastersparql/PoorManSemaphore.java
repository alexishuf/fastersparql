package com.github.alexishuf.fastersparql;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.invoke.MethodHandles.lookup;

@JCStressTest
@Outcome(id = "4", expect = Expect.ACCEPTABLE, desc = "Only allowed outcome.")
@Outcome(id = "[0-3]", expect = Expect.FORBIDDEN, desc = "Lost tokens.")
@Outcome(id = "[5-9]|1.+", expect = Expect.FORBIDDEN, desc = "Duplicated tokens.")
@State
public class PoorManSemaphore {
    private static final VarHandle TOKENS, CONSUMER;
    static {
        try {
            TOKENS = lookup().findVarHandle(PoorManSemaphore.class, "plainTokens", int.class);
            CONSUMER = lookup().findVarHandle(PoorManSemaphore.class, "plainConsumer", Thread.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused") private int plainTokens;
    @SuppressWarnings("unused") private Thread plainConsumer;

    @Actor public void producer() {
        for (int i = 0; i < 4; i++) {
            if ((int)TOKENS.getAndAddRelease(this, 1) == 0)
                LockSupport.unpark((Thread)CONSUMER.getOpaque(this));
        }
    }
    @Actor public void consumer(I_Result r) {
        CONSUMER.setRelease(this, Thread.currentThread());
        while (r.r1 < 4) {
            int n = (int) TOKENS.getAndSetAcquire(this, 0);
            if (n == 0) LockSupport.park();
            else        r.r1 += n;
        }
        r.r1 += (int) TOKENS.getAndSetAcquire(this, 0); // should take 0 tokens
    }
}
