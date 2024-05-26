package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.FastAliveSet;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnusedReturnValue")
public class ActiveChannelSet implements SafeCloseable {
    private static final Logger log = LoggerFactory.getLogger(ActiveChannelSet.class);
    private static final VarHandle CLOSED;
    static {
        try {
            CLOSED = MethodHandles.lookup().findVarHandle(ActiveChannelSet.class, "plainClosed", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String name;
    @SuppressWarnings("unused") private boolean plainClosed;
    private final FastAliveSet<Channel> open = new FastAliveSet<>(256);
    private final FastAliveSet<Channel> active = new FastAliveSet<>(128);

    public ActiveChannelSet(String name) { this.name = name; }

    public synchronized ActiveChannelSet add(Channel ch) {
        open.add(ch);
        ch.closeFuture().addListener(ignored -> remove(ch));
        return this;
    }

    public synchronized Channel   setActive(Channel ch) { active.add(ch); return ch; }
    public synchronized Channel setInactive(Channel ch) { active.remove(ch);  return ch; }
    public synchronized void         remove(Channel ch) { open.remove(setInactive(ch)); }

    @Override public void close() {
        if ((boolean)CLOSED.compareAndExchangeRelease(this, false, true))
            return; // already closed
        Semaphore activeClosed = new Semaphore(0);
        GenericFutureListener<Future<? super Void>> onClose = ignore -> activeClosed.release();
        int[] state = {0, 0};
        active.destruct(ch -> {
            ++state[0];
            if (state[1]++ == 0)
                log.info("Closing active channels on {}.close()", this);
            log.debug("Closing {}", ch);
            try {
                ch.close().addListener(onClose);
            } catch (Throwable t) {
                log.error("Unexpected {} from {}.close()", t.getClass().getSimpleName(), ch, t);
            }
        });
        open.destruct(ch -> {
            try {
                ch.close();
            } catch (Throwable t) {
                log.debug("Unexpected {} from {}.close()", t.getClass().getSimpleName(), ch, t);
            }
        });
        if (state[0] <= 0)
            return;
        try {
            log.warn("Waiting at most 10s for {} active channels to close at {}.close()",
                    state[0], this);
            if (!activeClosed.tryAcquire(state[0], 10, TimeUnit.SECONDS)) {
                int pending = state[0] - activeClosed.availablePermits();
                log.warn("Abandoning {} channels still not closed at {}.close()", pending, this);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for closing of {} active Channels at {}.close()",
                    state[0], this);
            Thread.currentThread().interrupt();
        }
    }

    @Override public String toString() {
        return "ActiveChannelSet["+name+"]";
    }
}
