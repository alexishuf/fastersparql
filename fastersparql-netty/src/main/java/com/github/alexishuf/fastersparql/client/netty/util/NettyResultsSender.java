package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.EventExecutor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;

/**
 * A {@link ResultsSender} that executes all its actions inside a netty event loop.
 */
public abstract class NettyResultsSender<M> extends ResultsSender<ByteBufSink, ByteBuf>
                                            implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NettyResultsSender.class);
    private static final byte TOUCH_DISABLED = 0x00;
    private static final byte TOUCH_AUTO     = 0x01;
    private static final byte TOUCH_PARKED   = 0x02;
    private static final VarHandle LOCK;
    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(NettyResultsSender.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final EventExecutor executor;
    private final ChannelHandlerContext ctx;
    protected Thread owner;
    protected @Nullable Throwable error;
    private final Object[] actions = new Object[(128-16)>>2];
    private byte actionsHead = 0, actionsSize = 0;
    private byte touchState;
    protected boolean active;
    @SuppressWarnings("unused") private int plainLock;

    public static class NettyExecutionException extends RuntimeException {
        public NettyExecutionException(Throwable cause) {
            super(cause);
        }
    }

    public abstract static class Action {
        private final String name;
        public static final Action RELEASE = new ReleaseAction();

        public Action(String name) { this.name = name; }

        public abstract void run(NettyResultsSender<?> sender);

        @Override public String toString() { return name; }
    }

    private static class ReleaseAction extends Action {
        public ReleaseAction() {super("RELEASE");}
        @Override public void run(NettyResultsSender<?> sender) {
            sender.touchState = TOUCH_DISABLED;
            sender.sink.release();
        }
    }

    public NettyResultsSender(ResultsSerializer serializer, ChannelHandlerContext ctx) {
        super(serializer, new ByteBufSink(ctx.alloc()));
        this.executor = (this.ctx = ctx).executor();
        this.touchState = TOUCH_AUTO;
        this.owner = Thread.currentThread();
    }

    @Override public void close() {
        try {
            touchState = TOUCH_DISABLED;
            execute(Action.RELEASE);
        } catch (RejectedExecutionException e) {
            sink.release();
        }
    }

    @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {
        if (sink.needsTouch()) touch();
        try {
            serializer.init(vars, subset, isAsk, sink);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            execute(t);
        }
    }

    @Override public void sendSerialized(Batch<?> batch) {
        if (sink.needsTouch()) touch();
        try {
            serializer.serialize(batch, sink);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            execute(t);
        }
    }

    @Override public void sendSerialized(Batch<?> batch, int from, int nRows) {
        if (sink.needsTouch()) touch();
        try {
            serializer.serialize(batch, from, nRows, sink);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            execute(t);
        }
    }

    @Override public void sendTrailer() {
        if (sink.needsTouch()) touch();
        try {
            touchState = TOUCH_DISABLED;
            serializer.serializeTrailer(sink);
            execute(wrapLast(sink.take()));
        } catch (Throwable t) {
            execute(t);
        }
    }

    @Override public void sendError(Throwable t) {
        execute(t);
    }

    protected void lock() {
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
    }
    protected void unlock() {
        LOCK.setRelease(this, 0);
    }

    protected void execute(Object action) {
        boolean spawn;
        lock();
        try {
            if (actionsSize >= actions.length) waitCapacity();
            actions[(byte) (((int) actionsHead + actionsSize) % actions.length)] = action;
            actionsSize++;
            spawn = !active;
            if (spawn)
                active = true;
        } finally { unlock(); }
        if (spawn) executor.execute(this);
    }

    private void waitCapacity() {
        while (actionsSize == actions.length) {
            unlock();
            LockSupport.park(this);
            lock();
        }
    }

    protected void touch() {
        boolean spawn;
        lock();
        try {
            if (!sink.needsTouch()) return;
            touchState |= TOUCH_PARKED;
            error = null;
            spawn = !active;
            if (spawn)
                active = true;
        } finally { unlock(); }
        if (spawn)
            executor.execute(this);

        Throwable error;
        while (true) {
            lock();
            try {
                if ((error = this.error) != null || !sink.needsTouch()) {
                    this.error = null;
                    touchState &= ~TOUCH_PARKED;
                    break;
                }
            } finally { unlock(); }
        }
        if (error != null)
            throw new NettyExecutionException(error);
    }

    /**
     * Wrap {@code bb} into a protocol-specific wrapper, such as {@link HttpContent}
     * or {@link TextWebSocketFrame}
     *
     * @param bb the message contents. Ownership is taken by this method.
     * @return A protocol-specific message object wrapping {@code bb}
     */
    protected abstract M wrap(ByteBuf bb);

    /**
     * Similar to {@link #wrap(ByteBuf)}, but uses a wrapper specific for the last message
     * serializing a query results set.
     *
     * @param bb the message content. Ownership is taken by this method.
     * @return A protocol-specific message object wrapping {@code bb}
     */
    protected M wrapLast(ByteBuf bb) { return wrap(bb); }

    protected void beforeSend() { }
    protected void onError(Throwable t) {
        log.error("{}.run(): failed to run action", this, t);
    }

    private void doTouch() {
        try {
            sink.touch();
        } catch (Throwable t) {
            error = t;
            onError(t);
        } finally {
            if ((touchState & TOUCH_PARKED) != 0)
                LockSupport.unpark(owner);
        }
    }

    @Override public void run() {
        boolean exhausted = false, flush = false;
        Object action;
        for (int i = 0; i < 8; i++) {
            if (touchState != TOUCH_DISABLED && sink.needsTouch())
                doTouch();
            lock();
            try {
                exhausted = actionsSize == 0;
                if (exhausted) {
                    active = false;
                    break;
                }
                --actionsSize;
                action = actions[actionsHead];
                actionsHead = (byte) ((actionsHead + 1) % actions.length);
            } catch (Throwable t) {
                error = t;
                onError(t);
                continue;
            } finally {
                unlock();
                if (actionsSize == actions.length-1)
                    LockSupport.unpark(owner);
            }
            if (touchState != TOUCH_DISABLED && sink.needsTouch())
                doTouch();
            try {
                if (action instanceof Action a) {
                    a.run(this);
                } else if (action instanceof Throwable err) {
                    throw err;
                } else {
                    beforeSend();
                    ctx.write(action);
                    flush = true;
                }
            } catch (Throwable t) {
                onError(t);
            }
        }
        if (flush)
            ctx.flush(); // send messages over the wire
        if (!exhausted)
            executor.execute(this); // not all actions processed, do netty work before continuing
    }
}
