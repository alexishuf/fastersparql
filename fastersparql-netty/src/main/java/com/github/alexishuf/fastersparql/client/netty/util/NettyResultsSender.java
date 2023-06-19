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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;

/**
 * A {@link ResultsSender} that executes all its actions inside a netty event loop.
 */
public abstract class NettyResultsSender<M> extends ResultsSender<ByteBufSink, ByteBuf>
                                            implements Runnable {
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
    protected @Nullable Object result;
    private final Object[] actions = new Object[(128-16)>>2];
    private byte actionsHead = 0, actionsSize = 0;
    protected boolean active, autoTouch = true;
    @SuppressWarnings("unused") private int plainLock;

    public static class NettyExecutionException extends RuntimeException {
        public NettyExecutionException(Throwable cause) {
            super(cause);
        }
    }

    public abstract static class Action {
        private final String name;
        private final boolean isSync;
        public static final Action TOUCH = new TouchAction();
        public static final Action RELEASE = new ReleaseAction();

        public Action(String name, boolean isSync) {
            this.name = name;
            this.isSync = isSync;
        }

        public abstract void run(NettyResultsSender<?> sender);

        @Override public String toString() { return name; }
    }

    private static class TouchAction extends Action {
        public TouchAction() {super("TOUCH", true);}
        @Override public void run(NettyResultsSender<?> sender) {sender.sink.touch();}
    }

    private static class ReleaseAction extends Action {
        public ReleaseAction() {super("RELEASE", false);}
        @Override public void run(NettyResultsSender<?> sender) {sender.sink.release();}
    }

    public NettyResultsSender(ResultsSerializer serializer, ChannelHandlerContext ctx) {
        super(serializer, new ByteBufSink(ctx.alloc()));
        this.executor = (this.ctx = ctx).executor();
        owner = Thread.currentThread();
    }

    @Override public void close() {
        try {
            execute(Action.RELEASE);
        } catch (RejectedExecutionException e) {
            sink.release();
        }
    }

    @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {
        autoTouch = true;
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
            autoTouch = false;
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
        boolean spawn, sync = action instanceof Action a && a.isSync;
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
        if (sync) sync();
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
            if (actionsSize == actions.length) waitCapacity();
            ++actionsSize;
            actionsHead = (byte) ((actionsHead == 0 ? actions.length : actionsHead)-1);
            actions[actionsHead] = Action.TOUCH;
            spawn = !active;
            if (spawn)
                active = true;
        } finally { unlock(); }
        if (spawn)
            executor.execute(this);
        sync();
    }

    private void sync() {
        Object result;
        while (true) {
            lock();
            try {
                if ((result = this.result) != null) break;
            } finally { unlock(); }
        }
        this.result = null;
        if (result instanceof Throwable t)
            throw new NettyExecutionException(t);
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
    protected void onError(Throwable t) { }

    @Override public void run() {
        boolean exhausted = false, flush = false;
        Object action;
        for (int i = 0; i < 8; i++) {
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
            } finally { unlock(); }
            boolean sync = action instanceof Action a && a.isSync;
            try {
                if (action instanceof Action a) {
                    a.run(this);
                } else if (action instanceof Throwable err) {
                    throw err;
                } else {
                    if (autoTouch && sink.needsTouch())
                        sink.touch();
                    ctx.write(action);
                    beforeSend();
                    flush = true;
                }
                if (sync)
                    result = action;
            } catch (Throwable t) {
                if (sync)
                    result = t;
                onError(t);
            } finally {
                if (sync || actionsSize == actions.length-1)
                    LockSupport.unpark(owner);
            }
        }
        if (flush)
            ctx.flush(); // send messages over the wire
        if (!exhausted)
            executor.execute(this); // not all actions processed, do netty work before continuing
    }
}
