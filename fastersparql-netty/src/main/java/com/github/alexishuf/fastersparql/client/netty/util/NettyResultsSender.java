package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.EventExecutor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;

/**
 * A {@link ResultsSender} that executes all its actions inside a netty event loop.
 */
public abstract class NettyResultsSender<M> extends ResultsSender<ByteBufSink, ByteBuf>
                                            implements ChannelBound, Runnable {
    private static final Logger log = LoggerFactory.getLogger(NettyResultsSender.class);
    private static final VarHandle LOCK;
    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(NettyResultsSender.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final EventExecutor executor;
    protected final ChannelHandlerContext ctx;
    protected @Nullable Throwable error;
    private Object[] actions;
    private byte actionsHead = 0, actionsSize = 0;
    private boolean autoTouch;
    protected boolean active;
    private volatile boolean termSent;
    @SuppressWarnings("unused") private int plainLock;
    protected @Nullable Thread touchParked, capacityParked;
    private final Object[] instanceActions;
    private Object @Nullable[] sharedActions;

    public static class NettyExecutionException extends RuntimeException {
        public NettyExecutionException(Throwable cause) {
            super(cause);
        }
    }

    public abstract static class Action {
        private final String name;
        public static final Action RELEASE_SINK = new ReleaseSinkAction();

        public Action(String name) { this.name = name; }

        public abstract void run(NettyResultsSender<?> sender);

        @Override public String toString() { return name; }
    }

    private static class ReleaseSinkAction extends Action {
        private ReleaseSinkAction() {super("RELEASE");}
        @Override public void run(NettyResultsSender<?> sender) {
            sender.disableAutoTouch();
            sender.sink.release();
        }
    }

    public NettyResultsSender(ResultsSerializer serializer, ChannelHandlerContext ctx) {
        super(serializer, new ByteBufSink(ctx.alloc()));
        Object[] actions = new Object[128>>2];
        this.actions          = actions;
        this.instanceActions  = actions;
        this.executor         = (this.ctx = ctx).executor();
        this.autoTouch = true;
    }

    @Override public @Nullable Channel channel() { return ctx.channel(); }
    @Override public void setChannel(Channel ch) { throw new UnsupportedOperationException(); }
    @Override public String        journalName() { return "NRS:"+ctx.channel().id().asShortText(); }

    @Override public String toString() {
        return getClass().getSimpleName()+':'+ctx.channel().id().asShortText()
                                         +'@'+Integer.toHexString(System.identityHashCode(this));
    }

    @Override public void close() {
        disableAutoTouch();
        if (shouldScheduleRelease())
            execute(Action.RELEASE_SINK); // will also lead to recycleSharedActions()
        else if (sharedActions != null)
            recycleSharedActions();
    }

    protected void closeFromEventLoop() {
        disableAutoTouch();
        sink.release();
    }

    private void copyActions(Object[] dst) {
        int n0 = Math.min(actionsSize, actions.length-actionsHead);
        arraycopy(actions, actionsHead, dst, 0, n0);
        int n1 = actionsSize-n0;
        if (n1 > 0)
            arraycopy(actions, 0, dst, n0, n1);
    }

    private void recycleSharedActions() {
        lock();
        try {
            if (sharedActions == null || actionsSize > instanceActions.length)
                return;
            if (actionsSize > 0)
                copyActions(instanceActions);
            Arrays.fill(sharedActions, null);
            actionsHead   = 0;
            actions       = instanceActions;
            sharedActions = ArrayPool.OBJECT.offerToNearest(sharedActions, sharedActions.length);
        } finally { unlock(); }
    }

    protected void sendingTerminal() {
        assert !termSent : "terminal already sent";
        termSent  = true;
    }

    protected void disableAutoTouch() {
        lock();
        try {
            autoTouch = false;
        } finally { unlock(); }
    }

    protected void thaw() {
        lock();
        try {
            termSent  = false;
            autoTouch = true;
        } finally { unlockAndSpawn(); }
    }

    @Override public void preTouch() {
        if (!sink.needsTouch() || !autoTouch || active)
            return;
        lock();
        unlockAndSpawn();
    }

    @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {
        preTouch();
        try {
            serializer.init(vars, subset, isAsk);
            touch();
            serializer.serializeHeader(sink);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            log.error("Failed to init results sender", t);
            execute(t);
        }
    }

    @Override public void sendSerializedAll(Batch<?> batch) {
        assert !termSent : "sending batch after sendCancel()";
        if (actionsSize == actions.length && !ctx.channel().isActive()) {
            log.error("channel is closed, {} will not serialize", this);
            return;
        }
        if (sink.needsTouch()) touch();
        try {
            serializer.serializeAll(batch, sink);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            log.error("Failed to serialize batch", t);
            execute(t);
        }
    }

    @Override
    public <B extends Batch<B>> void sendSerializedAll(B batch, ResultsSerializer.SerializedNodeConsumer<B> nodeConsumer) {
        assert !termSent : "sending batch after sendCancel()";
        if (actionsSize == actions.length && !ctx.channel().isActive()) {
            log.error("channel is closed, {} will not serialize", this);
            return;
        }
        if (sink.needsTouch()) sink.touch();
        try {
            serializer.serializeAll(batch, sink, nodeConsumer);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            log.error("Failed to serialize batch", t);
            execute(t);
        }
    }

    @Override public void sendSerialized(Batch<?> batch, int from, int nRows) {
        assert !termSent : "sending batch after sendCancel()";
        if (actionsSize == actions.length && !ctx.channel().isActive()) {
            log.error("channel is closed, {} will not serialize", this);
            return;
        }
        if (sink.needsTouch()) touch();
        try {
            serializer.serialize(batch, from, nRows, sink);
            execute(wrap(sink.take()));
        } catch (Throwable t) {
            log.error("Failed to serialize batch", t);
            execute(t);
        }
    }

    @Override public void sendTrailer() {
        sendingTerminal();
        if (sink.needsTouch()) touch();
        try {
            serializer.serializeTrailer(sink);
            M msg = wrapLast(sink.take());
            disableAutoTouch();
            execute(msg, Action.RELEASE_SINK);
        } catch (Throwable t) {
            log.error("Failed to serialize trailer", t);
            execute(t);
        }
    }

    @Override public void sendError(Throwable t) {
        sendingTerminal();
        disableAutoTouch();
        execute(t, Action.RELEASE_SINK);
    }

    @Override public void waitCanSend() {
        lock();
        try {
            assert capacityParked == null : "concurrent usage";
            Thread thread = currentThread();
            capacityParked = thread;
            while (actionsSize >= actions.length) {
                unlockAndSpawn();
                LockSupport.park(this);
                lock();
            }
            if (capacityParked == thread)
                capacityParked = null;
        } finally { unlock(); }
    }

    protected void lock() {
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
    }
    protected void unlock() {
        LOCK.setRelease(this, 0);
    }
    private void unlockAndSpawn() {
        boolean spawn = !active;
        if (spawn) active = true;
        unlock();
        if (spawn) {
            try {
                executor.execute(this);
            } catch (RejectedExecutionException e) {
                onRejectedExecution();
            }
        }
    }

    protected void execute(Object action) {
        lock();
        try {
            if (actionsSize >= actions.length) growCapacity();
            actions[(byte) ((actionsHead + actionsSize++) % actions.length)] = action;
        } finally { unlockAndSpawn(); }
    }

    protected void execute(Object action0, Object action1) {
        lock();
        try {
            if (actionsSize+2 > actions.length) growCapacity();
            actions[(byte) ((actionsHead+actionsSize++) % actions.length)] = action0;
            actions[(byte) ((actionsHead+actionsSize++) % actions.length)] = action1;
        } finally { unlockAndSpawn(); }
    }

    private void onRejectedExecution() {
        int nActions = 0, nReleases = 0;
        lock();
        try {
            active = false;
            while (actionsSize > 0) {
                --actionsSize;
                Object a = actions[actionsHead];
                if      (a instanceof Action           ac) { ac.run(this); ++nActions;  }
                else if (a instanceof ReferenceCounted rc) { rc.release(); ++nReleases; }
                actionsHead = (byte)((actionsHead + 1) % actions.length);
            }
            if (touchParked != null && sink.needsTouch() && touchParked == currentThread())
                sink.touch();
        } finally { unlock(); }
        log.debug("{} rejected {}. Ran {} actions, released {} netty objects",
                  executor, this, nActions, nReleases);
    }

    private void growCapacity() {
        journal("growing actions from", actions.length, "sender=", this);
        int required = actions.length<<1;
        if (required > Short.MAX_VALUE)
            throw new IllegalStateException("actions overflow");
        var bigger = ArrayPool.objectsAtLeast(required);
        copyActions(bigger);
        if (sharedActions != null) {
            Arrays.fill(sharedActions, null);
            ArrayPool.OBJECT.offerToNearest(sharedActions, sharedActions.length);
        }
        sharedActions = bigger;
        actions       = bigger;
        actionsHead   = 0;
    }

    protected void touch() {
        if (executor.inEventLoop()) {
            sink.touch();
            return;
        }
        Thread thread;
        lock();
        try {
            if (!sink.needsTouch()) return;
            assert touchParked == null : "concurrent usage";
            thread = currentThread();
            touchParked = thread;
            error = null;
        } finally { unlockAndSpawn(); }

        Throwable error;
        for (int i = 0; true; ++i) {
            lock();
            try {
                if ((error = this.error) != null || !sink.needsTouch()) {
                    this.error = null;
                    if (touchParked == thread)
                        touchParked = null;
                    break;
                }
            } finally { unlock(); }
            if      ((i&7) ==  7) Thread.yield();
            else if (i     > 255) LockSupport.park(this);
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

    protected boolean beforeSend() { return true; }
    protected void onError(Throwable t) {
        journal("onError", t, "sender=", this);
        log.error("{}.run(): failed to run action", this, t);
    }

    protected boolean shouldScheduleRelease() {
        return active || !sink.needsTouch();
    }

    private void doTouch() {
        try {
            sink.touch();
        } catch (Throwable t) {
            error = t;
            onError(t);
        } finally {
            if (touchParked != null)
                Unparker.unpark(touchParked);
        }
    }

    @Override public void run() {
        boolean exhausted = false, flush = false;
        try {
            Object action;
            for (int i = 0; i < 8 || executor.isShuttingDown(); i++) {
                // speculative touch
                if (touchParked != null || (autoTouch && sink.needsTouch()))
                    doTouch();
                // take next action
                lock();
                try {
                    exhausted = actionsSize == 0;
                    if (exhausted) {
                        active = false;
                        break;
                    }
                    --actionsSize;
                    action = actions[actionsHead];
                    actionsHead = (byte) ((actionsHead+1) % actions.length);
                    if (capacityParked != null)
                        Unparker.unpark(capacityParked);
                } catch (Throwable t) {
                    error = t;
                    onError(t);
                    continue;
                } finally {
                    unlock();
                }
                // speculative touch
                if (touchParked != null || (autoTouch && sink.needsTouch()))
                    doTouch();
                // execute action
                ReferenceCounted refCntAction = null;
                try {
                    if (action instanceof Action a) {
                        a.run(this);
                    } else if (action instanceof Throwable err) {
                        onError(err);
                    } else {
                        refCntAction = action instanceof ReferenceCounted r ? r : null;
                        if (beforeSend()) {
                            refCntAction = null;
                            ctx.write(action);
                            flush = true;
                        }
                    }
                } catch (Throwable t) {
                    onError(t);
                } finally {
                    if (refCntAction != null)
                        refCntAction.release();
                }
            }
            if (flush)
                ctx.flush(); // send messages over the wire
            if (!exhausted) {
                try {
                    executor.execute(this);
                } catch (RejectedExecutionException e) {
                    run(); // executor shutting down, cannot continue later
                }
            }
        } finally {
            // release the rare speculative ByteBuf that should not have been allocated
            lock();
            if (!autoTouch && touchParked == null)
                sink.release();
            unlock();
            if (exhausted)
                recycleSharedActions();
        }
    }
}
