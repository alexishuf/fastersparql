package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.RequestAwareCompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.async.CallbackEmitter;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import io.netty.channel.Channel;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.client.util.ClientRetry.retry;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public abstract class NettyCallbackEmitter<B extends Batch<B>> extends CallbackEmitter<B>
        implements ChannelBound, RequestAwareCompletableBatchQueue<B> {
    private   static final int NO_AUTO_READ  = 0x40000000;
    protected static final int STARTED       = 0x20000000;
    protected static final int RETRIES_MASK  = 0x1f000000;
    protected static final int RETRIES_ONE   = 0x01000000;
    protected static final int CANCEL_CALLED = 0x00800000;
    protected static final int REQUEST_SENT  = 0x00400000;
    protected static final int REBIND_CLEAR = STARTED|RETRIES_MASK|CANCEL_CALLED|REQUEST_SENT;
    protected static final Stateful.Flags FLAGS = TASK_FLAGS.toBuilder()
            .flag(NO_AUTO_READ,    "NO_AUTO_READ")
            .flag(STARTED,         "STARTED")
            .flag(CANCEL_CALLED,   "CANCEL_CALLED")
            .flag(REQUEST_SENT,    "REQUEST_SENT")
            .counter(RETRIES_MASK, "retries")
            .build();

    protected @MonotonicNonNull Channel channel;
    private final Runnable autoReadSetter = this::setAutoRead0;
    protected final SparqlClient client;

    public NettyCallbackEmitter(BatchType<B> batchType, Vars vars, SparqlClient client) {
        super(batchType, vars, EMITTER_SVC, RR_WORKER, CREATED, FLAGS);
        assert flags.contains(FLAGS) : "bad Flags";
        this.client = client;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    /* --- --- --- ChannelBound --- --- --- */

    @Override public @Nullable Channel channel() { return channel; }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder().append(journalName());
        if (type != StreamNodeDOT.Label.MINIMAL)
            appendToSimpleLabel(sb);
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state())).append(", requested=");
            StreamNodeDOT.appendRequested(sb, requested());
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override protected void appendToSimpleLabel(StringBuilder out) {
        out.append(" ch=").append(channel);
        String u = client.endpoint().uri();
        if (u.startsWith("file:///"))
            u = u.replaceFirst("^.*/", "");
        out.append('\n').append(u);
    }

    @Override public final String toString() {
        return "NettyCallbackEmitter["+client.endpoint()+"]"+channel;
    }

    /* --- --- --- helper methods --- --- --- */

    private void setAutoRead0() {
        var ch = channel;
        if (ch != null) {
            var cfg = ch.config();
            boolean autoRead = (state()&NO_AUTO_READ) == 0;
            if (cfg.isAutoRead() != autoRead)
                cfg.setAutoRead(autoRead);
        }
    }

    private void setAutoRead(boolean value) {
        int state = statePlain();
        if (value) clearFlagsRelease(state, NO_AUTO_READ);
        else       setFlagsRelease  (state, NO_AUTO_READ);
        Channel ch = this.channel;
        if (ch != null)
            ch.eventLoop().execute(autoReadSetter);
    }

    protected void setChannel(Channel channel) {
        if (this.channel == channel)
            return;
        this.channel = channel;
        int st = state();
        if ((st&IS_CANCEL_REQ) != 0 || isCancelled(st))
            channel.close();
    }

    /* --- --- --- abstract methods --- --- --- */

    protected abstract void request();

    /**
     * Handles a {@link #cancel()} after the request WS frame or HTTP request has already
     * been sent to the server.
     *
     * @return {@code true} iff this implementation fully handled the cancel and
     *         {@link TaskEmitter#cancel()}  must not be called.
     */
    protected abstract boolean cancelAfterRequestSent();

    /* --- --- --- CallbackEmitter --- --- --- */

    @Override protected int complete2state(int current, @Nullable Throwable cause) {
        cause = cause == CancelledException.INSTANCE ? cause
                : FSException.wrap(client.endpoint(), cause);
        int state = state();
        if ((state&IS_LIVE) != 0) {
            int retries = addToCounterRelease(state, RETRIES_MASK, RETRIES_ONE);
            if (retry(retries, cause, this::request))
                return 0; // do not complete
        }
        return super.complete2state(current, cause);
    }

    @Override public void  pause() { setAutoRead(false); }
    @Override public void resume() {
        setAutoRead(true);
        if (compareAndSetFlagRelease(STARTED))
            request();
    }

    @Override public void cancel() {
        int st = lock(statePlain());
        try {
            st = setFlagsRelease(st, CANCEL_CALLED);
            if ((st&(IS_CANCEL_REQ|IS_TERM|REQUEST_SENT)) == REQUEST_SENT) {
                if (cancelAfterRequestSent())
                    return; // do not do super.cancel() if implementation handles it
            }
            assert (st&REQUEST_SENT) == 0 : "expected !REQUEST_SENT";
            // else: terminated or will ABORT_REQUEST at onConnected()
        } finally { unlock(st); }
        super.cancel();
    }

    /* --- --- --- RequestAwareCompletableBatchQueue --- --- --- */

    @Override public void   lockRequest() {   lock(statePlain()); }
    @Override public void unlockRequest() { unlock(statePlain()); }

    @Override public boolean canSendRequest() {
        int st = statePlain();
        if ((st&CANCEL_CALLED) != 0)
            return false;
        setFlagsRelease(st, REQUEST_SENT);
        return true;
    }
}
