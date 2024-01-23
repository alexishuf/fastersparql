package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.RequestAwareCompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.util.ClientRetry;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.Vars;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;


public abstract class NettySPSCBIt<B extends Batch<B>> extends SPSCBIt<B>
        implements ChannelBound, RequestAwareCompletableBatchQueue<B> {
    private int retries;
    protected final SparqlClient client;
    protected @MonotonicNonNull Channel channel;
    private boolean backPressured, requestSent;

    public NettySPSCBIt(BatchType<B> batchType, Vars vars, int maxBatches,
                        SparqlClient client) {
        super(batchType, vars, maxBatches);
        this.client = client;
    }

    protected abstract void request();
    protected void afterNormalComplete() {}

    /**
     * Cancel a query after the WS frame or HTTP request has already been sent. This can be
     * implemented by sending a {@code !cancel } or closing the {@link Channel}.
     */
    protected abstract void cancelAfterRequestSent();

    /* --- --- --- RequestAwareCompletableBatchQueue --- --- --- */

    @Override public void lockRequest() { lock(); }
    @Override public void unlockRequest() { unlock(); }

    @Override public boolean canSendRequest() {
        if (!ownsLock())
            throw new IllegalStateException("not locked");
        if (state().isTerminated())
            return false;
        requestSent = true;
        return true;
    }

    /* --- --- --- ChannelBound --- --- --- */

    @Override public @Nullable Channel channel() { return channel; }

    /* --- --- --- SPSCBIt --- --- --- */

    @Override public void complete(@Nullable Throwable error) {
        final Channel ch = channel;
        EventLoop el;
        if (ch != null && !(el=ch.eventLoop()).inEventLoop()) {
            Throwable finalError = error;
            el.execute(() -> complete(finalError));
            return;
        }
        error = FSException.wrap(client.endpoint(), error);
        if (state() == State.ACTIVE && ClientRetry.retry(++retries, error, this::request))
            return; //will retry request
        if (ch != null) {
            if (error == null) ch.config().setAutoRead(true);
            else ch.close();
        }
        boolean first = state() == State.ACTIVE;
        super.complete(error);
        if (error == null && first)
            afterNormalComplete();
        channel = null;
    }

    @Override protected boolean mustPark(int offerRows, int queuedRows) {
        if (super.mustPark(offerRows, queuedRows)) {
            Channel ch = this.channel;
            assert ch == null || ch.eventLoop().inEventLoop() : "offer() outside channel event loop";
            if (!backPressured && ch != null) {
                backPressured = true;
                ch.config().setAutoRead(false);
            }
        }
        return false;
    }

    @Override public @Nullable B nextBatch(@Nullable B b) {
        b = super.nextBatch(b);
        if (backPressured) {
            final Channel ch = channel;
            if (ch != null)
                ch.config().setAutoRead(true);
            backPressured = false;
        }
        return b;
    }

    @Override public void close() {
        lock();
        try {
            if (!state().isTerminated() && requestSent)
                cancelAfterRequestSent();
            super.close();
        } finally { unlock(); }
    }

    @Override public void cancel() {
        lock();
        try {
            if (!state().isTerminated() && requestSent)
                cancelAfterRequestSent();
            super.cancel();
        } finally { unlock(); }
    }

    @Override public String toString() {
        return "NettySPSC["+client.endpoint()+"]@"+id()+(channel == null ? "" : channel);
    }
}
