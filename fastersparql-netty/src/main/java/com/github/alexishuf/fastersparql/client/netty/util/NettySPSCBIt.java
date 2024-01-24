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
     *
     * @return {@code true} if the implementation took full responsibility of performing the cancel
     */
    protected abstract boolean cancelAfterRequestSent();

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

    @Override public boolean complete(@Nullable Throwable error) {
        error = FSException.wrap(client.endpoint(), error);
        lock();
        try {
            final Channel ch = channel;
            if (state() == State.ACTIVE && ClientRetry.retry(++retries, error, this::request))
                return false; //will retry request
            if (ch != null) {
                if (error == null) ch.config().setAutoRead(true);
                else ch.close();
            }
            boolean first = state() == State.ACTIVE;
            super.complete(error);
            if (error == null && first)
                afterNormalComplete();
            channel = null;
            return true;
        } finally {
            unlock();
        }
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

    @Override public boolean cancel(boolean ack) {
        lock();
        try {
            if (state().isTerminated())
                return false;
            if (!ack && requestSent && cancelAfterRequestSent())
                return true; //cancelAfterRequestSent() sent a !cancel
            return super.cancel(ack);
        } finally { unlock(); }
    }

    @Override public String toString() {
        return "NettySPSC["+client.endpoint()+"]@"+id()+(channel == null ? "" : channel);
    }
}
