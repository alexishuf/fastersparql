package com.github.alexishuf.fastersparql.client.netty.util;

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


public abstract class NettySPSCBIt<B extends Batch<B>> extends SPSCBIt<B> implements ChannelBound {
    private int retries;
    protected final SparqlClient client;
    protected @MonotonicNonNull Channel channel;
    private boolean backPressured;

    public NettySPSCBIt(BatchType<B> batchType, Vars vars, int maxBatches,
                        SparqlClient client) {
        super(batchType, vars, maxBatches);
        this.client = client;
    }

    protected abstract void request();
    protected void afterNormalComplete() {}

    @Override public @Nullable Channel channel() { return channel; }

    @Override public void complete(@Nullable Throwable error) {
        final Channel ch = channel;
        assert ch == null || ch.eventLoop().inEventLoop() : "complete() outside event loop";
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

    @Override public String toString() {
        return "NettySPSC["+client.endpoint()+"]@"+id()+(channel == null ? "" : channel);
    }
}
