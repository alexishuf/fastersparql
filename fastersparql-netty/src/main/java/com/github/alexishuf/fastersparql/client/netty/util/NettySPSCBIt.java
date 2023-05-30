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


public abstract class NettySPSCBIt<B extends Batch<B>> extends SPSCBIt<B> {

    private int retries;
    protected @MonotonicNonNull Channel channel;
    private boolean backPressured;

    public NettySPSCBIt(BatchType<B> batchType, Vars vars, int maxBatches) {
        super(batchType, vars, maxBatches);
    }

    public abstract SparqlClient client();
    protected abstract void request();
    protected void afterNormalComplete() {}

    @Override public void complete(@Nullable Throwable error) {
        final Channel ch = channel;
        assert ch == null || ch.eventLoop().inEventLoop() : "complete() outside event loop";
        //noinspection resource
        error = FSException.wrap(client().endpoint(), error);
        if (!isTerminated() && ClientRetry.retry(++retries, error, this::request))
            return; //will retry request
        if (ch != null) {
            if (error == null) ch.config().setAutoRead(true);
            else ch.close();
        }
        boolean first = !isTerminated();
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

    @Override public String toString() {//noinspection resource
        return "NettySPSC["+client().endpoint()+"]@"+id();
    }
}
