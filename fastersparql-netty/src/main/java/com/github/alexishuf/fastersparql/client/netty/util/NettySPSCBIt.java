package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.util.ClientRetry;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.Vars;
import io.netty.channel.Channel;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public abstract class NettySPSCBIt<B extends Batch<B>> extends SPSCBIt<B> {
    private static final Logger log = LoggerFactory.getLogger(NettySPSCBIt.class);

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
        if (!terminated && error instanceof FSServerException se && se.shouldRetry()) {
            if (ClientRetry.retry(++retries, error, this::request, this::complete))
                log.debug("{}: retry {} after {}", this, retries, error.toString());
        } else {
            if (ch != null) {
                if (error == null) ch.config().setAutoRead(true);
                else ch.close();
            }
            boolean first = !terminated;
            super.complete(error);
            if (error == null && first)
                afterNormalComplete();
            channel = null;
        }
    }

    @Override protected boolean blocksOnNoCapacity() {
        Channel ch = this.channel;
        assert ch == null || ch.eventLoop().inEventLoop() : "offer() outside channel event loop";
        if (!backPressured && ch != null) {
            backPressured = true;
            ch.config().setAutoRead(false);
        }
        return false; // do not block, force a put() into last queued batch
    }

    @Override public @Nullable B nextBatch(@Nullable B b) {
        b = super.nextBatch(b);
        if (backPressured && hasCapacity()) {
            final Channel ch = channel;
            if (ch != null)
                ch.config().setAutoRead(true);
            backPressured = false;
        }
        return b;
    }

    @Override public String toString() { return toStringWithOperands(List.of(client())); }
}