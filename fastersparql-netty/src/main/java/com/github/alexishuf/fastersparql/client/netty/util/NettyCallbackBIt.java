package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.adapters.LazyCallbackBIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.util.ClientRetry;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import io.netty.channel.Channel;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public abstract class NettyCallbackBIt<T> extends LazyCallbackBIt<T> {
    private static final Logger log = LoggerFactory.getLogger(NettyCallbackBIt.class);

    private int retries;
    protected @MonotonicNonNull Channel channel;
    private boolean backPressured;

    public NettyCallbackBIt(RowType<T> rowType, Vars vars) {
        super(rowType, vars);
    }

    public abstract SparqlClient client();
    protected abstract void request();
    protected void afterNormalComplete() {}

    @Override protected void run() { request(); }

    @Override public void complete(@Nullable Throwable error) {
        lock.lock();
        try {
            assert channel == null || channel.eventLoop().inEventLoop()
                   : "non-cancel() complete() from outside channel event loop";
            //noinspection resource
            error = FSException.wrap(client().endpoint(), error);
            if (!ended && error instanceof FSServerException se && se.shouldRetry()) {
                if (ClientRetry.retry(++retries, error, this::request, this::complete))
                    log.debug("{}: retry {} after {}", this, retries, error.toString());
            } else {
                if (channel != null) {
                    if (error == null) channel.config().setAutoRead(true);
                    else channel.close();
                }
                boolean first = !ended;
                super.complete(error);
                if (error == null && first)
                    afterNormalComplete();
                channel = null;
            }
        } finally { lock.unlock(); }
    }

    @Override protected void waitForCapacity() {
        assert channel == null || channel.eventLoop().inEventLoop()
                : "Feeding from outside channel event loop";
        if (!backPressured && !ended
                && (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches)
                && channel != null) {
            backPressured = true;
            channel.config().setAutoRead(false);
        }
    }

    @Override protected @Nullable Batch<T> fetch() {
        lock.lock();
        try {
            Batch<T> batch = super.fetch();
            if (batch != null && backPressured
                    && ready.size() < maxReadyBatches && readyItems < maxReadyItems) {
                if (channel != null)
                    channel.config().setAutoRead(true);
                backPressured = false;
            }
            return batch;
        } finally { lock.unlock(); }
    }

    @Override public String toString() { return toStringWithOperands(List.of(client())); }
}
