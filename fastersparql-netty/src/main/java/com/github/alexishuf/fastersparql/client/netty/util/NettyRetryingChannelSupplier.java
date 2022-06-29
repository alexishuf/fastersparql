package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.CompletableAsyncTask;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class NettyRetryingChannelSupplier {
    public static AsyncTask<Channel> open(Supplier<Future<?>> channelSupplier) {
        return open(0, channelSupplier);
    }
    private static AsyncTask<Channel> open(int attempt, Supplier<Future<?>> channelSupplier) {
        CompletableAsyncTask<Channel> task = new CompletableAsyncTask<>();
        Future<?> future = channelSupplier.get();
        future.addListener(ignored -> {
            try {
                if (future instanceof ChannelFuture)
                    task.complete(((ChannelFuture) future).channel());
                else
                    task.complete((Channel) future.get());
            } catch (Throwable e) {
                Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                boolean retry = (FasterSparqlProperties.maxRetries()+1) > attempt
                             && (cause instanceof NoRouteToHostException ||
                                 cause instanceof ConnectException);
                if (retry) {
                    long ms = FasterSparqlProperties.retryWait(MILLISECONDS);
                    log.info("{}: attempt={} retrying in {}ms", e.getMessage(), attempt, ms);
                    Async.schedule(ms, MILLISECONDS,
                            () -> open(attempt + 1, channelSupplier).handle(
                                    (ret, err) -> err == null
                                            ? task.complete(ret) : task.completeExceptionally(err)));
                } else {
                    task.completeExceptionally(cause);
                }
            }
        });
        return task;
    }
}
