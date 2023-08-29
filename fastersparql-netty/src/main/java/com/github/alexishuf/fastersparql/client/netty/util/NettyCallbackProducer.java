package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.async.CallbackProducer;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.util.StreamNode;
import io.netty.channel.Channel;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.ClientRetry.retry;

public abstract class NettyCallbackProducer<B extends Batch<B>> extends CallbackProducer<B> {
    private static final int NO_AUTO_READ = 0x40000000;
    private static final int STARTED      = 0x20000000;
    private static final Stateful.Flags FLAGS = Flags.DEFAULT.toBuilder()
            .flag(NO_AUTO_READ, "NO_AUTO_READ")
            .flag(STARTED, "STARTED")
            .build();

    protected @MonotonicNonNull Channel channel;
    private final Runnable autoReadSetter = this::setAutoRead0;
    protected final SparqlClient client;
    private int retries;

    public NettyCallbackProducer(SparqlClient client) {
        super(FLAGS);
        this.client = client;
    }

    @Override public String toString() {
        return "NettyCallbackProducer["+client.endpoint()+"]"+channel;
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.empty();
    }

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
        if (this.channel != null)
            this.channel.close();
        this.channel = channel;
        if ((state()&IS_CANCEL_REQ) != 0)
            channel.close();
    }

    protected abstract void request();

    @Override protected int interceptComplete(@Nullable Throwable cause) {
        cause = cause == CancelledException.INSTANCE ? cause
              : FSException.wrap(client.endpoint(), cause);
        if ((state()&IS_LIVE) != 0 && retry(++retries, cause, this::request))
            return -1; // do not complete
        return super.interceptComplete(cause);
    }

    @Override protected void pause() {
        setAutoRead(false);
    }
    @Override public    void resume() {
        setAutoRead(true);
        if (compareAndSetFlagRelease(STARTED))
            request();
    }

    @Override protected void doCancel() {
        Channel ch = channel;
        if (ch != null)
            ch.close();
    }
}
