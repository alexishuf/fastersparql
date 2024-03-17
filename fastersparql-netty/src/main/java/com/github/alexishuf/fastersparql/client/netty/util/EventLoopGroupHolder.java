package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventLoopGroupHolder {
    private static final Logger log = LoggerFactory.getLogger(EventLoopGroupHolder.class);

    /**
     * The {@link NettyTransport} implementation
     */
    @SuppressWarnings("unused") public NettyTransport transport() { return transport; }
    private final NettyTransport transport;

    private final Lock lock = new ReentrantLock();

    /** Number of {@code acquire()}s without corresponding {@code release()}. */
    private int references = 0;

    /** Whether {@link FS#addShutdownHook(Runnable)} was called for {@code this}. */
    private boolean shutdownHookAdded = false;

    /**
     * Amount of time to wait before shutting down the {@link EventLoopGroup}
     * once {@link EventLoopGroupHolder#references} reaches zero. If an
     * {@link EventLoopGroupHolder#acquire()} occurs within this time window, no shutdown will
     * occur.
     */
    @SuppressWarnings("unused") public @NonNegative int keepAlive() { return keepAlive; }
    private final @NonNegative int keepAlive;

    /**
     * The {@link TimeUnit} for {@link EventLoopGroupHolder#keepAlive()}.
     */
    @SuppressWarnings("unused") public TimeUnit keepAliveTimeUnit() { return keepAliveUnit; }
    private final TimeUnit keepAliveUnit;

    private @Nullable KeepAliveShutdown keepAliveShutdown;

    @SuppressWarnings("unused") public int threads() { return threads; }
    private final int threads;

    /**
     * The current alvie {@link EventLoopGroup}. This field must be set to null before
     * shutting the {@link EventLoopGroup} down.
     */
    private @Nullable EventLoopGroup group;

    private CompletableFuture<Void> shutdown() {
        return shutdown == null ? shutdown = new CompletableFuture<>() : shutdown;
    }
    private @Nullable CompletableFuture<Void> shutdown;

    private String name;

    private static NettyTransport chooseTransport() {
        NettyTransport selected = NettyTransport.NIO;
        for (NettyTransport transport : NettyTransport.values()) {
            if (transport.isAvailable()) {
                selected = transport;
                break;
            }
        }
        log.debug("Using "+selected+" for transport");
        return selected;
    }

    public EventLoopGroupHolder(String name, @Nullable NettyTransport transport, int keepAlive,
                                @Nullable TimeUnit keepAliveTimeUnit, int threads) {
        this.name = name;
        this.transport = transport == null ? chooseTransport() : transport;
        if (keepAlive < 0)
            throw new IllegalArgumentException("Negative keepAlive="+keepAlive);
        this.keepAlive         = keepAlive;
        this.keepAliveUnit = keepAliveTimeUnit == null ? MILLISECONDS : keepAliveTimeUnit;
        this.threads           = Math.max(0, threads);
    }

    /**
     * Get {@link EventLoopGroupHolder#keepAlive()} in the given unit.
     *
     * @param timeUnit desired output {@link TimeUnit}
     *
     * @return The {@link EventLoopGroupHolder#keepAlive()} in {@code timeUnit}s.
     */
    public long keepAlive(TimeUnit timeUnit) {
        return timeUnit.convert(keepAlive, keepAliveUnit);
    }

    /**
     * Gets a {@link EventLoopGroup}, initializing one if necessary.
     *
     * <p>The caller receives ownership of a reference to the {@link EventLoopGroup}, which must
     * be released with a later call to {@link EventLoopGroupHolder#release()}.
     *
     * @return a non-null usable {@link EventLoopGroup}.
     */
    @MustCall("this.release")
    public EventLoopGroup acquire() {
        lock.lock();
        try {
            if (references < 0) {
                log.error("Negative references={} on {}", references, this);
                assert false : "negative references";
            }
            ++references;
            if (references == 1 && keepAliveShutdown != null) {
                keepAliveShutdown.cancel();
                keepAliveShutdown = null;
            }
            if (group == null) {
                if (references != 1) {
                    log.error("Null {}.group with references={}", this, references);
                    assert false : "group==null with references != 1";
                }
                group = transport.createGroup(threads);
                if (!shutdownHookAdded) {
                    shutdownHookAdded = true;
                    FS.addShutdownHook(() -> shutdownNow(this.group, "FS.shutdownNow"));
                }
            }
            return group;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Builds a {@link Bootstrap} with the {@link EventLoopGroupHolder#acquire()}d
     * {@link EventLoopGroup}.
     *
     * <p>Applicable configurations from {@link FSProperties} will be set.</p>
     *
     * <p><strong>{@link EventLoopGroupHolder#release()} must be called once the
     * {@link Bootstrap} is discarded.</strong></p>
     *
     * @param host The DNS host name or IPv4/IPv6 address of the remote server
     * @param port The TCP port where the HTTP server is listening
     * @return A new {@link Bootstrap} bound to the acquired {@link EventLoopGroup}.
     */
    public Bootstrap acquireBootstrap(String host, int port) {
        EventLoopGroup group = acquire();
        try {
            Bootstrap bootstrap = new Bootstrap().group(group).remoteAddress(host, port)
                                                 .channel(transport.channelClass());
            int connTimeoutMs = FSProperties.connectTimeoutMs();
            if (connTimeoutMs > 0)
                bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connTimeoutMs);
            int soTimeoutMs = FSProperties.soTimeoutMs();
            if (soTimeoutMs > 0)
                bootstrap = bootstrap.option(ChannelOption.SO_TIMEOUT, soTimeoutMs);
            return bootstrap;
        } catch (Throwable t) {
            release();
            throw t;
        }
    }

    /**
     * Equivalent to {@link EventLoopGroupHolder#release(long, TimeUnit)} with zero MILLISECONDS.
     */
    public Future<?> release() {
        lock.lock();
        try {
            if (references == 0 || group == null) {
                log.error("{}.release() with {} references!", this, references);
            } else if (--references == 0) {
                if (keepAlive > 0) {
                    int ms = (int)Math.min(Integer.MAX_VALUE, keepAliveUnit.toMillis(keepAlive));
                    keepAliveShutdown = new KeepAliveShutdown(ms, "keepAlive timeout");
                    Thread.startVirtualThread(keepAliveShutdown);
                } else {
                    shutdownNow(group, "last release(), no keepAlive");
                }
            }
            return shutdown();
        } finally { lock.unlock(); }
    }

    private final class KeepAliveShutdown implements Runnable {
        private final int ms;
        private final String reason;
        private final EventLoopGroup target;
        private boolean cancelled;

        public KeepAliveShutdown(int ms, String reason) {
            this.ms = ms;
            this.reason = reason;
            this.target = group;
        }

        public void cancel() { cancelled = true; }

        @Override public void run() {
            try {
                Thread.sleep(ms);
                if (!cancelled)
                    shutdownNow(target, reason);
            } catch (InterruptedException ignored) {}
        }
    }

    private void shutdownNow(EventLoopGroup target, String reason) {
        lock.lock();
        try {
            if (group == target && references == 0) {
                log.debug("{}.shutdownGracefully() reason: {}", group, reason);
                var future = shutdown();
                target.shutdownGracefully().addListener(f -> {
                    if (f.isSuccess())
                        future.complete(null);
                    else
                        future.completeExceptionally(f.cause());
                });
                group = null;
            }
        } finally { lock.unlock(); }
    }

    @Override public String toString() {return "ELGHolder("+name+")";}
}
