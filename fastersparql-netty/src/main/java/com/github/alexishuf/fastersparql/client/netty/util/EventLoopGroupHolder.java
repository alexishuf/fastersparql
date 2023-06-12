package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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

    /** {@link System#nanoTime()} timestamp when {@code this.group.shutdownGracefully()}
     *  shall be called. {@code -1} means "never". */
    private long shutdownDeadline = -1;

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
    @SuppressWarnings("unused") public TimeUnit keepAliveTimeUnit() { return keepAliveTimeUnit; }
    private final TimeUnit keepAliveTimeUnit;

    /**
     * The current alvie {@link EventLoopGroup}. This field must be set to null before
     * shutting the {@link EventLoopGroup} down.
     */
    private @Nullable EventLoopGroup group;

    private @Nullable CompletableFuture<Void> shutdown;

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

    public EventLoopGroupHolder(@Nullable NettyTransport transport,
                                int keepAlive, @Nullable TimeUnit keepAliveTimeUnit) {
        this.transport = transport == null ? chooseTransport() : transport;
        if (keepAlive < 0)
            throw new IllegalArgumentException("Negative keepAlive="+keepAlive);
        this.keepAlive = keepAlive;
        this.keepAliveTimeUnit = keepAliveTimeUnit == null ? MILLISECONDS : keepAliveTimeUnit;
    }

    /**
     * Get {@link EventLoopGroupHolder#keepAlive()} in the given unit.
     *
     * @param timeUnit desired output {@link TimeUnit}
     *
     * @return The {@link EventLoopGroupHolder#keepAlive()} in {@code timeUnit}s.
     */
    public long keepAlive(TimeUnit timeUnit) {
        return timeUnit.convert(keepAlive, keepAliveTimeUnit);
    }

    /**
     * Gets a {@link EventLoopGroup}, initializing one if necessary.
     *
     * <p>The caller receives ownership of a reference to the {@link EventLoopGroup}, which must
     * be released with a later call to {@link EventLoopGroupHolder#release()} or
     * {@link EventLoopGroupHolder#release(long, TimeUnit)}.</p>
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
            if (group == null) {
                if (references != 1) {
                    log.error("Null {}.group with references={}", this, references);
                    assert false : "group==null with references != 1";
                }
                group = transport.createGroup();
                if (!shutdownHookAdded) {
                    shutdownHookAdded = true;
                    FS.addShutdownHook(() -> {
                        var dummy = new CompletableFuture<>();
                        immediateShutdown(dummy, "FS.shutdown()");
                    });
                }
            }
            shutdownDeadline = -1; // stops already launched virtual thread from shutting down
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
            var future = shutdown == null ? shutdown = new CompletableFuture<>() : shutdown;
            if (references == 0 || group == null) {
                log.error("{}.release() with {} references!", this, references);
                future.complete(null);
                return this.shutdown;
            }
            if (--references == 0) {
                if (keepAlive > 0 && shutdownDeadline == -1) {
                    shutdownDeadline = Timestamp.nanoTime()+keepAliveTimeUnit.toNanos(keepAlive);
                    Thread.startVirtualThread(() -> {
                        try {
                            Thread.sleep(keepAliveTimeUnit.toMillis(keepAlive));
                            immediateShutdown( future, "keepAlive timeout");
                        } catch (InterruptedException ignored) {}
                    });
                } else {
                    immediateShutdown(future, "last release(), no keepAlive");
                }
            }
            return future;
        } finally { lock.unlock(); }
    }

    /**
     * Releases the {@link EventLoopGroup}, allowing for its shutdown if the reference count
     * reaches zero.
     *
     * <p>If the reference count reaches zero, and {@link EventLoopGroupHolder#keepAlive()}
     * {@code > 0}, the {@link EventLoopGroup} will only be shutdown if no
     * {@link EventLoopGroupHolder#acquire()} call happens within the keep alive window (which
     * starts now). If there is no {@code keepAlive} {@link EventLoopGroup#shutdownGracefully()}
     * is called immediately and its completion will be awaited for at most {@code wait}
     * {@code waitTimeUnit}s.</p>
     *
     * @param wait how much time to wait on {@link EventLoopGroup#shutdownGracefully()} if
     *             {@link EventLoopGroupHolder#keepAlive()} is zero and the reference count
     *             reaches zero on this call.
     * @param waitTimeUnit unit of {@code wait}
     */
    public void release(long wait, TimeUnit waitTimeUnit) {
        Future<?> future = release();
        boolean interrupted = false;
        for (long blocked, nanos = waitTimeUnit.toNanos(wait); nanos > 0; ) {
            blocked = Timestamp.nanoTime();
            try {
                future.get(nanos, NANOSECONDS);
                break;
            } catch (InterruptedException e) {
                interrupted  = true;
                nanos -= Timestamp.nanoTime() - blocked;
            } catch (ExecutionException e) {
                log.error("{}: shutdown failed", this, e.getCause());
                break;
            } catch (TimeoutException e) {
                log.debug("{}: shutdown nto complete after {} {}", this, wait, waitTimeUnit);
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    private void immediateShutdown(CompletableFuture<?> shutdownFuture, String reason) {
        lock.lock();
        try {
            if (group != null && references == 0) {
                log.debug("{}.shutdownGracefully() reason: {}", group, reason);
                group.shutdownGracefully().addListener(f -> {
                    if (f.isSuccess())
                        shutdownFuture.complete(null);
                    else
                        shutdownFuture.completeExceptionally(f.cause());
                });
                group = null;
                shutdownDeadline = -1;
            }
        } finally { lock.unlock(); }
    }

    @Override public String toString() {
        String katSuff = switch (keepAliveTimeUnit) {
            case NANOSECONDS  -> "ns";
            case MICROSECONDS -> "us";
            case MILLISECONDS -> "ms";
            case SECONDS      -> "s";
            case MINUTES      -> "m";
            case HOURS        -> "h";
            case DAYS         -> "d";
        };
        return "EventLoopGroupHolder["+transport+"]{" +
                ", refs=" + references +
                ", ka=" + keepAlive + katSuff +
                ", group=" + group +
                '}';
    }
}
