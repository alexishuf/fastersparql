package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.FS;
import com.github.alexishuf.fastersparql.client.util.FSProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventLoopGroupHolder {
    private static final Logger log = LoggerFactory.getLogger(EventLoopGroupHolder.class);

    /**
     * The {@link NettyTransport} implementation
     */
    @SuppressWarnings("unused") public NettyTransport transport() { return transport; }
    private final NettyTransport transport;


    /**
     * How many {@link EventLoopGroupHolder#acquire()}s are active
     * (i.e., no {@link EventLoopGroupHolder#release()} yet).
     */
    private int references;

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
     * If present, is a scheduled task to shut down the {@link EventLoopGroupHolder#group}
     * after {@link EventLoopGroupHolder#keepAlive()}
     * {@link EventLoopGroupHolder#keepAliveTimeUnit()}s if it still has no references.
     */
    private @Nullable Thread shutdownThread = null;

    /**
     * The current alvie {@link EventLoopGroup}. This field must be set to null before
     * shutting the {@link EventLoopGroup} down.
     */
    private @Nullable EventLoopGroup group;

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
    public synchronized EventLoopGroup acquire() {
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
            FS.addShutdownHook(() -> immediateShutdown("FasterSparql.shutdown()"));
        } else if (shutdownThread != null) {
            // if interrupted too late, it will see references > 0 and will do nothing
            shutdownThread.interrupt();
        }
        assert group != null : "null group";
        return group;
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
    public void release() {
        release(0, MILLISECONDS);
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
        Future<?> future = null;
        EventLoopGroup shuttingDown = null;
        synchronized (this) {
            if (references == 0) {
                log.error("release() on zero-references {}!", this);
                return;
            } else if (group == null) {
                log.error("null group with references={} at {}", references, this);
                assert false : "null group";
                return;
            }
            --references;
            if (references == 0) {
                if (keepAlive > 0 && shutdownThread == null) {
                    shutdownThread = Thread.startVirtualThread(() -> {
                        try {
                            Thread.sleep(keepAliveTimeUnit.toMillis(keepAlive));
                            immediateShutdown("keepAlive timeout");
                        } catch (InterruptedException ignored) {}
                    });
                } else {
                    future = (shuttingDown = group).shutdownGracefully();
                    group = null;
                }
            }
        }
        if (future != null && wait > 0) {
            if (!future.awaitUninterruptibly(wait, waitTimeUnit))
                log.debug("{} not terminated after {} {}.", shuttingDown, wait, waitTimeUnit);
        }
    }

    private void immediateShutdown(String reason) {
        synchronized (EventLoopGroupHolder.this) {
            if (group != null && references == 0) {
                log.debug("{}.shutdownGracefully() reason: {}", group, reason);
                group.shutdownGracefully();
                group = null;
                if (shutdownThread != null && Thread.currentThread() != shutdownThread)
                    shutdownThread.interrupt();
                shutdownThread = null;
            }
        }
    }

    @Override public String toString() {
        return "EventLoopGroupHolder{" +
                "transport=" + transport +
                ", references=" + references +
                ", keepAlive=" + keepAlive + keepAliveTimeUnit.name().toLowerCase() +
                ", group=" + group +
                '}';
    }
}
