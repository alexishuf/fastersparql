package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.FasterSparql;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Accessors(fluent = true) @ToString  @Slf4j
public class EventLoopGroupHolder {
    /**
     * The {@link NettyTransport} implementation
     */
    @Getter private final NettyTransport transport;

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
    @Getter private final @NonNegative int keepAlive;

    /**
     * The {@link TimeUnit} for {@link EventLoopGroupHolder#keepAlive()}.
     */
    @Getter private final TimeUnit keepAliveTimeUnit;

    /**
     * If present, is a scheduled task to shut down the {@link EventLoopGroupHolder#group}
     * after {@link EventLoopGroupHolder#keepAlive()}
     * {@link EventLoopGroupHolder#keepAliveTimeUnit()}s if it still has no references.
     */
    private @Nullable AsyncTask<?> shutdownTask;

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

    public EventLoopGroupHolder() {
        this(chooseTransport(), 0, MILLISECONDS);
    }

    @Builder
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
     * The caller receives ownership of a reference to the {@link EventLoopGroup}, which must
     * be released with a later call to {@link EventLoopGroupHolder#release()} or
     * {@link EventLoopGroupHolder#release(long, TimeUnit)}.
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
            FasterSparql.addShutdownHook(() -> immediateShutdown("FasterSparql.shutdown()"));
        } else if (shutdownTask != null) {
            // if cancel() is too late, the task will see references > 0 and will do nothing
            shutdownTask.cancel(false);
            shutdownTask = null;
        }
        assert group != null : "null group";
        return group;
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
     * If the reference count reaches zero, and {@link EventLoopGroupHolder#keepAlive()}
     * {@code > 0}, the {@link EventLoopGroup} will only be shutdown if no
     * {@link EventLoopGroupHolder#acquire()} call happens within the keep alive window (which
     * starts now). If there is no {@code keepAlive} {@link EventLoopGroup#shutdownGracefully()}
     * is called immediately and its completion will be awaited for at most {@code wait}
     * {@code waitTimeUnit}s.
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
                if (keepAlive > 0 && shutdownTask == null) {
                    shutdownTask = Async.schedule(keepAlive, keepAliveTimeUnit, () ->
                                                  immediateShutdown("keepAlive timeout"));
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
                if (shutdownTask != null)
                    shutdownTask.cancel(false);
                shutdownTask = null;
            }
        }
    }
}
