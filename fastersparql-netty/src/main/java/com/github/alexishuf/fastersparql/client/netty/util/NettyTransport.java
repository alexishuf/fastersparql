package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.emit.async.CpuAffinityRunnable;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.BitSet;

public enum NettyTransport {
    NIO {
        @Override public boolean isAvailable() { return true; }
        @Override public EventLoopGroup createGroup(int threads, BitSet affinity) {
            return new NioEventLoopGroup(threads, new AffinityFac(affinity));
        }
        @Override public Class<? extends SocketChannel> channelClass() {
            return NioSocketChannel.class;
        }
    },
    IO_URING {
        @Override public boolean isAvailable() {
            try {
                Class.forName("io.netty.incubator.channel.uring.IOUring");
                return IOUring.isAvailable();
            } catch (ClassNotFoundException e) { return false; }
        }
        @Override public EventLoopGroup createGroup(int threads, BitSet cpuAffinity) {
            return new IOUringEventLoopGroup(threads, new AffinityFac(cpuAffinity));
        }
        @Override public Class<? extends SocketChannel> channelClass() {
            return IOUringSocketChannel.class;
        }
    },
    KQUEUE {
        @Override public boolean isAvailable() {
            try {
                Class.forName("io.netty.channel.kqueue.KQueue");
                return KQueue.isAvailable();
            } catch (ClassNotFoundException e) {return false;}
        }
        @Override public EventLoopGroup createGroup(int threads, BitSet cpuAffinity) {
            return new KQueueEventLoopGroup(threads, new AffinityFac(cpuAffinity));
        }
        @Override public Class<? extends SocketChannel> channelClass() {
            return KQueueSocketChannel.class;
        }
    },
    EPOLL {
        @Override public boolean isAvailable() {
            try {
                Class.forName("io.netty.channel.epoll.Epoll");
                return Epoll.isAvailable();
            } catch (ClassNotFoundException e) {return false;}
        }
        @Override public EventLoopGroup createGroup(int threads, BitSet cpuAffinity) {
            return new EpollEventLoopGroup(threads, new AffinityFac(cpuAffinity));
        }
        @Override public Class<? extends SocketChannel> channelClass() {
            return EpollSocketChannel.class;
        }
    };

    abstract public boolean isAvailable();
    abstract public EventLoopGroup createGroup(int threads, BitSet affinity);
    abstract public Class<? extends SocketChannel>  channelClass();


    private static final class AffinityFac extends DefaultThreadFactory {
        private final BitSet cpuAffinity;
        public AffinityFac(BitSet cpuAffinity) {
            super("NettyELG", true);
            this.cpuAffinity = cpuAffinity;
        }

        @Override public Thread newThread(Runnable r) {
            return super.newThread(new CpuAffinityRunnable(r, cpuAffinity));
        }

        @Override protected Thread newThread(Runnable r, String name) {
            return super.newThread(new CpuAffinityRunnable(r, cpuAffinity), name);
        }
    }


}
