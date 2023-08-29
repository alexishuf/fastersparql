package com.github.alexishuf.fastersparql.client.netty.util;

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

import static com.github.alexishuf.fastersparql.FSProperties.nettyEventLoopThreads;

public enum NettyTransport {
    NIO {
        @Override public boolean isAvailable() { return true; }
        @Override public EventLoopGroup createGroup() {
            return new NioEventLoopGroup(nettyEventLoopThreads());
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
        @Override public EventLoopGroup createGroup() {
            return new IOUringEventLoopGroup(nettyEventLoopThreads());
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
        @Override public EventLoopGroup createGroup() {
            return new KQueueEventLoopGroup(nettyEventLoopThreads());
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
        @Override public EventLoopGroup createGroup() {
            return new EpollEventLoopGroup(nettyEventLoopThreads());
        }
        @Override public Class<? extends SocketChannel> channelClass() {
            return EpollSocketChannel.class;
        }
    };

    abstract public boolean isAvailable();
    abstract public EventLoopGroup createGroup();
    abstract public Class<? extends SocketChannel>  channelClass();
}
