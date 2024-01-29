package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NettyChannelDebugger extends ChannelDuplexHandler {
    private static final int TICK_COLUMN_WIDTH = "99m59s999_999us".length()+1;
    private static final String  IN_SIGN = ">> ";
    private static final String OUT_SIGN = "<< ";
    private static final String MSG_LF = "\n"+" ".repeat(TICK_COLUMN_WIDTH+3);
    private static final Set<NettyChannelDebugger> ACTIVE = new HashSet<>();
    private static final Lock ACTIVE_LOCK = new ReentrantLock();
    private static final Logger log = LoggerFactory.getLogger(NettyChannelDebugger.class);

    private final String nameContext;
    private String name;
    private final StringBuilder history = new StringBuilder();

    public NettyChannelDebugger(String nameContext) {
        this.nameContext = nameContext;
        this.name = nameContext;
        ACTIVE_LOCK.lock();
        try {
            ACTIVE.add(this);
        } finally { ACTIVE_LOCK.unlock(); }
    }

    @SuppressWarnings("unused") public static void dump(PrintStream dest) {
        ACTIVE_LOCK.lock();
        try {
            for (NettyChannelDebugger d : ACTIVE) {
                dest.println(d.name);
                dest.println(d.history);
            }
        } finally { ACTIVE_LOCK.unlock(); }
    }

    public static void reset() {
        ACTIVE_LOCK.lock();
        try {
            for (NettyChannelDebugger d : ACTIVE)
                d.history.setLength(0);
        } finally { ACTIVE_LOCK.unlock(); }
    }

    @SuppressWarnings("unused") public static void dumpAndReset(PrintStream dest) {
        ACTIVE_LOCK.lock();
        try {
            for (NettyChannelDebugger d : ACTIVE) {
                dest.println(d.name);
                dest.println(d.history);
                d.history.setLength(0);
            }
        } finally { ACTIVE_LOCK.unlock(); }
    }

    private void writeTimeColumn() {
        long ns = Timestamp.nanoTime()-Timestamp.ORIGIN;
        long us = ns/1_000L;
        long ms = us/1_000L;
        us -= ms*1_000;
        int secs = (int)(ms/1_000L);
        ms -= secs*1_000L;
        int mins = secs/60;
        secs -= mins*60;

        if (mins < 10) history.append('0');
        history.append(mins).append('m');

        if (secs < 10) history.append('0');
        history.append(secs).append('s');

        if (ms < 100) history.append('0');
        if (ms <  10) history.append('0');
        history.append(ms).append('_');

        if (us < 100) history.append('0');
        if (us <  10) history.append('0');
        history.append(us).append("us");
        history.append(' ');
    }

    private void writeString(String str) {
        for (int i = 0, len = str.length(); i < len; i++) {
            char c = str.charAt(i);
            switch (c) {
                case '\n' -> history.append(MSG_LF);
                case '\r' -> history.append("\\r");
                case '\t' -> history.append("\\t");
                case '\0' -> history.append("\\0");
                default -> {
                    if (c < ' ')
                        history.append(String.format("\\u%04x", (int)c));
                    history.append(c);
                }
            }
        }
        if (!str.isEmpty() && str.charAt(str.length()-1) == '\n')
            history.setLength(history.length()-MSG_LF.length());
    }

    private void writeMessage(String sign, Object msg) {
        writeTimeColumn();
        history.append(sign);
        if (msg instanceof TextWebSocketFrame f) msg = f.content();
        writeString(msg instanceof ByteBuf b ? b.toString(UTF_8) : msg.toString());
        if (msg instanceof HttpContent hc) {
            history.append(MSG_LF);
            writeString(hc.content().toString(UTF_8));
        }
        history.append('\n');
    }

    @Override public void channelActive(ChannelHandlerContext ctx) {
        name = ctx.channel() + "("+nameContext+")";
        writeMessage("ACTIVE ", ctx.channel());
        ctx.fireChannelActive();
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        writeMessage("INACTIVE ", ctx.channel());
        ACTIVE_LOCK.lock();
        try {
            ACTIVE.remove(this);
            log.info("Channel history for {}:\n{}", name, history);
        } finally {
            ACTIVE_LOCK.unlock();
            ctx.fireChannelInactive();
        }
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
        writeMessage(IN_SIGN, msg);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        writeMessage(OUT_SIGN, msg);
        ctx.write(msg, promise);
    }
}
