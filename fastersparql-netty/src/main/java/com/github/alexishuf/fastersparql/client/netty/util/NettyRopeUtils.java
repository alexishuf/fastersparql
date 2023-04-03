package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.BufferRope;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class NettyRopeUtils {

    /** {@code Unpooled.copiedBuffer(cs, charset)}, but faster */
    public static ByteBuf wrap(CharSequence cs, @Nullable Charset charset) {
        if (charset != null && charset.equals(StandardCharsets.UTF_8))
            return Unpooled.copiedBuffer(cs, charset);
        return switch (cs) {
            case ByteRope b -> Unpooled.wrappedBuffer(b.utf8, b.offset, b.len);
            case BufferRope b -> Unpooled.wrappedBuffer(b.buffer);
            case Rope r -> Unpooled.wrappedBuffer(r.toArray(0, r.len()));
            default -> Unpooled.copiedBuffer(cs, StandardCharsets.UTF_8);
        };
    }

//    /** {@code destination.writeBytes(source.copy())}, but faster */
//    public static ByteBuf copy(ByteBuf destination, Rope source) {
//        switch (source) {
//            case null -> {}
//            case ByteRope b -> destination.writeBytes(b.utf8, b.offset, b.len);
//            case BufferRope b -> destination.writeBytes(Unpooled.wrappedBuffer(b.buffer));
//            case Term t -> {
//                int id = t.flaggedDictId;
//                var fst = id > 0 ? RopeDict.get(id).utf8 : (id == 0 ? EMPTY.utf8 : t.local);
//                var snd = id < 0 ? RopeDict.get(id&0x7fffffff).utf8 : t.local;
//                destination.writeBytes(fst);
//                destination.writeBytes(snd);
//            }
//            case Rope rr -> {
//                assert false : "Wasteful implementation!";
//                destination.writeBytes(rr.toArray(0, rr.len()));
//            }
//        }
//        return destination;
//    }
}