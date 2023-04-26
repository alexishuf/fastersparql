package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NettyRopeUtils {

    /** {@code Unpooled.copiedBuffer(cs, charset)}, but faster */
    public static ByteBuf wrap(CharSequence cs, @Nullable Charset charset) {
        if (charset != null && !charset.equals(UTF_8))
            return Unpooled.copiedBuffer(cs, charset);
        if (cs instanceof Rope r) {
            byte[] u8 = r.backingArray();
            if (u8 != null)
                return Unpooled.wrappedBuffer(u8, r.backingArrayOffset(), r.len);
            return Unpooled.wrappedBuffer(r.toArray(0, r.len));
        }
        return Unpooled.copiedBuffer(cs, UTF_8);
    }

    /** {@code dest.writeBytes(source.copy())}, but faster */
    public static ByteBuf write(ByteBuf dest, CharSequence source) {
        if (source instanceof SegmentRope sr) {
            int len = sr.len;
            byte[] u8 = sr.backingArray();
            if (u8 == null) {
                dest.ensureWritable(len, true);
                int wIdx = dest.writerIndex(), freeCap = dest.capacity() - wIdx;
                var segment = MemorySegment.ofBuffer(dest.internalNioBuffer(wIdx, freeCap));
                MemorySegment.copy(sr.segment(), sr.offset(), segment, 0, len);
                dest.writerIndex(wIdx+len);
            } else {
                dest.writeBytes(u8, (int) sr.offset(), len);
            }
        } else if (source instanceof Rope) {
            if (!(source instanceof Term t))
                throw new UnsupportedOperationException();
            var shared = RopeDict.getTolerant(t.flaggedDictId).u8();
            if (t.flaggedDictId < 0) {
                dest.writeBytes(t.local);
                dest.writeBytes(shared);
            } else {
                dest.writeBytes(shared);
                dest.writeBytes(t.local);
            }
        } else {
            dest.writeCharSequence(source, UTF_8);
        }
        return dest;
    }
}
