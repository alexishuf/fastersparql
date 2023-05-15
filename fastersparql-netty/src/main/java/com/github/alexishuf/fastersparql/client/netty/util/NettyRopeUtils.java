package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

import static java.lang.foreign.MemorySegment.ofBuffer;
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
    /** {@code bb.writeBytes(source.copy())}, but faster */
    public static ByteBuf write(ByteBuf bb, MemorySegment segment, long off, int len) {
        if (len <= 0) return bb;
        bb.ensureWritable(len);
        byte[] u8 = (byte[]) segment.array().orElse(null);
        if (u8 == null) {
            int wIdx = bb.writerIndex(), free = bb.capacity() - wIdx;
            MemorySegment.copy(segment, off,
                    ofBuffer(bb.internalNioBuffer(wIdx, free)), 0, len);
            bb.writerIndex(wIdx+len);
        } else {
            bb.writeBytes(u8, (int)(segment.address()+off), len);
        }
        return bb;
    }

    /** {@code bb.writeBytes(source.copy())}, but faster */
    public static ByteBuf write(ByteBuf bb, Rope rope) {
        if (rope == null || rope.len == 0) {
            return bb;
        } else if (rope instanceof SegmentRope sr) {
            return write(bb, sr.segment, sr.offset, sr.len);
        } else {
            MemorySegment fst, snd;
            long fstOff, sndOff;
            int fstLen, sndLen;
            if (rope instanceof TwoSegmentRope t) {
                fst = t.fst; fstOff = t.fstOff; fstLen = t.fstLen;
                snd = t.snd; sndOff = t.sndOff; sndLen = t.fstLen;
            } else if (rope instanceof Term t) {
                SegmentRope fr = t.first(), sr = t.second();
                fst = fr.segment; fstOff = fr.offset; fstLen = fr.len;
                snd = sr.segment; sndOff = sr.offset; sndLen = sr.len;
            } else {
                throw new UnsupportedOperationException("Unsupported Rope type: "+rope.getClass());
            }
            write(bb, fst, fstOff, fstLen);
            write(bb, snd, sndOff, sndLen);
            return bb;
        }
    }

    /** {@code bb.writeBytes(source.copy())}, but faster */
    public static ByteBuf write(ByteBuf bb, CharSequence source) {
        if (source instanceof Rope r)
            write(bb, r);
        else
            bb.writeCharSequence(source, UTF_8);
        return bb;
    }
}
