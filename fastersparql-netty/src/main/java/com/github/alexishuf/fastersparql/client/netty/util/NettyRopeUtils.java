package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.Nullable;
import sun.misc.Unsafe;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import static java.lang.foreign.MemorySegment.ofBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettyRopeUtils {
    private static final Unsafe U;
    private static final int U8_BASE;
    static {
        Unsafe u = null;
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            u = (Unsafe) field.get(null);
        } catch (Throwable ignored) {
            try {
                Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
                u = c.newInstance();
            } catch (Throwable ignored1) {}
        }
        U = u;
        U8_BASE = u == null ? 0 : u.arrayBaseOffset(byte[].class);
    }


    /** Get a {@link ByteBuf} with the contents of {@code cs} */
    public static ByteBuf asByteBuf(FinalSegmentRope r) {return asByteBuf0(r);}

    /** Get a {@link ByteBuf} with the contents of {@code cs} */
    public static ByteBuf asByteBuf(SegmentRopeView r) {return asByteBuf0(r);}

    private static ByteBuf asByteBuf0(SegmentRope r) {
        byte[] utf8 = r.utf8;
        if (utf8 != null)
            return Unpooled.wrappedBuffer(utf8, (int)r.offset, r.len);
        return Unpooled.wrappedBuffer(r.segment.address()+r.offset, r.len, false);
    }

    /** Get a {@link ByteBuf} with the contents of {@code cs} */
    public static ByteBuf asByteBuf(MutableRope r) {
        return write(ByteBufAllocator.DEFAULT.buffer(r.len), r);
    }

    /** Get a {@link ByteBuf} with the contents of {@code cs} */
    public static ByteBuf asByteBuf(SegmentRope r) {
        if (r instanceof MutableRope m)
            return asByteBuf(m);
        return asByteBuf0(r);
    }

    /** {@code bb.writeBytes(source.copy())}, but faster */
    public static ByteBuf write(ByteBuf bb, MemorySegment segment, byte @Nullable[] array,
                                long off, int len) {
        if (len <= 0)
            return bb;
        if (off+len > segment.byteSize())
            throw new IndexOutOfBoundsException("[offset, offset+len) not in [0, segment.byteSize())");
        bb.ensureWritable(len);
        if (U != null) {
            byte[] destBase = null;
            int wIdx = bb.writerIndex();
            long destOff = -1;
            if (bb.hasArray()) {
                destBase = bb.array();
                destOff = wIdx+bb.arrayOffset()+U8_BASE;
            } else if (bb.hasMemoryAddress()) {
                destOff = wIdx+bb.memoryAddress();
            }
            if (destOff != -1) {
                U.copyMemory(array, segment.address()+(array == null ? 0 : U8_BASE)+off,
                             destBase, destOff, len);
                bb.writerIndex(wIdx+len);
                return bb;
            }
        }
        return writeSafe(bb, segment, off, len);
    }

    private static ByteBuf writeSafe(ByteBuf bb, MemorySegment segment, long off, int len) {
        byte[] array = segment.isNative() ? null : (byte[])segment.heapBase().orElse(null);
        if (array == null) {
            int wIdx = bb.writerIndex(), free = bb.capacity() - wIdx;
            MemorySegment.copy(segment, off,
                               ofBuffer(bb.internalNioBuffer(wIdx, free)), 0, len);
            bb.writerIndex(wIdx+len);
        } else {
            bb.writeBytes(array, (int)(segment.address()+off), len);
        }
        return bb;
    }

    /** {@code bb.writeBytes(source.copy())}, but faster */
    @SuppressWarnings("UnusedReturnValue") public static ByteBuf write(ByteBuf bb, SegmentRope rope) {
        return write(bb, rope.segment, rope.utf8, rope.offset, rope.len);
    }

    /** {@code bb.writeBytes(source.copy())}, but faster */
    @SuppressWarnings("UnusedReturnValue") public static ByteBuf write(ByteBuf bb, Rope rope) {
        if (rope == null || rope.len == 0) {
            return bb;
        } else if (rope instanceof SegmentRope sr) {
            return write(bb, sr.segment, sr.utf8, sr.offset, sr.len);
        } else {
            MemorySegment fst, snd;
            byte[] fstU8, sndU8;
            long fstOff, sndOff;
            int fstLen, sndLen;
            if (rope instanceof TwoSegmentRope t) {
                fst = t.fst; fstU8 = t.fstU8; fstOff = t.fstOff; fstLen = t.fstLen;
                snd = t.snd; sndU8 = t.sndU8; sndOff = t.sndOff; sndLen = t.fstLen;
            } else if (rope instanceof Term t) {
                SegmentRope fr = t.first(), sr = t.second();
                fst = fr.segment; fstU8 = fr.utf8; fstOff = fr.offset; fstLen = fr.len;
                snd = sr.segment; sndU8 = sr.utf8; sndOff = sr.offset; sndLen = sr.len;
            } else {
                throw new UnsupportedOperationException("Unsupported Rope type: "+rope.getClass());
            }
            write(bb, fst, fstU8, fstOff, fstLen);
            write(bb, snd, sndU8, sndOff, sndLen);
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
