package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("unchecked")
public interface ByteSink<S extends ByteSink<S, T>, T> extends AutoCloseable  {
    /**
     * Copy or move the bytes written to this {@link ByteSink} as an instance of {@code T}.
     *
     * <p>{@link #touch()} must be called after this method before data can be written to
     * this sink. Writes to the sink after this method is called will not be visible in the
     * {@code T} object returned from this call.</p>
     *
     * @return an object containing all bytes written since the last call to this method and
     *         which will not be affected by future writes to {@code this} sink.
     */
    T take();

    /**
     * Similar to {@link #take()}, but only the first {@code len} bytes are moved or copied
     * into the returned instance of {@code T}. The bytes between index {@code len} (inclusive)
     * and {@link #len()} (exclusive) are copied/moved to {@code this} {@link ByteSink} after
     * an implicit  {@link #touch()}.
     *
     * <p>This is equivalent to (but more efficient than):</p>
     *
     * <pre>{@code
     * var data = sink.take();
     * sink.touch().append(data, len, data.len()); // copy trailing bytes back
     * data.len = len;                             // remove trailing bytes
     * }</pre>
     *
     * @param len the number of bytes to copy/move out. Must be {@code <=} {@link #len()}
     * @return an object containing all bytes from {@code 0} to {@code len} (non-inclusive).
     *         Such object has an independent lifecycle: mutations to it are not visible in the
     *         sink and mutations in the sink are not visible in it.
     */
    T takeUntil(int len);

    /**
     * If {@link #take()} implementation relies on mocking an object under manual memory management
     * (e.g., reference-counted), this will have the same effects as {@link #take()} on this sink
     * but will also perform the type-specific release required by the internal object used by
     * this sink to hold written bytes.
     */
    default void close() {}

    /**
     * Whether {@link #touch()} must be called before bytes are appended to {@code this}.
     */
    @SuppressWarnings("unused") default boolean needsTouch() { return false; }

    /**
     * Ensures that this sink has backing storage to where the writing methods can write to.
     *
     * @return {@code this}
     */
    default @This S touch() { return (S) this; }

    boolean isEmpty();
    int len();

    /** How many bytes can be appended before the ByteSink performs reallocation internally.  */
    int freeCapacity();

    /**
     * Get the last value set with {@link #sizeHint(int)}.
     */
    default int sizeHint() { return freeCapacity(); }

    /**
     * At next {@link #touch()} call, if {@link #needsTouch()}, suggest an allocation that would
     * yield {@link #freeCapacity()} {@code >= hint}.
     *
     * <p>This method is merely a hint and can be safely ignored by some implementations.</p>
     *
     * @param hint desired {@link #freeCapacity()} on next {@link #touch()}
     */
    default void sizeHint(int hint) { /* ignore */  }

    @This S append(MemorySegment segment, byte @Nullable [] utf8, long offset, int len);
    @This S append(byte[] arr, int begin, int len);
    default @This S append(byte[] arr) { return append(arr, 0, arr.length); }
    default @This S append(char c) {
        if (c > 127) throw new IllegalArgumentException();
        append((byte)c);
        return (S) this;
    }
    @This S append(byte c);

    default @This S appendCodePoint(int code) {
        ensureFreeCapacity(RopeFactory.requiredBytesByCodePoint(code));
        RopeEncoder.codePoint2utf8(code, RopeEncoder.BYTE_SINK_APPENDER, this);
        return (S) this;
    }

    default @This S append(Rope rope) { return append(rope, 0, rope.len); }
    @This S append(Rope rope, int begin, int end);

    default @This S append(SegmentRope rope) { return append(rope, 0, rope.len); }
    @This S append(SegmentRope rope, int begin, int end);

    default @This S append(CharSequence cs) {
        if (cs instanceof Rope r)
            return append(r, 0, r.len);
        return appendCharSequence(cs, 0, cs.length());
    }

    default @This S append(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r)
            return append(r, begin, end);
        return appendCharSequence(cs, begin, end);
    }

    default @This S appendCharSequence(CharSequence cs, int begin, int end) {
        ensureFreeCapacity(RopeFactory.requiredBytes(cs, begin, end));
        RopeEncoder.charSequence2utf8(cs, begin, end, RopeEncoder.BYTE_SINK_APPENDER, this);
        return (S)this;
    }

    default @This S appendEscapingLF(Object o) {
        Rope r = Rope.asRope(o);
        ensureFreeCapacity(r.len()+8);
        for (int consumed = 0, i, end = r.len(); consumed < end; consumed = i+1) {
            i = r.skipUntil(consumed, end, (byte)'\n');
            append(r, consumed, i);
            if (i < end)
                append('\\').append('n');
        }
        return (S)this;
    }

    default @This S repeat(byte c, int n) {
        ensureFreeCapacity(n);
        while (n-- > 0) append(c);
        return (S)this;
    }

    @This S ensureFreeCapacity(int increment);

    default @This S newline(int spaces) { return append('\n').repeat((byte)' ', spaces); }

    default @This S indented(int spaces, Object o) {
        Rope r = Rope.asRope(o);
        int end = r.len();
        ensureFreeCapacity(end+(spaces+1)<<3);
        repeat((byte)' ', spaces);
        for (int i = 0, eol; i < end; i = eol+1) {
            if (i > 0) newline(spaces);
            append(r, i, eol = r.skipUntil(i, end, (byte)'\n'));
        }
        if (end > 0 && r.get(end-1) == '\n') append('\n');
        return (S)this;
    }
}
