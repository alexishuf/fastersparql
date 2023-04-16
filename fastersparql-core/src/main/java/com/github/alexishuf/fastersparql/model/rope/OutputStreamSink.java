package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.io.OutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class OutputStreamSink implements ByteSink<OutputStreamSink> {
    public OutputStream os;
    private boolean empty = true;

    public OutputStreamSink(OutputStream os) {
        this.os = os;
    }

    private @This OutputStreamSink write(byte b) {
        try {
            empty = false;
            os.write(b);
            return this;
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    private @This OutputStreamSink write(byte[] bytes, int off, int len) {
        try {
            empty &= len == 0;
            os.write(bytes, off, len);
            return this;
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override public boolean isEmpty() {
        return empty;
    }

    @Override public @This OutputStreamSink append(byte[] arr, int begin, int len) {
        return write(arr, begin, len);
    }

    @Override public @This OutputStreamSink append(byte c) {
        return write(c);
    }

    @Override public @This OutputStreamSink append(Rope rope, int begin, int end) {
        if (rope instanceof ByteRope b)
            return write(b.utf8, b.offset+begin, end-begin);
        byte[] a = rope.toArray(begin, end);
        return write(a, 0, a.length);
    }

    @Override public @This OutputStreamSink append(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r)
            return append(r, begin, end);
        return append(cs.subSequence(begin, end).toString().getBytes(UTF_8));
    }

    @Override public @This OutputStreamSink ensureFreeCapacity(int increment) {
        return this;
    }
}
