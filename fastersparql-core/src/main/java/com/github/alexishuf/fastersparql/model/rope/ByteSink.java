package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.common.returnsreceiver.qual.This;

@SuppressWarnings("unchecked")
public interface ByteSink<B extends ByteSink<B>> {
    boolean isEmpty();

    @This B append(byte[] arr, int begin, int len);
    default @This B append(byte[] arr) { return append(arr, 0, arr.length); }
    default @This B append(char c) {
        if (c > 127) throw new IllegalArgumentException();
        append((byte)c);
        return (B) this;
    }
    @This B append(byte c);

    default @This B append(Rope rope) { return append(rope, 0, rope.len); }
    @This B append(Rope rope, int begin, int end);

    default @This B append(CharSequence cs) { return append(cs, 0, cs.length()); }

    @This B append(CharSequence cs, int begin, int end);

    default @This B appendEscapingLF(Object o) {
        Rope r = Rope.of(o);
        ensureFreeCapacity(r.len()+8);
        for (int consumed = 0, i, end = r.len(); consumed < end; consumed = i+1) {
            i = r.skipUntil(consumed, end, '\n');
            append(r, consumed, i);
            if (i < end)
                append('\\').append('n');
        }
        return (B)this;
    }

    default @This B repeat(byte c, int n) {
        ensureFreeCapacity(n);
        while (n-- > 0) append(c);
        return (B)this;
    }

    @This B ensureFreeCapacity(int increment);

    default @This B newline(int spaces) { return append('\n').repeat((byte) ' ', spaces); }

    default @This B indented(int spaces, Object o) {
        Rope r = Rope.of(o);
        int end = r.len();
        ensureFreeCapacity(end+(spaces+1)<<3);
        repeat((byte) ' ', spaces);
        for (int i = 0, eol; i < end; i = eol+1) {
            if (i > 0) newline(spaces);
            append(r, i, eol = r.skipUntil(i, end, '\n'));
        }
        if (end > 0 && r.get(end-1) == '\n') append('\n');
        return (B)this;
    }
}
