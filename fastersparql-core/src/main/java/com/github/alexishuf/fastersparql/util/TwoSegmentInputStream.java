package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.io.InputStream;

public class TwoSegmentInputStream extends InputStream {
    private TwoSegmentRope rope;
    private int pos, end, mark;

    public @This TwoSegmentInputStream wrap(TwoSegmentRope rope, int begin, int end) {
        this.rope = rope;
        this.pos  = begin;
        this.mark = begin;
        this.end  = end;
        return this;
    }

    @Override public int read() { return pos == end ? -1 : rope.get(pos++)&0xff; }

    @Override public int read(byte @NonNull [] b, int off, int len) {
        int pos = this.pos, end = this.end;
        if (len <= 0)
            return 0;
        if (pos >= end)
            return -1;
        int until = Math.min(end, pos+Math.min(b.length-off, len));
        rope.copy(pos, until, b, off);
        this.pos = until;
        return until-pos;
    }

    @Override public long skip(long n) throws IOException {
        long skipped = Math.min(end-pos, Math.max(0, n));
        pos += (int)skipped;
        return skipped;
    }

    @Override public int         available()            { return end-pos; }
    @Override public void             mark(int ignored) { mark =  pos; }
    @Override public void            reset()            { pos  = mark; }
    @Override public boolean markSupported()            { return true; }
}
