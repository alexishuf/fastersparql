package com.github.alexishuf.fastersparql.util;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.OutputStream;

public class CloseShieldOutputStream extends OutputStream {
    private final OutputStream delegate;

    public CloseShieldOutputStream(OutputStream delegate) {
        this.delegate = delegate;
    }

    @Override public void close() throws IOException {
        delegate.flush();
    }

    @Override public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override public void write(byte @NonNull [] b) throws IOException {
        delegate.write(b);
    }

    @Override public void write(byte @NonNull [] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override public void flush() throws IOException {
        delegate.flush();
    }

    @Override public String toString() {
        return "CloseShieldOutputStream("+delegate+")";
    }

    @Override public int hashCode() {
        return delegate.hashCode();
    }

    @Override public boolean equals(Object o) {
        return o == this || o instanceof CloseShieldOutputStream s && delegate.equals(s.delegate);
    }
}
