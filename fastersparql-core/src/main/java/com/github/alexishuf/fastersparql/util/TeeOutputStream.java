package com.github.alexishuf.fastersparql.util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class TeeOutputStream extends OutputStream {
    private final List<OutputStream> destinations = new ArrayList<>();

    public @This TeeOutputStream add(OutputStream os) {
        destinations.add(os);
        return this;
    }

    @Override public void close() throws IOException {
        IOException error = null;
        for (OutputStream os : destinations) {
            try {
                os.close();
            } catch (IOException e) {
                if (error == null) error = e;
                else error.addSuppressed(e);
            }
        }
        if (error != null) throw error;
    }

    @Override public void write(int b) throws IOException {
        IOException error = null;
        for (OutputStream o : destinations) {
            try {
                o.write(b);
            } catch (IOException e) {
                if (error == null) error = e;
                else               error.addSuppressed(e);
            }
        }
        if (error != null)
            throw error;
    }

    @Override public void write(byte @NonNull [] b, int off, int len) throws IOException {
        IOException error = null;
        for (OutputStream os : destinations) {
            try {
                os.write(b, off, len);
            } catch (IOException e) {
                if (error == null) error = e;
                else               error.addSuppressed(e);
            }
        }
        if (error != null)
            throw error;
    }

    @Override public void write(byte @NonNull [] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override public void flush() throws IOException {
        IOException error = null;
        for (OutputStream os : destinations) {
            try {
                os.flush();
            } catch (IOException e) {
                if (error == null) error = e;
                else               error.addSuppressed(e);
            }
        }
        if (error != null)
            throw error;
    }

    @Override public String toString() {
        return destinations.toString();
    }

    @Override public int hashCode() {
        int h = 0;
        for (OutputStream os : destinations)
            h = (31*h) + os.hashCode();
        return h;
    }

    @Override public boolean equals(Object o) {
        return o == this || o instanceof TeeOutputStream t && destinations.equals(t.destinations);
    }
}
