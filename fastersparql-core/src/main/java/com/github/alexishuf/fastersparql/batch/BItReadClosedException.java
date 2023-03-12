package com.github.alexishuf.fastersparql.batch;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BItReadClosedException extends BItIllegalStateException {
    private final BItClosedAtException when;

    private static String buildMessage(BIt<?> it, @Nullable BItClosedAtException when) {
        StackTraceElement[] trace = when == null ? null : when.getStackTrace();
        boolean has = trace != null && trace.length >= 2;
        return it + " already close()d by " + (has ? trace[1] : "unknown caller");
    }

    public BItReadClosedException(BIt<?> it, @Nullable BItClosedAtException when) {
        super(buildMessage(it, when), it);
        this.when = when;
    }

    public BItClosedAtException when() { return when; }
}
