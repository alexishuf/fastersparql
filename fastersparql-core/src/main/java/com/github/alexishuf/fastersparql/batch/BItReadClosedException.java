package com.github.alexishuf.fastersparql.batch;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BItReadClosedException extends BItIllegalStateException {
    public final BItClosedAtException when;

    private static String buildMessage(BIt<?> it, @Nullable BItClosedAtException when) {
        StackTraceElement[] trace = when == null ? null : when.getStackTrace();
        var sb = new StringBuilder().append(it).append(" already close()d ");
        if (trace == null) return sb.toString();
        for (int i = 1, last = Math.min(trace.length, 6); i <= last; i++) {
            StackTraceElement e = trace[i];
            String cls = e.getClassName();
            sb.append(cls.substring(cls.lastIndexOf('.')+1));
            sb.append('.').append(e.getMethodName()).append('/');
        }
        sb.setLength(sb.length()-1);
        return sb.toString();
    }

    public BItReadClosedException(BIt<?> it, @Nullable BItClosedAtException when) {
        super(buildMessage(it, when), it);
        this.when = when;
    }
}
