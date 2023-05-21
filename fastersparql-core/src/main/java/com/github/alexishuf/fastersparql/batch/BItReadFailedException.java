package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.exceptions.FSException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class BItReadFailedException extends FSException {
    private final BIt<?> it;

    private static String buildMsg(BIt<?> it, @Nullable Throwable cause) {
        if (cause == null) return "Unknown cause on "+it;
        var m = Objects.requireNonNullElse(cause.getMessage(), "");
        return cause.getClass().getSimpleName()+": "+m+" on "+it;
    }
    public BItReadFailedException(BIt<?> it, Throwable cause) {
        super(buildMsg(it, cause), cause);
        this.it = it;
    }

    public Throwable rootCause() {
        Throwable t = this;
        while (t instanceof BItReadFailedException && t.getCause() != null) t = t.getCause();
        return t;
    }

    public BIt<?> it() { return it; }
}
