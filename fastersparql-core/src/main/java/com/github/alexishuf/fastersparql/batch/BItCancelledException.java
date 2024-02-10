package com.github.alexishuf.fastersparql.batch;


import com.github.alexishuf.fastersparql.FSProperties;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper exception that records the stack trace of who close()d a {@link BIt} */
public class BItCancelledException extends BItIllegalStateException {
    private static final boolean TRACE = FSProperties.itTraceCancel();
    private static final BItCancelledException UNTRACED = new BItCancelledException(null);

    private BItCancelledException(@Nullable BIt<?> it) {
        super(it == null ? "Cancelled via tryCancel()"
                         : it+" cancelled at this point");
    }

    public static BItCancelledException get(BIt<?> it) {
        if (TRACE) return new BItCancelledException(it);
        return UNTRACED;
    }
}
