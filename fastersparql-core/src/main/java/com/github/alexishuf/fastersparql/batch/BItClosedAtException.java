package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper exception that records the stack trace of who close()d a {@link BIt} */
public class BItClosedAtException extends BItIllegalStateException {
    public BItClosedAtException(BIt<?> it) {
        super(it+"close()d at this point", it);
    }

    public BItClosedAtException(BIt<?> it, BItClosedAtException cause) {
        super(it+"close()d at this point due to "+cause, it);
    }

    /**
     * Whether {@code this} or a {@link BItClosedAtException} that is a {@link Exception#getCause()}
     * of {@code this} refers to {@code it} or an (in)direct {@link DelegatedControlBIt#delegate()}
     * of {@code it}.
     */
    public boolean isFor(BIt<?> it) {
        for (var e = this; ((e=e.getCause() instanceof BItClosedAtException c ? c : null)) != null; ) {
            for (var i=it; (i=i instanceof DelegatedControlBIt<?,?> d ? d.delegate() : null) != null; ) {
                if (e.it == i) return true;
            }
        }
        return false;
    }

    public static boolean isClosedFor(@Nullable Throwable t, BIt<?> it) {
        return t instanceof BItClosedAtException e && e.isFor(it);
    }

    /** Get {@code this} or a new instance that points to {@code it} as
     *  {@link BItIllegalStateException#it()} and to {@code this} as {@link Exception#getCause()}. */
    public BItClosedAtException asFor(BIt<?> it) {
        return it == this.it ? this : new BItClosedAtException(it, this);
    }

}
