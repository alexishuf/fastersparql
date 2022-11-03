package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;

public class BItClosedException extends BItIllegalStateException {
    public BItClosedException(BIt<?> it) { super(it+": already close()d", it); }

    public static boolean isClosedExceptionFor(Throwable t, BIt<?> it) {
        BItClosedException ce = t instanceof BItClosedException cast ? cast
                : (t != null && t.getCause() instanceof BItClosedException cast ? cast : null);
        if (ce == null) return false;
        while (it != null) {
            //noinspection resource
            if (ce.it() == it) return true;
            it = it instanceof DelegatedControlBIt<?,?> d ? d.delegate() : null;
        }
        return false;
    }
}
