package com.github.alexishuf.fastersparql.batch;


/** Helper exception that records the stack trace of who close()d a {@link BIt} */
public class BItClosedAtException extends BItIllegalStateException {
    public BItClosedAtException(BIt<?> it) {
        super(it+" close()d at this point", it);
    }

    public BItClosedAtException(BIt<?> it, BItClosedAtException cause) {
        super(it+" close()d at this point due to "+cause, it);
    }

    /** Get {@code this} or a new instance that points to {@code it} as
     *  {@link BItIllegalStateException#it()} and to {@code this} as {@link Exception#getCause()}. */
    public BItClosedAtException asFor(BIt<?> it) {
        return it == this.it ? this : new BItClosedAtException(it, this);
    }

}
