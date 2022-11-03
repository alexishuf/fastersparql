package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BItIllegalStateException;

public class BItCompletedException extends BItIllegalStateException {
    public BItCompletedException(String s, BufferedBIt<?> it) { super(s, it); }
}
