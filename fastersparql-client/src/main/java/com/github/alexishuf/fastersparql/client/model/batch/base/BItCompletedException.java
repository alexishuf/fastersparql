package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.BItIllegalStateException;

public class BItCompletedException extends BItIllegalStateException {
    public BItCompletedException(String s, BufferedBIt<?> it) { super(s, it); }
}
