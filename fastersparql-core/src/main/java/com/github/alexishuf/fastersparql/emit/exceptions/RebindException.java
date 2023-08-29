package com.github.alexishuf.fastersparql.emit.exceptions;

import org.checkerframework.checker.nullness.qual.Nullable;

public class RebindException extends IllegalEmitStateException {
    public RebindException(String s) {super(s);}
    public RebindException(String s, @Nullable Throwable e) {super(s, e);}
}
