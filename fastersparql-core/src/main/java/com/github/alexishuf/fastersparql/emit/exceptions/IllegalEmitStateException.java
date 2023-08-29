package com.github.alexishuf.fastersparql.emit.exceptions;

import com.github.alexishuf.fastersparql.exceptions.FSIllegalStateException;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IllegalEmitStateException extends FSIllegalStateException {
    public IllegalEmitStateException(String s) {super(s);}
    public IllegalEmitStateException(String s, @Nullable Throwable e) {super(s, e);}
}
