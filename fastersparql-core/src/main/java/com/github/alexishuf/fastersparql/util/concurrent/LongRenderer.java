package com.github.alexishuf.fastersparql.util.concurrent;

public interface LongRenderer {
    String render(long value);

    LongRenderer HEX = l -> "0x"+Long.toHexString(l);

}
