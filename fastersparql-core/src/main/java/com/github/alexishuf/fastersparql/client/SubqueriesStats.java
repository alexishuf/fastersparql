package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.FSProperties;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SubqueriesStats {
    public static final boolean ENABLED = FSProperties.countSubqueries();

    private static final VarHandle SUBQUERIES, SUBQUERIES_BYTES, RESPONSE_BYTES, BINDINGS_BYTES;
    @SuppressWarnings("unused") private static long plainSubqueries;
    @SuppressWarnings("unused") private static long plainSubqueriesBytes;
    @SuppressWarnings("unused") private static long plainResponseBytes;
    @SuppressWarnings("unused") private static long plainBindingsBytes;
    static {
        try {
            SUBQUERIES       = MethodHandles.lookup().findStaticVarHandle(SubqueriesStats.class, "plainSubqueries",      long.class);
            SUBQUERIES_BYTES = MethodHandles.lookup().findStaticVarHandle(SubqueriesStats.class, "plainSubqueriesBytes", long.class);
            RESPONSE_BYTES   = MethodHandles.lookup().findStaticVarHandle(SubqueriesStats.class, "plainResponseBytes",   long.class);
            BINDINGS_BYTES   = MethodHandles.lookup().findStaticVarHandle(SubqueriesStats.class, "plainBindingsBytes",   long.class);
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static void reset() {
        if (ENABLED) {
            SUBQUERIES.setRelease(0L);
            SUBQUERIES_BYTES.setRelease(0L);
            RESPONSE_BYTES.setRelease(0L);
            BINDINGS_BYTES.setRelease(0L);
        }
    }

    public static long subqueriesBytesSent() {return (long)SUBQUERIES_BYTES.getOpaque();}
    public static long       bindingsBytes() {return (long)BINDINGS_BYTES  .getOpaque();}
    public static long       responseBytes() {return (long)RESPONSE_BYTES  .getOpaque();}
    public static long      subqueriesSent() {return (long)SUBQUERIES      .getOpaque();}

    public static void subquerySent(int bytes) {
        if (ENABLED) {
            SUBQUERIES      .getAndAddRelease(1L);
            SUBQUERIES_BYTES.getAndAddRelease((long)bytes);
        }
    }

    public static void responseReceived(int bytes) {
        if (ENABLED)
            RESPONSE_BYTES.getAndAddRelease((long)bytes);
    }

    public static void bindingsReceived(int bytes) {
        if (ENABLED)
            BINDINGS_BYTES.getAndAddRelease((long)bytes);
    }
}
