package com.github.alexishuf.fastersparql.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class OOMHandler {
    private static final AtomicInteger   nextCallbackPos = new AtomicInteger();
    private static final String  []      callbackName    = new String  [512];
    private static final Runnable[]      callback        = new Runnable[512];
    private static int callbackCount;
    private static final ExitThread exitThread = new ExitThread();
    private static final ReportThread reportThread = new ReportThread();
    private static int exit;
    private static volatile OutOfMemoryError handlingOOM;
    private static String oomMsg = "Uncaught java.lang.OutOfMemoryError\n";


    public static final class ExitThread extends Thread {
        private volatile int code = -1;

        public ExitThread() {
            super("OOMExit");
            setDaemon(true);
            start();
        }

        public void exit(int code) {
            this.code = code;
            LockSupport.unpark(this);
        }

        @Override public void run() {
            int code;
            while ((code=this.code) == -1)
                LockSupport.park();
            try {
                Thread.sleep(1_000);
            } catch (Throwable ignored) {}
            System.exit(code);
        }
    }

    private static final class ReportThread extends Thread {
        private boolean failedReport = false;
        private volatile boolean exit;
        private volatile OutOfMemoryError oom;

        public ReportThread() {
            super("OOMReporter");
            setDaemon(true);
            setPriority(MAX_PRIORITY);
            start();
        }

        @SuppressWarnings("unused") public void exit() {
            exit = true;
            LockSupport.unpark(this);
        }

        @Override public void run() {
            try {
                while (!exit) {
                    OutOfMemoryError oom;
                    while ((oom=this.oom) == null)
                        LockSupport.park();
                    if (!failedReport) {
                        try {
                            oom.printStackTrace(System.err);
                        } catch (Throwable ignored) {
                            failedReport = true;
                        }
                    }
                }
            } catch (OutOfMemoryError ignored) {}
        }

        public void asyncReport(OutOfMemoryError e) {
            oom = e;
            LockSupport.unpark(this);
        }
    }

    public static void notifyOOM(OutOfMemoryError error) {
        // this Absolutely disgusting lock implementation.
        //  - java.util.concurrent.Lock may create objects (to implement queueing)
        //  - synchronized() may do allocations in the C++ code.
        //    I'm not certain if it is guaranteed to not fail after an OOM
        //  - VarHandle.getAndSet/compareAndExchange() will only link here and linking
        //    could fail due to the OOM state.
        acquire:
        while (true) {
            if (handlingOOM == null)
                handlingOOM = error;
            for (int i = 0; i < 1024; i++) {
                if (handlingOOM != error)
                    continue acquire; // race detected
            }
            break; // won the race
        }
        try { // mutual exclusion
            reportThread.asyncReport(error);
            for (int i = 0; i < callbackCount; i++) {
                try {
                    callback[i].run();
                } catch (OutOfMemoryError ignored) {
                } catch (Exception e) {
                    try {
                        System.err.print(e.getClass().getName());
                        System.err.print(" while notifying OOM to ");
                        System.err.print(callbackName[i]);
                    } catch (Throwable ignored) {}
                }
            }
        } finally {
            handlingOOM = null; // release "lock"
            if (exit >= 0)
                exitThread.exit(exit);
            try {
                System.err.print(oomMsg);
                System.err.flush();
            } catch (Throwable ignored) {}
        }
    }

    public static void exitOnOOM(int exitCode) {
        exit = exitCode;
        oomMsg = "Uncaught java.lang.OutOfMemoryError, will exit with code "+exitCode+"\n";
    }

    public static void onOOM(String name, Runnable action) {
        int i = nextCallbackPos.getAndIncrement();
        if (i >= callback.length) {
            nextCallbackPos.getAndAdd(-1);
            throw new UnsupportedOperationException("Too many OOM callbacks");
        }
        callback    [i] = action;
        callbackName[i] = name;
        callbackCount   = i;
    }
}
