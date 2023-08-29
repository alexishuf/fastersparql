package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.util.concurrent.DebugJournal.RoleJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
public class ThreadJournal {
    public static final boolean THREAD_JOURNAL = false;

    private static final ConcurrentHashMap<Thread, RoleJournal> thread2journal
            = new ConcurrentHashMap<>();

    /**
     * Closes all {@link RoleJournal} assigned to threads. Future attempts to journal from such
     * threads will transparently trigger the assignment of a new journal.
     */
    public static void closeThreadJournals() {
        for (var it = thread2journal.values().iterator(); it.hasNext(); ) {
            RoleJournal j = it.next();
            it.remove();
            j.close();
        }
    }

    /** Write all logged messages to {@code dest} in lanes of the given {@code columnWidth} and
     *  calls {@link #closeThreadJournals()}. */
    public static void dumpAndReset(Appendable dest, int columnWidth) {
        DebugJournal.SHARED.dump(dest, columnWidth);
        closeThreadJournals();
    }

    /** Create an un-{@link Watchdog#start(long)}ed watchdog for
     *  {@link #dumpAndReset(Appendable, int)}. */
    public static Watchdog watchdog(Appendable dest, int columnWidth) {
        return new Watchdog(() -> dumpAndReset(dest, columnWidth));
    }

    private static RoleJournal journal() {
        Thread thread = Thread.currentThread();
        return thread2journal.computeIfAbsent(thread, t -> {
            String name = thread.getName();
            if (name.length() > 20)
                name = name.substring(0, 4)+"..."+name.substring(name.length()-(20-7));
            return DebugJournal.SHARED.role(name);
        });
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, long l2) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1R, long l2, @Nullable LongRenderer l2R) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1R, l2, l2R);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1R, long l2) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1R, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, long l2, @Nullable LongRenderer l2R) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l2, l2R);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, long l2, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l2, o3, o4);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2, @Nullable LongRenderer l2r) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2, l2, l2r);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2, @Nullable LongRenderer l2r) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2, l2, l2r);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2, l2, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2, @Nullable LongRenderer l2r, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2, l2, l2r, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2, l2, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2, @Nullable LongRenderer l2r, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2, l2, l2r, o3, o4);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, Object o3) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2, o3);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, Object o3) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2, o3);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, o2, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, l1, l1r, o2, o3, o4);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1) {
        if (THREAD_JOURNAL)
            journal().write(o1);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, Object o2) {
        if (THREAD_JOURNAL)
            journal().write(o1, o2);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, Object o2, Object o3) {
        if (THREAD_JOURNAL)
            journal().write(o1, o2, o3);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, Object o2, Object o3, Object o4) {
        if (THREAD_JOURNAL)
            journal().write(o1, o2, o3, o4);
    }
}
