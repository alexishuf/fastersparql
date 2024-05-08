package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.util.concurrent.DebugJournal.RoleJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

@SuppressWarnings("unused")
public class ThreadJournal {
    public static final boolean ENABLED = false;

    private static final class Node {
        @Nullable Thread key;
        @Nullable RoleJournal value;
        @Nullable Node next;

        public Node(@Nullable Thread key, @Nullable RoleJournal value) {
            this.key = key;
            this.value = value;
        }
    }

    private static final int            N_BUCKETS = 1024;
    private static final int         BUCKETS_MASK = N_BUCKETS-1;
    private static final VarHandle         WRITER;
    private static       int          writerDepth;
    private static       Thread       plainWriter;
    private static final Node[]    thread2journal;
    private static final Node            resetTmp = new Node(null, null);

    static {
        try {
            WRITER = MethodHandles.lookup().findStaticVarHandle(ThreadJournal.class, "plainWriter", Thread.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
        assert Integer.bitCount(N_BUCKETS) == 1;
        thread2journal = new Node[1024];
        for (int i = 0; i < thread2journal.length; i++)
            thread2journal[i] = new Node(null, null);
    }

    private static void lockForWrite() {
        Thread me = Thread.currentThread(), owner;
        while ((owner=(Thread)WRITER.compareAndExchangeAcquire(null, me)) != null && owner != me)
            Thread.onSpinWait();
        ++writerDepth;
    }

    private static void unlockForWrite() {
        assert WRITER.getOpaque() == Thread.currentThread() : "current thread does not own lock";
        int nextDepth = --writerDepth;
        if (nextDepth < 0) {
            writerDepth = 0;
            assert false : "unlockForWrite() calls exceed lockForWrite() calls";
        } else if (nextDepth == 0) {
            WRITER.setRelease((Thread)null);
        }
    }

    /**
     * Closes all {@link RoleJournal} assigned to threads. Future attempts to journal from such
     * threads will transparently trigger the assignment of a new journal.
     */
    public static void resetJournals() {
        lockForWrite();
        try {
            resetJournals0();
        } finally { unlockForWrite(); }
    }

    private static void resetJournals0() {
        for (Node node : thread2journal) {
            resetTmp.next  = node.next;
            node.next      = null;
            resetTmp.key   = node.key;
            node.key       = null;
            resetTmp.value = node.value;
            node.value     = null;
            while (true) {
                var j = resetTmp.value;
                resetTmp.value = null;
                if (j != null)
                    j.close();
                var next = resetTmp.next;
                if (next != null) {
                    resetTmp.key   = next.key;
                    resetTmp.value = next.value;
                    resetTmp.next  = next.next;
                } else {
                    resetTmp.key   = null;
                    resetTmp.value = null;
                    break;
                }
            }
        }
    }

    /** Write all logged messages to {@code dest} in lanes of the given {@code columnWidth} and
     *  calls {@link #resetJournals()}. */
    public static void dumpAndReset(Appendable dest, int columnWidth) {
        lockForWrite();
        try {
            DebugJournal.SHARED.dump(dest, columnWidth);
            resetJournals();
        } finally { unlockForWrite(); }
    }

    public static String render(Object... args) {
        var sb = new StringBuilder();
        for (Object a : args) {
            String str = render(a);
            int len = sb.length();
            if (len > 0) {
                char c = sb.charAt(len-1);
                if (c != ' ' && c != '=' && !str.isEmpty() && str.charAt(0) != ' ')
                    sb.append(' ');
            }
            sb.append(str);
        }
        return sb.toString();
    }

    public static String render(Object o) {
        return o instanceof JournalNamed n ? n.journalName()
                : DebugJournal.DefaultRenderer.INSTANCE.renderObj(o);
    }

    private static RoleJournal journal() {
        Thread thread = Thread.currentThread();
        Node root = thread2journal[(int)thread.threadId()&BUCKETS_MASK];
        RoleJournal journal;
        for (Node node = root; node != null; node = node.next) {
            if (node.key == thread && (journal=node.value) != null)  return journal;
        }

        lockForWrite();
        try {
            // must create journal for this thread
            String name = thread.getName();
            if (name.length() > 20)
                name = name.substring(0, 4) + "..." + name.substring(name.length() - (20 - 7));
            journal = DebugJournal.SHARED.role(name);
            for (Node node = root; node != null; node = node.next) {
                if (node.key == null) {
                    node.key = thread;
                    node.value = journal;
                    break;
                } else if (node.key == thread) {
                    var older = node.value;
                    if   (older == null) node.value = journal;
                    else                    journal = older;
                    break;
                } else if (node.next == null) {
                    node.next = new Node(thread, journal);
                    break;
                }
            }
            return journal;
        } finally { unlockForWrite(); }
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, long l2) {
        if (ENABLED)
            journal().write(o1, l1, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1R, long l2, @Nullable LongRenderer l2R) {
        if (ENABLED)
            journal().write(o1, l1, l1R, l2, l2R);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1R, long l2) {
        if (ENABLED)
            journal().write(o1, l1, l1R, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, long l2, @Nullable LongRenderer l2R) {
        if (ENABLED)
            journal().write(o1, l1, l2, l2R);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, long l2, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, l2, o3, o4);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2) {
        if (ENABLED)
            journal().write(o1, l1, o2, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2, @Nullable LongRenderer l2r) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2, l2, l2r);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2, l2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2, @Nullable LongRenderer l2r) {
        if (ENABLED)
            journal().write(o1, l1, o2, l2, l2r);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, o2, l2, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2, @Nullable LongRenderer l2r, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2, l2, l2r, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, long l2, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2, l2, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, long l2, @Nullable LongRenderer l2r, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, o2, l2, l2r, o3, o4);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1) {
        if (ENABLED)
            journal().write(o1, l1);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r) {
        if (ENABLED)
            journal().write(o1, l1, l1r);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2) {
        if (ENABLED)
            journal().write(o1, l1, o2);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, Object o3) {
        if (ENABLED)
            journal().write(o1, l1, o2, o3);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, Object o3) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2, o3);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, Object o2, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, o2, o3, o4);
    }
    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, long l1, @Nullable LongRenderer l1r, Object o2, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, l1, l1r, o2, o3, o4);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1) {
        if (ENABLED)
            journal().write(o1);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, Object o2) {
        if (ENABLED)
            journal().write(o1, o2);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, Object o2, Object o3) {
        if (ENABLED)
            journal().write(o1, o2, o3);
    }

    /** Delegates to the {@code write()} method of the {@link RoleJournal} of the calling thread. */
    public static void journal(Object o1, Object o2, Object o3, Object o4) {
        if (ENABLED)
            journal().write(o1, o2, o3, o4);
    }
}
