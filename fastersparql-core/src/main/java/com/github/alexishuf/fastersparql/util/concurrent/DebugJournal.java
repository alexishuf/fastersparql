package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.Flushable;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;
import static java.lang.invoke.MethodHandles.lookup;


/**
 * A minimal overhead, lock-free, logical time logger. It does no allocation outside constructors,
 * and client code should not allocate objects when calling {@code write()} methods. Unlike SLF4J,
 * logs are rendered in a track layout:
 * <p/>
 *
 * <pre>
 *       |              T1 |              T1
 *   T=0 | read running=0  |
 *   T=1 |                 | read running=0
 *   T=3 |                 | set running=1
 *   T=3 | set running=1   |
 * </pre>
 *
 * The only synchronization happens when CPUs enforce cache coherence when {@link RoleJournal}s
 * on distinct threads race to update the logical clock with a non-synchronized {@code ++tick}.
 * This minimal synchronization reduces chances that logging affects the observed buggy behavior,
 * but also means that two log entries with a happens-before relation (i.e., correctly synchronized)
 * can appear in the same logical time if a third thread also logging causes a lost write on
 * {@code tick}.
 *
 * <p/>Usage: Get a {@link RoleJournal} with {@link DebugJournal#role(String)} and log using
 * {@link RoleJournal#write(Object, long, Object, long)} method and its overloads. Call
 * {@link RoleJournal#close()} once there is no point in dumping messages written to that role in
 * a {@link #dump(Appendable, int)}s. While one may instantiate a {@link DebugJournal}, there is
 *  convenience global instance at {@link DebugJournal#SHARED}.
 */
@SuppressWarnings("unused") // this class should be used only when debugging concurrency issues
public class DebugJournal {
    public static final DebugJournal SHARED = new DebugJournal();
    private static final VarHandle LOCK;

    static {
        try {
            LOCK = lookup().findVarHandle(DebugJournal.class, "plainLockStorage", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") private int plainLockStorage = 0;
    private final List<String> roles = new ArrayList<>();
    private final List<RoleJournal> roleJournals = new ArrayList<>();
    private int tick = 0;

    public Watchdog watchdog(int columnWidth) {
        return new Watchdog(() -> dump(columnWidth));
    }

    public RoleJournal role(String name) { return role(name, 2048); }

    public RoleJournal role(String name, int lines) {
        while (!LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            int i = roles.indexOf(name);
            if (i >= 0)
                return roleJournals.get(i);
            roles.add(name);
            RoleJournal j  = new RoleJournal(name, lines);
            roleJournals.add(j);
            return j;
        } finally { LOCK.setRelease(this, 0); }
    }

    /**
     * Equivalent to calling {@link RoleJournal#close()} on all {@link RoleJournal}s not yet closed.
     */
    public void reset() {
        while (!LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            for (RoleJournal j : roleJournals)
                j.closed = true;
            roleJournals.clear();
            roles.clear();
        } finally { LOCK.setRelease(this, 0); }
    }

    public void dump(int columnWidth) { dump(System.err, columnWidth); }

    public void dump(Appendable dest, int columnWidth) {
        try {
            dest.append('\n');
            dest.append(toString(columnWidth));
            if (dest instanceof Flushable f) f.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString(int columnWidth) {
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
        try {
            String tickFmt = "T=%0" + Integer.toString(tick).length() + "d";
            String colFmt = " | %" + columnWidth + "s";

            // write header
            StringBuilder sb = new StringBuilder();
            appendHeaders(sb, tickFmt, colFmt);

            // write rs
            int[] rs = new int[roles.size()];
            for (int i = 0,  t = currentTick(rs); !ended(rs); t = currentTick(increment(t, rs)), ++i) {
                sb.append(String.format(tickFmt, t));
                render(sb, columnWidth, t, rs);
                sb.append('\n');
                if (i == 100)  {
                    i = -1;
                    appendHeaders(sb, tickFmt, colFmt);
                }
            }
            return sb.toString();
        } finally {
            LOCK.setRelease(this, 0);
        }
    }

    private void appendHeaders(StringBuilder sb, String tickFmt, String colFmt) {
        sb.append(" ".repeat(String.format(tickFmt, 0).length()));
        for (String name : roles)
            sb.append(String.format(colFmt, name));
        sb.append('\n');
    }

    private boolean ended(int[] rows) {
        for (int i = 0; i < rows.length; i++) {
            if (rows[i] < roleJournals.get(i).size())
                return false;
        }
        return true;
    }

    private int currentTick(int[] rows) {
        int t = Integer.MAX_VALUE;
        for (int i = 0; i < rows.length; i++) {
            RoleJournal j = roleJournals.get(i);
            if (rows[i] < j.size())
                t = min(t, j.tick(rows[i]));
        }
        return t;
    }

    private void render(StringBuilder dest, int width, int tick, int[] rows) {
        String whitespace = " ".repeat(Math.max(0, width));
        for (int i = 0; i < rows.length; i++) {
            dest.append(" | ");
            RoleJournal j = roleJournals.get(i);
            if (j.tick(rows[i]) == tick) {
                int before = dest.length();
                j.render(dest, rows[i]);
                dest.append(" ".repeat(Math.max(0, width - (dest.length() - before))));
            } else {
                dest.ensureCapacity(dest.length()+width);
                dest.append(whitespace);
            }
        }
    }

    private int[] increment(int tick, int[] rows) {
        for (int i = 0; i < rows.length; i++) {
            if (roleJournals.get(i).tick(rows[i]) == tick) ++rows[i];
        }
        return rows;
    }

    private void dropRole(String name) {
        while (!LOCK.weakCompareAndSetAcquire(this, 0, 1)) Thread.onSpinWait();
        try {
            int i = roles.indexOf(name);
            if (i >= 0) {
                roles.remove(i);
                roleJournals.remove(i);
            }
        } finally {
            LOCK.setRelease(this, 0);
        }
    }

    public interface Renderer {
        void render(StringBuilder dest, Object o1, long l1, Object o2, long l2, Object o3, Object o4);
        void render(StringBuilder dest, Object o1, long l1, Object o2,          Object o3, Object o4);
        void render(StringBuilder dest, Object o1,          Object o2, long l2, Object o3, Object o4);
        void render(StringBuilder dest, Object o1,          Object o2,          Object o3, Object o4);
    }

    public static class DefaultRenderer implements Renderer {
        public static final DefaultRenderer INSTANCE = new DefaultRenderer();

        private static StringBuilder writeObj(StringBuilder sb, Object o1) {
            if (o1 != null) {
                if (o1 instanceof Term t)
                    sb.append(t.toSparql());
                else
                    sb.append(o1);
                if (sb.length() > 0 && sb.charAt(sb.length()-1) != '=')
                    sb.append(' ');
            }
            return sb;
        }

        @Override public void render(StringBuilder dest, Object o1, long l1, Object o2, long l2, Object o3, Object o4) {
            writeObj(dest, o1).append(l1).append(' ');
            writeObj(dest, o2).append(l2).append(' ');
            writeObj(dest, o3);
            writeObj(dest, o4);
        }

        @Override public void render(StringBuilder dest, Object o1, long l1, Object o2, Object o3, Object o4) {
            writeObj(dest, o1).append(l1).append(' ');
            writeObj(dest, o2);
            writeObj(dest, o3);
            writeObj(dest, o4);
        }

        @Override public void render(StringBuilder dest, Object o1, Object o2, long l2, Object o3, Object o4) {
            writeObj(dest, o1);
            writeObj(dest, o2).append(l2);
            writeObj(dest, o3);
            writeObj(dest, o4);
        }

        @Override public void render(StringBuilder dest, Object o1, Object o2, Object o3, Object o4) {
            writeObj(dest, o1);
            writeObj(dest, o2);
            writeObj(dest, o3);
            writeObj(dest, o4);
        }
    }

    public final class RoleJournal {
        public static final VarHandle END;

        static {
            try {
                END = lookup().findVarHandle(RoleJournal.class, "end0", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private static final int NO_L = 0;
        private static final int HAS_L1 = 1;
        private static final int HAS_L2 = 2;
        private static final int HAS_L1L2 = HAS_L1|HAS_L2;

        private final String name;
        private final Object[] messages;
        private final long[] longs;
        private final int[] ticksAndFlags;
        private Renderer renderer = DefaultRenderer.INSTANCE;
        @SuppressWarnings("FieldMayBeFinal") private int end0 = 1;
        private boolean closed;

        public RoleJournal(String name, int lines) {
            this.name  = name;
            this.messages = new Object[4*lines];
            this.longs = new long[2*lines];
            this.ticksAndFlags = new int[2*lines];
            this.messages[0] = name+" journal started";
            this.ticksAndFlags[0] = tick;
        }

        public boolean isClosed() {
            return closed;
        }

        public void close() {
            dropRole(name);
            closed = true;
        }

        public @This RoleJournal renderer(Renderer renderer) {
            this.renderer = renderer;
            return this;
        }

        public int size() {
            return Math.min((int)END.getOpaque(this), ticksAndFlags.length>>1);
        }

        public int tick(int row) {
            int capacity = ticksAndFlags.length >> 1;
            int physBegin = Math.max(0, (int) END.getOpaque(this) - capacity);
            int physRow = (physBegin+row) % capacity;
            return ticksAndFlags[2*physRow];
        }

        public void render(StringBuilder dest, int row) {
            int capacity = ticksAndFlags.length>>1;
            int physBegin = Math.max(0, (int) END.getOpaque(this) - capacity);
            int physRow = (physBegin+row) % capacity;
            int base = physRow*4;
            Object o1 = messages[base  ], o2 = messages[base+1];
            Object o3 = messages[base+2], o4 = messages[base+3];
            base = physRow*2;
            long l1 = longs[base], l2 = longs[base+1];
            switch (ticksAndFlags[base+1]) {
                case NO_L     -> renderer.render(dest, o1, o2, o3, o4);
                case HAS_L1   -> renderer.render(dest, o1, l1, o2, o3, o4);
                case HAS_L2   -> renderer.render(dest, o1, o2, l2, o3, o4);
                case HAS_L1L2 -> renderer.render(dest, o1, l1, o2, l2, o3, o4);
            }
        }

        public void write(Object o1, long l1, long l2)                                   { write(HAS_L1L2, o1, l1, null, l2, null, null); }
        public void write(Object o1, long l1, long l2, Object o3, Object o4)             { write(HAS_L1L2, o1, l1, null, l2, o3, o4); }
        public void write(Object o1, long l1, Object o2, long l2)                        { write(HAS_L1L2, o1, l1, o2, l2, null, null); }
        public void write(Object o1, long l1, Object o2, long l2, Object o3, Object o4)  { write(HAS_L1L2, o1, l1, o2, l2, o3, o4); }
        public void write(Object o1, long l1)                                            { write(HAS_L1, o1, l1, null, 0, null, null); }
        public void write(Object o1, long l1, Object o2)                                 { write(HAS_L1, o1, l1, o2, 0, null, null); }
        public void write(Object o1, long l1, Object o2, Object o3)                      { write(HAS_L1, o1, l1, o2, 0, o3, null); }
        public void write(Object o1, long l1, Object o2, Object o3, Object o4)           { write(HAS_L1, o1, l1, o2, 0, o3, o4); }
        public void write(Object o1)                                                     { write(NO_L, o1, 0, null, 0, null, null); }
        public void write(Object o1, Object o2)                                          { write(NO_L, o1, 0, o2, 0, null, null); }
        public void write(Object o1, Object o2, Object o3)                               { write(NO_L, o1, 0, o2, 0, o3, null); }
        public void write(Object o1, Object o2, Object o3, Object o4)                    { write(NO_L, o1, 0, o2, 0, o3, o4); }

        private void write(int flags, Object o1, long l1, Object o2, long l2, Object o3, Object o4) {
            int physRow = (int)END.getAndAdd(this, 1) % (ticksAndFlags.length>>1);
            int base = physRow*2;
            ticksAndFlags[base  ] = tick++;
            ticksAndFlags[base+1] = flags;
            longs[base  ] = l1;
            longs[base+1] = l2;

            base = physRow * 4;
            messages[base  ] = o1;
            messages[base+1] = o2;
            messages[base+2] = o3;
            messages[base+3] = o4;

        }
    }
}
