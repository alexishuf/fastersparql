package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.Flushable;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.toHexString;
import static java.lang.Math.min;
import static java.lang.System.identityHashCode;
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
 * <p>
 * <p/>Usage: Get a {@link RoleJournal} with {@link DebugJournal#role(String)} and log using
 * {@link RoleJournal#write(Object, long, Object, long)} method and its overloads. Call
 * {@link RoleJournal#close()} once there is no point in dumping messages written to that role in
 * a {@link #dump(Appendable, int)}s. While one may instantiate a {@link DebugJournal}, there is
 *  convenience global instance at {@link DebugJournal#SHARED}.
 */
@SuppressWarnings("unused") // this class should be used only when debugging concurrency issues
public class DebugJournal {
    public static final DebugJournal SHARED = new DebugJournal();
    /**
     * Default lines capacity for {@link #role(String)}.
     */
    public static final int DEF_LINES = 512;

    private static final int UNLOCKED = 0;
    private static final int LOCKED   = 1;
    private static final int DUMPING  = 2;
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

    public RoleJournal role(String name) { return role(name, DEF_LINES); }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted") private boolean lock(int target) {
        int actual;
        while ((actual=(int)LOCK.compareAndExchangeAcquire(this, UNLOCKED, target)) != UNLOCKED) {
            if (actual == DUMPING) return false;
            else                   Thread.onSpinWait();
        }
        return true;
    }


    public RoleJournal role(String name, int lines) {
        if (!lock(LOCKED))
            return new RoleJournal(name, 2);
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

    /** Equivalent to calling {@link RoleJournal#close()} on all journals not yet closed. */
    public void closeAll() {
        if (!lock(LOCKED))
            return;
        try {
            for (RoleJournal j : roleJournals)
                j.closed = true;
            roleJournals.clear();
            roles.clear();
        } finally { LOCK.setRelease(this, 0); }
    }

    public int tick() { return tick; }

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
        if (!lock(DUMPING))
            return "<<<concurrent DebugJournal.toString() call>>>";
        try {
            String tickFmt = "T=%0" + Integer.toString(tick).length() + "d";
            String colFmt = " | %" + columnWidth + "s";

            // write header
            StringBuilder sb = new StringBuilder();
            appendHeaders(sb, tickFmt, colFmt);

            // write rs
            ParallelIterator it = new ParallelIterator();
            for (int i = 0, t = it.currentTick(); it.hasNext(); t = it.nextTick(t), ++i) {
                sb.append(String.format(tickFmt, t));
                it.render(sb, columnWidth, t);
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

    private final class ParallelIterator {
        int[] physRows;
        int[] physSize;

        public ParallelIterator() {
            int n = roleJournals.size();
            physRows = new int[n];
            physSize = new int[n];
            for (int i = 0; i < n; i++) {
                var journal  = roleJournals.get(i);
                int end      = (int)RoleJournal.END.getOpaque(journal);
                int capacity = journal.ticksAndFlags.length >> 1;
                physRows[i] = Math.max(0, end-capacity)%capacity;
                physSize[i] = min(end, capacity);
            }
        }

        boolean hasNext() {
            for (int size : physSize) {
                if (size > 0) return true;
            }
            return false;
        }

        int currentTick() {
            int t = Integer.MAX_VALUE;
            for (int i = 0; i < physRows.length; i++) {
                if (physSize[i] > 0)
                    t = min(t, roleJournals.get(i).ticksAndFlags[physRows[i]<<1]);
            }
            return t;
        }

        int nextTick(int curr) {
            for (int i = 0; i < physRows.length; i++) {
                if (physSize[i] > 0) {
                    var journal = roleJournals.get(i);
                    if (journal.ticksAndFlags[physRows[i]<<1] == curr) {
                        int capacity = journal.ticksAndFlags.length >> 1;
                        physRows[i] = (physRows[i]+1) % capacity;
                        --physSize[i];
                    }
                }
            }
            return currentTick();
        }

        void render(StringBuilder dst, int width, int currentTick) {
            String whitespace = " ".repeat(Math.max(0, width));
            for (int i = 0; i < physRows.length; i++) {
                dst.append(" | ");
                RoleJournal j = roleJournals.get(i);
                int physRow = physRows[i];
                if (j.ticksAndFlags[physRow<<1] == currentTick) {
                    int before = dst.length();
                    j.render(dst, width, physRow);
                    dst.append(" ".repeat(Math.max(0, width - (dst.length() - before))));
                } else {
                    dst.ensureCapacity(dst.length()+width);
                    dst.append(whitespace);
                }
            }
        }

    }

    private void dropRole(String name) {
        if (!lock(LOCKED))
            return;
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
        void render(int maxWidth, StringBuilder dest, Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4);
        void render(int maxWidth, StringBuilder dest, Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2,          Object o3, Object o4);
        void render(int maxWidth, StringBuilder dest, Object o1,          Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4);
        void render(int maxWidth, StringBuilder dest, Object o1,          Object o2,          Object o3, Object o4);
    }

    public static class DefaultRenderer implements Renderer {
        public static final DefaultRenderer INSTANCE = new DefaultRenderer();

        private static StringBuilder writeObj(StringBuilder sb, Object o, int maxWidth) {
            if (o == null)
                return sb;
            if (o instanceof String) {
                sb.append(o);
            } else {
                String str = switch (o) {
                    case Throwable t  -> t.getClass().getSimpleName();
                    case Term t       -> t.toSparql().toString();
                    case StreamNode n -> n.label(StreamNodeDOT.Label.MINIMAL);
                    default           -> o.toString();
                };
                if (str.length() > maxWidth) {
                    int side = Math.max(1, (maxWidth - 12)/2);
                    sb.append(str, 0, side).append("...")
                            .append(str, str.length() - side, str.length()).append('@')
                            .append(toHexString(identityHashCode(o)));
                } else {
                    sb.append(str);
                }
            }
            if (!sb.isEmpty() && sb.charAt(sb.length()-1) != '=')
                sb.append(' ');
            return sb;
        }

        private static int objWidth(int max, Object o1, Object o2, Object o3, Object o4) {
            int objs = 0;
            if      (o1 instanceof String || o1 instanceof Number) max -= o1.toString().length();
            else if (o1 != null)                                   objs++;
            if      (o2 instanceof String || o2 instanceof Number) max -= o2.toString().length();
            else if (o2 != null)                                   objs++;
            if      (o3 instanceof String || o3 instanceof Number) max -= o3.toString().length();
            else if (o3 != null)                                   objs++;
            if      (o4 instanceof String || o4 instanceof Number) max -= o4.toString().length();
            else if (o4 != null)                                   objs++;
            return Math.max(8, (max-4)/Math.max(1, objs))-1;
        }

        private static String render(long l, LongRenderer lr) {
            if (lr != null) return lr.render(l);
            return l > 1_000 ? "0x"+Long.toHexString(l) : Long.toString(l);
        }

        @Override public void render(int maxWidth, StringBuilder dest, Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4) {
            String l1s = render(l1, l1Renderer);
            String l2s = render(l2, l2Renderer);
            maxWidth -= l1s.length() + l2s.length();
            int objWidth = objWidth(maxWidth, o1, o2, o3, o4);
            writeObj(dest, o1, objWidth).append(l1s).append(' ');
            writeObj(dest, o2, objWidth).append(l2s).append(' ');
            writeObj(dest, o3, objWidth);
            writeObj(dest, o4, objWidth);
        }

        @Override public void render(int maxWidth, StringBuilder dest, Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, Object o3, Object o4) {
            String l1s = render(l1, l1Renderer);
            maxWidth -= l1s.length();
            int objWidth = objWidth(maxWidth, o1, o2, o3, o4);
            writeObj(dest, o1, objWidth).append(l1s).append(' ');
            writeObj(dest, o2, objWidth);
            writeObj(dest, o3, objWidth);
            writeObj(dest, o4, objWidth);
        }

        @Override public void render(int maxWidth, StringBuilder dest, Object o1, Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4) {
            String l2s = render(l2, l2Renderer);
            maxWidth -= l2s.length();
            int objWidth = objWidth(maxWidth, o1, o2, o3, o4);
            writeObj(dest, o1, objWidth);
            writeObj(dest, o2, objWidth).append(l2s);
            writeObj(dest, o3, objWidth);
            writeObj(dest, o4, objWidth);
        }

        @Override public void render(int maxWidth, StringBuilder dest, Object o1, Object o2, Object o3, Object o4) {
            int objWidth = objWidth(maxWidth, o1, o2, o3, o4);
            writeObj(dest, o1, objWidth);
            writeObj(dest, o2, objWidth);
            writeObj(dest, o3, objWidth);
            writeObj(dest, o4, objWidth);
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
        private final LongRenderer[] longRenderers;
        private final int[] ticksAndFlags;
        private Renderer renderer = DefaultRenderer.INSTANCE;
        @SuppressWarnings("FieldMayBeFinal") private int end0 = 1;
        private boolean closed;

        public RoleJournal(String name, int lines) {
            this.name  = name;
            this.messages = new Object[4*lines];
            this.longs = new long[2*lines];
            this.longRenderers = new LongRenderer[2*lines];
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

        void render(StringBuilder dest, int colWidth, int physRow) {
            int base = physRow*4;
            Object o1 = messages[base  ], o2 = messages[base+1];
            Object o3 = messages[base+2], o4 = messages[base+3];
            base = physRow*2;
            long l1 = longs[base], l2 = longs[base+1];
            LongRenderer l1R = longRenderers[base], l2R = longRenderers[base+1];
            switch (ticksAndFlags[base+1]) {
                case NO_L     -> renderer.render(colWidth, dest, o1, o2, o3, o4);
                case HAS_L1   -> renderer.render(colWidth, dest, o1, l1, l1R, o2, o3, o4);
                case HAS_L2   -> renderer.render(colWidth, dest, o1, o2, l2, l2R, o3, o4);
                case HAS_L1L2 -> renderer.render(colWidth, dest, o1, l1, l1R, o2, l2, l2R, o3, o4);
            }
        }

        public void write(Object o1, long l1, long l2)                                   { write(HAS_L1L2, o1, l1, null, null, l2, null, null, null); }
        public void write(Object o1, long l1, long l2, Object o3, Object o4)             { write(HAS_L1L2, o1, l1, null, null, l2, null, o3, o4); }
        public void write(Object o1, long l1, Object o2, long l2)                        { write(HAS_L1L2, o1, l1, null, o2, l2, null, null, null); }
        public void write(Object o1, long l1, Object o2, long l2, Object o3, Object o4)  { write(HAS_L1L2, o1, l1, null, o2, l2, null, o3, o4); }
        public void write(Object o1, long l1)                                            { write(HAS_L1, o1, l1, null, null, 0, null, null, null); }
        public void write(Object o1, long l1, Object o2)                                 { write(HAS_L1, o1, l1, null, o2, 0, null, null, null); }
        public void write(Object o1, long l1, Object o2, Object o3)                      { write(HAS_L1, o1, l1, null, o2, 0, null, o3, null); }
        public void write(Object o1, long l1, Object o2, Object o3, Object o4)           { write(HAS_L1, o1, l1, null, o2, 0, null, o3, o4); }
        public void write(Object o1)                                                     { write(NO_L, o1, 0, null, null, 0, null, null, null); }
        public void write(Object o1, Object o2)                                          { write(NO_L, o1, 0, null, o2, 0, null, null, null); }
        public void write(Object o1, Object o2, Object o3)                               { write(NO_L, o1, 0, null, o2, 0, null, o3, null); }
        public void write(Object o1, Object o2, Object o3, Object o4)                    { write(NO_L, o1, 0, null, o2, 0, null, o3, o4); }

        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, long l2)                                    { write(HAS_L1L2, o1, l1, l1Renderer, null, l2, null, null, null); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, long l2, @Nullable LongRenderer l2Renderer) { write(HAS_L1L2, o1, l1, l1Renderer, null, l2, l2Renderer, null, null); }
        public void write(Object o1, long l1, long l2, @Nullable LongRenderer l2Renderer)                                    { write(HAS_L1L2, o1, l1, null, null, l2, l2Renderer, null, null); }

        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, long l2, Object o3, Object o4)                                    { write(HAS_L1L2, o1, l1, l1Renderer, null, l2, null, o3, o4); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4) { write(HAS_L1L2, o1, l1, l1Renderer, null, l2, l2Renderer, o3, o4); }
        public void write(Object o1, long l1, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4)                                    { write(HAS_L1L2, o1, l1, null, null, l2, l2Renderer, o3, o4); }

        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2)                                    { write(HAS_L1L2, o1, l1, l1Renderer, o2, l2, null, null, null); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2, @Nullable LongRenderer l2Renderer) { write(HAS_L1L2, o1, l1, l1Renderer, o2, l2, l2Renderer, null, null); }
        public void write(Object o1, long l1, Object o2, long l2, @Nullable LongRenderer l2Renderer)                                    { write(HAS_L1L2, o1, l1, null, o2, l2, l2Renderer, null, null); }

        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2, Object o3, Object o4)                                    { write(HAS_L1L2, o1, l1, l1Renderer, o2, l2, null, o3, o4); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4) { write(HAS_L1L2, o1, l1, l1Renderer, o2, l2, l2Renderer, o3, o4); }
        public void write(Object o1, long l1, Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4)                                    { write(HAS_L1L2, o1, l1, null, o2, l2, l2Renderer, o3, o4); }

        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer)                                            { write(HAS_L1, o1, l1, l1Renderer, null, 0, null, null, null); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2)                                 { write(HAS_L1, o1, l1, l1Renderer, o2, 0, null, null, null); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, Object o3)                      { write(HAS_L1, o1, l1, l1Renderer, o2, 0, null, o3, null); }
        public void write(Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, Object o3, Object o4)           { write(HAS_L1, o1, l1, l1Renderer, o2, 0, null, o3, o4); }

        private void write(int flags, Object o1, long l1, @Nullable LongRenderer l1Renderer, Object o2, long l2, @Nullable LongRenderer l2Renderer, Object o3, Object o4) {
            int physRow = (int)END.getAndAdd(this, 1) % (ticksAndFlags.length>>1);
            int base = physRow*2;
            ticksAndFlags[base  ] = tick++;
            ticksAndFlags[base+1] = flags;
            longs        [base  ] = l1;
            longs        [base+1] = l2;
            longRenderers[base  ] = l1Renderer;
            longRenderers[base+1] = l2Renderer;

            base = physRow * 4;
            messages[base  ] = o1;
            messages[base+1] = o2;
            messages[base+2] = o3;
            messages[base+3] = o4;
            assert !(o2 instanceof Integer || o2 instanceof Long
                  || o3 instanceof Integer || o3 instanceof Long) : "Boxing a number, reorder args";
        }
    }
}
