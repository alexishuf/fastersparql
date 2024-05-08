package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.results.serializer.TsvSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.TempOwner;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.identityHashCode;
import static java.lang.Thread.onSpinWait;

public class ResultJournal {
    public static final boolean ENABLED = false;

    private static final ConcurrentHashMap<Object, EmitterJournal> JOURNALS
            = new ConcurrentHashMap<>();

    private static final class EmitterJournal {
        private static final VarHandle LOCK;
        static {
            try {
                LOCK = MethodHandles.lookup().findVarHandle(EmitterJournal.class, "plainLock", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        @SuppressWarnings("unused") private int plainLock;
        private final MutableRope log;
        private final TsvSerializer serializer;

        public EmitterJournal(Object emitter, Vars vars) {
            String label = emitter instanceof StreamNode sn ? sn.label(StreamNodeDOT.Label.SIMPLE)
                         : emitter.toString();
            log = new MutableRope(256);
            log.append("\n[[").append(label.replace("\n", "\n ")).append("]]\n");
            serializer = TsvSerializer.create().takeOwnership(this);
            serializer.init(vars, vars, vars.isEmpty());
            serializer.serializeHeader(log);
        }

        void close() {
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                log.close();
                serializer.recycle(this);
            } finally {
                LOCK.setRelease(this, 0);
            }
        }

        void add(BatchBinding rebind) {
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                log.append(rebind.toString()).append('\n');
            } finally { LOCK.setRelease(this, 0); }
        }
    }


    public static void initEmitter(Object emitter, Vars vars) {
        if (ENABLED)
            JOURNALS.computeIfAbsent(emitter, k -> new EmitterJournal(k, vars));
    }

    public static void rebindEmitter(Object emitter, BatchBinding binding) {
        EmitterJournal j;
        if (ENABLED && (j = JOURNALS.get(emitter)) != null)
            j.add(binding);
    }

    public static <B extends Batch<B>> void logBatch(Object emitter, B b) {
        EmitterJournal j;
        if (ENABLED && b != null && (j = JOURNALS.get(emitter)) != null) {
            while ((int) EmitterJournal.LOCK.compareAndExchangeAcquire(j, 0, 1) != 0)
                onSpinWait();
            try (var tmp = new TempOwner<>(b)) {
                j.serializer.serialize(b, tmp.tempOwner(), j.log);
                j.log.append("tick=").append(DebugJournal.SHARED.tick())
                        .append(", &b=0x").append(Integer.toHexString(identityHashCode(b)))
                        .append(", fromRow=").append(0)
                        .append(", nRows=").append((int) b.rows).append('\n');
            } finally { EmitterJournal.LOCK.setRelease(j, 0); }
        }
    }

    public static void dump(Appendable dst) throws IOException {
        for (EmitterJournal j : JOURNALS.values()) {
            dst.append(j.log.toString());
        }
    }

    public static void clear() {
        for (var it = JOURNALS.entrySet().iterator(); it.hasNext(); ) {
            try {
                it.next().getValue().close();
                it.remove();
            } catch (NoSuchElementException ignored) {  }
        }
    }


}
