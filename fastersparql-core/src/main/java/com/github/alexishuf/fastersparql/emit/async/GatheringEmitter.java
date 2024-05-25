package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.HasFillingBatch;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindStateException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.maxRelease;
import static java.lang.System.identityHashCode;

public class GatheringEmitter<B extends Batch<B>>
        extends AsyncTaskEmitter<B, GatheringEmitter<B>> {
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[12];
    private short nConnectors, termConnectors;
    private int requestChunk = preferredRequestChunk();
    private int lastRebindSeq = -1;
    private Vars bindableVars = Vars.EMPTY;

    public static <B extends Batch<B>> Orphan<GatheringEmitter<B>>
    create(BatchType<B> bt, Vars vars) {
        return new Concrete<>(bt, vars);
    }
    protected GatheringEmitter(BatchType<B> batchType, Vars vars) {
        super(batchType, vars, EMITTER_SVC, CREATED, TASK_FLAGS);
    }
    private static final class Concrete<B extends Batch<B>> extends GatheringEmitter<B> implements Orphan<GatheringEmitter<B>> {
        private Concrete(BatchType<B> batchType, Vars vars) {super(batchType, vars);}
        @Override public GatheringEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected void doRelease() {
        for (int i = 0, n = nConnectors; i < n; i++) {
            Connector<B> c = connectors[i];
            if (c != null) c.doRelease();
        }
        super.doRelease();
    }

    public void subscribeTo(Orphan<? extends Emitter<B, ?>> upstream) {
        int st = lock();
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            var c = new Connector<>(upstream, this);
            assert isNovelUpstream(c.up);
            if (nConnectors == connectors.length)
                connectors = Arrays.copyOf(connectors, nConnectors<<1);
            int upChunk = c.up.preferredRequestChunk();
            if (upChunk > requestChunk)
                requestChunk = upChunk;
            connectors[nConnectors] = c;
            bindableVars = bindableVars.union(c.up.bindableVars());
            ++nConnectors;
        } finally { unlock(); }
    }

    private boolean isNovelUpstream(Emitter<B, ?> upstream) {
        for (int i = 0, n = nConnectors; i < n; i++) {
            if (connectors[i].up == upstream) return false;
        }
        return true;
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Arrays.stream(connectors, 0, nConnectors);
    }

    @Override public Vars bindableVars() { return bindableVars; }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (binding.sequence == lastRebindSeq)
            return; // duplicate rebind() due to diamond in emitter graph
        lastRebindSeq = binding.sequence;
        int st = resetForRebind(0, LOCKED_MASK);
        try {
            if ((st&(IS_INIT|IS_TERM)) == 0)
                throw new RebindStateException(this);
            requireAlive();
            termConnectors = 0;
            for (int i = 0, n = nConnectors; i < n; i++)
                connectors[i].rebind(binding);
        } finally {
            unlock();
        }
    }
    @Override public void rebindPrefetch(BatchBinding binding) {
        for (int i = 0, n = nConnectors; i < n; i++)
            connectors[i].up.rebindPrefetch(binding);
    }
    @Override public void rebindPrefetchEnd() {
        for (int i = 0, n = nConnectors; i < n; i++)
            connectors[i].up.rebindPrefetchEnd();
    }

    @Override public boolean cancel() {
        if ((dropAllQueuedAndGetState()&IS_TERM) != 0)
            return false;
        for (int i = 0, n = nConnectors; i < n; i++)
            connectors[i].up.cancel();
        return true;
    }

    @Override protected void resume() { updateRequests(); }

    private void updateRequests() {
        long requested = requested();
        if (requested > 0) {
            for (int i = 0, n = nConnectors; i < n; i++)
                connectors[i].updateRequest(requested);
        }
    }

    @Override protected void deliver(Orphan<B> orphan) {
        super.deliver(orphan);
        updateRequests();
    }

    /* --- --- --- Connector --- --- --- */

    private void onConnectorTerminated() {
        int st = lock(), termState = PENDING_COMPLETED;
        try {
            if (++termConnectors < nConnectors || (st&IS_TERM) != 0)
                return;
            for (int i = 0, n = nConnectors; i < n; i++) {
                var c = connectors[i];
                switch (c.state) {
                    case FAILED -> {
                        termState = PENDING_FAILED;
                        if (error == UNSET_ERROR && c.error != null)
                            error = c.error;
                    }
                    case CANCELLED -> {
                        if (termState != PENDING_FAILED) termState = PENDING_CANCELLED;
                    }
                    case COMPLETED -> {}
                    default -> throw new IllegalStateException("non-terminated connector");
                }
            }
        } finally { st = unlock(); }
        if (moveStateRelease(st, termState))
            awake(true);
    }


    private static final class Connector<B extends Batch<B>>
            implements Receiver<B>, HasFillingBatch<B> {
        private static final VarHandle CONN_PENDING;
        static {
            try {
                CONN_PENDING = MethodHandles.lookup().findVarHandle(Connector.class, "plainPending", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final @Nullable BatchMerger<B, ?> projector;
        private final GatheringEmitter<B> down;
        private final Emitter<B, ?> up;
        @SuppressWarnings("unused") private int plainPending;
        private int state;
        private @Nullable Throwable error;

        private Connector(Orphan<? extends Emitter<B, ?>> up, GatheringEmitter<B> down) {
            this.down = down;
            this.up = up.takeOwnership(this);
            this.up.subscribe(this);
            var projector = down.bt.projector(down.vars(), this.up.vars());
            this.projector = projector == null ? null : projector.takeOwnership(this);
        }

        void doRelease() {
            Owned.safeRecycle(projector, this);
            Owned.safeRecycle(up, this);
        }

        void updateRequest(long downstreamPending) {
            int chunk = down.requestChunk;
            if ((int)CONN_PENDING.getAcquire(this) <= chunk>>1) {
                int req = (int)Math.min(downstreamPending, chunk);
                if (maxRelease(CONN_PENDING, this, req))
                    up.request(req);
            }
        }

        void rebind(BatchBinding binding) {
            CONN_PENDING.setRelease(this, 0);
            state = 0;
            error = null;
            up.rebind(binding);
        }

        @Override public String label(StreamNodeDOT.Label type) {
            int i = 0;
            while (i < down.nConnectors && down.connectors[i] != this) ++i;
            var sb = new StringBuilder("Gathering@")
                    .append(Integer.toHexString(identityHashCode(down)))
                    .append("[").append(i).append("]@")
                    .append(Integer.toHexString(identityHashCode(this)));
            if (type.showState()) {
                int st = state == 0 ? ACTIVE : state;
                sb.append("\nstate=").append(Flags.DEFAULT.render(st));
                sb.append(", connPending=");
                StreamNodeDOT.appendRequested(sb, (int)CONN_PENDING.getOpaque(this));
            }
            return sb.toString();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.of(up);
        }

        @Override public @Nullable Orphan<B> pollFillingBatch() {
            var tail = down.pollFillingBatch();
            if (tail == null)
                return null;
            CONN_PENDING.getAndAddRelease(this, Batch.peekTotalRows(tail));
            return tail ;
        }

        @Override public void onBatch(Orphan<B> orphan) {
            if (orphan == null)
                return;
            CONN_PENDING.getAndAddRelease(this, -Batch.peekTotalRows(orphan));
            if (projector != null)
                orphan = projector.projectInPlace(orphan);
            down.quickAppend(orphan);
        }

        @Override public void onBatchByCopy(B batch) {
            if (batch == null)
                return;
            CONN_PENDING.getAndAddRelease(this, -batch.totalRows());
            var concat = down.takeFillingOrCreate();
            if (projector == null) {
                B tmp = concat.takeOwnership(this);
                tmp.copy(batch);
                concat = tmp.releaseOwnership(this);
            } else {
                concat = projector.project(concat, batch);
            }
            down.quickAppend(concat);
        }

        @Override public void onComplete() {
            state = COMPLETED;
            down.onConnectorTerminated();
        }

        @Override public void onCancelled() {
            state = CANCELLED;
            down.onConnectorTerminated();
        }

        @Override public void onError(Throwable cause) {
            state = FAILED;
            error = cause;
            down.onConnectorTerminated();
        }
    }
}
