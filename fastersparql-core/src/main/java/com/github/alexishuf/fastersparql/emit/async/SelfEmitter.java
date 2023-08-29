package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public abstract class SelfEmitter<B extends Batch<B>> extends TaskEmitter<B> {
    protected static final int MUST_AWAKE =  0x80000000;
    protected static final Flags SELF_EMITTER_FLAGS = TASK_FLAGS.toBuilder()
            .flag(MUST_AWAKE, "MUST_AWAKE")
            .build();

    private @MonotonicNonNull Receiver<B> receiver0;
    private @MonotonicNonNull Receiver<B> receiver1;
    private @Nullable B scattered;
    private @Nullable Extra<B> extra;

    private static class Extra<B extends Batch<B>> {
        private int nReceivers;
        @SuppressWarnings("unchecked") private Receiver<B>[] receivers = new Receiver[10];

        private void add(Receiver<B> receiver) {
            Receiver<B>[] arr = receivers;
            for (int i = 0, n = nReceivers; i < n; i++) {
                if (arr[i] == receiver)
                    return;
            }
            if (nReceivers >= arr.length)
                receivers = arr = Arrays.copyOf(arr, nReceivers+(nReceivers>>1));
            arr[nReceivers++] = receiver;
        }
    }

    public SelfEmitter(BatchType<B> batchType, Vars vars,
                       EmitterService runner, int worker,
                       int initState, Flags flags) {
        super(batchType, vars, runner, worker, initState, flags);
        assert flags.contains(SELF_EMITTER_FLAGS);
    }

    @Override protected void doRelease() {
        scattered = Batch.recyclePooled(scattered);
        super.doRelease();
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.empty();
    }

    @Override public boolean canScatter() { return true; }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        int st = lock(statePlain());
        try {
            if ((st & IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (EmitterStats.ENABLED && stats != null)
                ++stats.receivers;
            if      (receiver0 == null || receiver0 == receiver) receiver0 = receiver;
            else if (receiver1 == null || receiver1 == receiver) receiver1 = receiver;
            else
                (extra == null ? extra = new Extra<>() : extra).add(receiver);
        } finally {
            unlock(st);
        }
        if (ThreadJournal.THREAD_JOURNAL)
            ThreadJournal.journal("subscribed", receiver, "to", this);
    }


    @Override protected final void task() {
        int st = state(), termState = st;
        if ((st&IS_PENDING_TERM) != 0) {
            termState = (st&~IS_PENDING_TERM)|IS_TERM;
        } else if ((st&IS_CANCEL_REQ) != 0) {
            termState = CANCELLED;
        } else if ((st&IS_LIVE) != 0) {
            try {
                termState = produceAndDeliver(st);
            } catch (Throwable t) {
                if (this.error == UNSET_ERROR) this.error = t;
                termState = FAILED;
            }
        }

        if ((termState&IS_TERM) != 0)
            deliverTermination(st, termState);
        else if ((termState&MUST_AWAKE) != 0)
            awake();
    }

    protected abstract int produceAndDeliver(int state);

    protected @Nullable B deliver(B b) {
        if (receiver1 != null) {
            B copy = b.copy(Batch.asUnpooled(scattered));
            scattered = null;
            copy = deliver(receiver1, copy);
            if (extra != null) {
                int rows = b.rows, cols = b.cols;
                Receiver<B>[] receivers = extra.receivers;
                for (int i = 0, n = extra.nReceivers; i < n; i++) {
                    if (copy == null || copy.rows != rows || copy.cols != cols)
                        copy = b.copy(copy);
                    copy = deliver(receivers[i], copy);
                }
            }
            scattered = Batch.asPooled(copy);
        }
        return deliver(receiver0, b);
    }

    protected void deliverTermination(int current, int termState) {
        if (moveStateRelease(current, termState)) {
            deliverTermination(receiver0, termState);
            if (receiver1 != null)
                deliverTermination(receiver1, termState);
            if (extra != null) {
                Receiver<B>[] others = extra.receivers;
                for (int i = 0, n = extra.nReceivers; i < n; i++)
                    deliverTermination(others[i], termState);
            }
            markDelivered(current, termState);
        }
    }
}
