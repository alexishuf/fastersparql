package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;


public class ProcessorBIt<B extends Batch<B>> extends DelegatedControlBIt<B, B> {
    protected final BatchProcessor<B, ?> processor;
    private @Nullable BIt<B> terminal;

    public ProcessorBIt(BIt<B> delegate, Orphan<? extends BatchProcessor<B, ?>> processor,
                        @Nullable MetricsFeeder metrics) {
        super(delegate, delegate.batchType(), Emitter.peekVars(processor));
        this.processor = processor.takeOwnership(this);
        this.metrics = metrics;
    }

    public static <B extends Batch<B>> BIt<B> project(Vars outVars, BIt<B> in) {
        var p = in.batchType().projector(outVars, in.vars());
        if (p == null)
            return in;
        return new ProcessorBIt<>(in, p, null);
    }

    protected StringBuilder selfLabel(StreamNodeDOT.Label type) {
        var sb = new StringBuilder(32);
        if (processor == null)
            return sb.append(getClass().getSimpleName());
        var pt = type == StreamNodeDOT.Label.MINIMAL ? type : StreamNodeDOT.Label.SIMPLE;
        return sb.append(processor.label(pt));
    }

    @Override protected void cleanup(@Nullable Throwable error) {
        Owned.safeRecycle(processor, this);
        try {
            delegate.tryCancel();
        } finally {
            if (terminal != null)
                terminal.tryCancel();
        }
    }

    @Override public @Nullable Orphan<B> nextBatch(Orphan<B> orphan) {
        if (terminal != null)
            return nextBatchTerminal(orphan);
        try (var g = new Guard.BatchGuard<>(orphan, this)) {
            BIt<B> delegate = this.delegate;
            while (g.nextBatch(delegate) != null) {
                lock();
                try {
                    if (isTerminated()) {
                        break;
                    } else if (g.set(processor.processInPlace(g.poll())) == null) {
                        delegate.tryCancel(); // premature end, likely due to LIMIT clause
                        break;
                    } else if (g.rows() > 0) {
                        break;
                    }
                } finally { unlock(); }
            }
            orphan = g.poll();
            if (orphan == null && (orphan=makeTerminalAndGetNextBatch()) == null)
                onTermination(null); //exhausted
            else
                onNextBatch(orphan);
            return orphan;
        } catch (Throwable t) {
            onTermination(t); //error
            throw t;
        }
    }

    private @Nullable Orphan<B> nextBatchTerminal(Orphan<B> orphan) {
        try {
            orphan = Objects.requireNonNull(terminal).nextBatch(orphan);
            if (orphan == null)
                onTermination(null);
            else
                onNextBatch(orphan);
            return orphan;
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
    }

    private @Nullable Orphan<B> makeTerminalAndGetNextBatch() {
        lock();
        try {
            if (!isTerminated()) {
                BIt<B> terminal = processor.terminalResults();
                if (terminal != null) {
                    this.terminal = terminal;
                    return terminal.nextBatch(null);
                }
            }
        } finally {unlock();}
        return null;
    }
}
