package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public abstract class AbstractWsClientParser<B extends Batch<B>> extends AbstractWsParser<B>
        implements ResultsSerializer.NodeConsumer<B> {
    protected final @Nullable BindQuery<B> bindQuery;
    private B sentBindings;
    private long currBinding = -1;
    private int currBindingRow = -1;
    private boolean bindingNotified = true;
    private final int[] bindingCol2OutCol;
    private final Metrics.@Nullable JoinMetrics metrics;

    public AbstractWsClientParser(CompletableBatchQueue<B> dst, @Nullable BindQuery<B> bindQuery) {
        super(dst);
        this.bindQuery = bindQuery;
        if (bindQuery == null) {
            bindingCol2OutCol = ArrayPool.EMPTY_INT;
            metrics = null;
        } else {
            Vars bindingsVars = bindQuery.bindingsVars(), outVars = dst.vars();
            bindingCol2OutCol = new int[bindingsVars.size()];
            for (int i = 0; i < bindingCol2OutCol.length; i++)
                bindingCol2OutCol[i] = outVars.indexOf(bindingsVars.get(i));
            if (dst instanceof BIt<?> b)
                b.metrics(bindQuery.metrics);
            metrics = bindQuery.metrics;
        }
    }

    @Override public void reset(CompletableBatchQueue<B> downstream) {
        dropSentBindings();
        super.reset(downstream);
    }

    private void dropSentBindings() {
        if (sentBindings != null)
            sentBindings = batchType().recycle(sentBindings);
        currBindingRow = -1;
        currBinding = -1;
        bindingNotified = true;
    }

    public void addSentBatch(B b) { sentBindings = Batch.quickAppend(sentBindings, b); }

    @Override public void onSerializedNode(B node) { addSentBatch(node); }

    @Override public void onNotSerializedNode(B node, int serializedUntilRow) {
        if (serializedUntilRow > 0) {
            node.rows = (short)Math.min(node.rows, serializedUntilRow);
            addSentBatch(node);
        } else {
            node.recycle();
        }
    }

    @Override protected void onCancel() {
        throw new FSServerException("server sent spontaneous !cancel");
    }

    @Override protected void onInfo(SegmentRope rope, int begin, int end) {
        journal(rope.toString(begin, end), "dst=", dst);
    }

    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        byte hint = begin + 6 /*!bind-*/ < eol ? rope.get(begin+6) : 0;
        byte[] cmd = hint == 'r' && rope.has(begin, BIND_REQUEST) ? BIND_REQUEST
                : hint == 'e' && rope.has(begin, BIND_EMPTY_UNTIL) ? BIND_EMPTY_UNTIL
                : null;
        if (cmd == null)
            return false;
        long n = cmd == BIND_REQUEST && rope.hasAnyCase(rope.skipWS(begin+cmd.length, eol), MAX)
                ? Long.MAX_VALUE : rope.parseLong(begin+cmd.length);
        if   (cmd == BIND_REQUEST) handleBindRequest(n);
        else                       handleBindEmptyUntil(n);
        return true;
    }

    protected abstract void handleBindRequest(long n);

    protected void handleBindEmptyUntil(long seq) {
        skipUntilBindingSeq(seq);
        if (!bindingNotified && bindQuery != null) {
            bindQuery.emptyBinding(seq);
            bindingNotified = true;
        }
    }
    @Override protected void beforeComplete(@Nullable Throwable error) {
        if (serverSentTermination && error == null && sentBindings != null) {
            // got a friendly !end, iterate over all remaining sent bindings and notify
            // they had zero results
            long until = currBinding == -1 ? sentBindings.totalRows()-1
                    : currBinding + sentBindings.totalRows()-1-currBindingRow;
            handleBindEmptyUntil(until);
        }
        super.beforeComplete(error);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        dropSentBindings();
    }

    @Override protected void setTerm() {
        if (bindQuery == null || column != 0) { // normal path, not WsBindingSeq.VAR
            super.setTerm();
            return;
        }
        long seq = WsBindingSeq.parse(termParser.localBuf(), termParser.localBegin(),
                termParser.localEnd);
        if (seq < currBinding)
            throw new InvalidSparqlResultsException("server sent binding seq in the past");
        skipUntilBindingSeq(seq);
        if (sentBindings == null || currBindingRow >= sentBindings.rows)
            throw new InvalidSparqlResultsException("server sent binding seq that was not yet sent");
        if (!bindingNotified) {
            bindQuery.nonEmptyBinding(seq);
            bindingNotified = true;
        }
        if (!incompleteRow)
            beginRow();
        for (int col = 0; col < bindingCol2OutCol.length; col++) {
            int outCol = bindingCol2OutCol[col];
            if (outCol >= 0)
                batch.putTerm(outCol, sentBindings, currBindingRow, col);
        }
    }

    private void skipUntilBindingSeq(long seq) {
        if (sentBindings == null) {
            if (currBinding >= seq) return;
            else throw new IllegalStateException("seq >= last sent binding");
        }
        while (currBinding < seq) {
            if (metrics != null) metrics.beginBinding();
            while (sentBindings != null && ++currBindingRow >= sentBindings.rows) {
                sentBindings = sentBindings.dropHead();
                currBindingRow = -1;
            }
            long prev = currBinding++;
            if (!bindingNotified && bindQuery != null)
                bindQuery.emptyBinding(prev);
            bindingNotified = false;
        }
    }
}
