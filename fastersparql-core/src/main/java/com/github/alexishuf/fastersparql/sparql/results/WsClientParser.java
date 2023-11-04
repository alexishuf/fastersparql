package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.BIt.DEF_MAX_BATCH;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.invoke.MethodHandles.lookup;

public class WsClientParser<B extends Batch<B>> extends AbstractWsParser<B> {
    private static final Logger log = LoggerFactory.getLogger(WsClientParser.class);
    private static final int[] EMPTY_COLS = new int[0];
    private static final SPSCBIt<?> DUMMY_SENT_BINDINGS;
    private static final VarHandle B_REQUESTED;
    static {
        try {
            B_REQUESTED = lookup().findVarHandle(WsClientParser.class, "plainBindingsRequested", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
        DUMMY_SENT_BINDINGS = new SPSCBIt<>(TermBatchType.TERM, Vars.EMPTY, DEF_MAX_BATCH);
        DUMMY_SENT_BINDINGS.complete(new Exception("DUMMY_SENT_BINDINGS should never be used"));
    }

    /* --- --- --- instance fields --- --- --- */

    private final @Nullable BindQuery<B> bindQuery;
    private final @Nullable BindingsReceiver bindingsReceiver;
    private final @Nullable JoinMetrics metrics;
    private final Vars usefulBindingsVars;
    private final SPSCBIt<B> sentBindings;
    private final int[] bindingCol2OutCol;
    private @Nullable B sentBatch;
    private int sentBatchRow;
    private long bindingSeq = -1;
    private boolean bindingNotified = true;
    private @MonotonicNonNull Thread bindingsSender;
    @SuppressWarnings("unused") private int plainBindingsRequested;

    /* --- --- --- constructors --- --- --- */

    /**
     * Create a parser for the result of a {@code !query} request in the experimental WebSockets
     * SPARQL protocol. The parser will die mid-parsing if used on the results of a {@code !bind}
     * request.
     *
     * @param frameSender object to be used when sending WebSocket frames
     * @param destination See {@link ResultsParser#ResultsParser(CompletableBatchQueue)}
     */
    public WsClientParser(WsFrameSender<?,?> frameSender, CompletableBatchQueue<B> destination) {
        super(frameSender, destination);
        usefulBindingsVars = Vars.EMPTY;
        //noinspection unchecked
        sentBindings = (SPSCBIt<B>) DUMMY_SENT_BINDINGS;
        bindingCol2OutCol = EMPTY_COLS;
        bindingsReceiver = null;
        bindQuery = null;
        metrics = null;
    }


    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request
     * that will redirect all rows and the results completion to {@code destination}
     *
     * @param frameSender       object to be used when sending WebSocket frames
     * @param destination       See {@link ResultsParser#ResultsParser(CompletableBatchQueue)}
     * @param bindQuery         a {@link ItBindQuery} specifying the bind operation
     *                          {@link ItBindQuery#emptyBinding(long)} and
     *                          {@link ItBindQuery#nonEmptyBinding(long)} will be called for every
     *                          binding, in order.
     * @param usefulBindingVars among the vars provided by {@code bindings} only these will be
     *                          sent to the server. This is equivalent to projecting
     *                          {@code bindings} just during the sending phase: for
     *                          {@link BindType#JOIN} and {@link BindType#LEFT_JOIN},
     *                          dropped vars will still be visible in the output rows.
     */
    public WsClientParser(@NonNull WsFrameSender<?, ?> frameSender,
                          @NonNull CompletableBatchQueue<B> destination,
                          BindQuery<B> bindQuery,
                          @Nullable Vars usefulBindingVars) {
        super(frameSender, destination);
        this.bindQuery = bindQuery;
        Vars bindingsVars = bindQuery.bindingsVars();
        this.sentBindings = new SPSCBIt<>(bindQuery.batchType(), bindingsVars, DEF_MAX_BATCH);
        this.usefulBindingsVars = usefulBindingVars == null ? bindingsVars : usefulBindingVars;
        assert bindingsVars.containsAll(this.usefulBindingsVars);
        this.bindingCol2OutCol = bindingCol2OutCol(vars(), bindingsVars);
        if (bindQuery instanceof EmitBindQuery<B> ebq)
            this.bindingsReceiver = new BindingsReceiver(ebq.bindings);
        else
            this.bindingsReceiver = null;
        if (destination instanceof BIt<?> b)
            b.metrics(bindQuery.metrics);
        metrics = bindQuery.metrics;
    }

    private static int[] bindingCol2OutCol(Vars outVars, Vars bindingsVars) {
        int[] cols = new int[bindingsVars.size()];
        for (int i = 0; i < cols.length; i++)
            cols[i] = outVars.indexOf(bindingsVars.get(i));
        return cols;
    }

    /* --- --- --- specialize SVParserBIt.Tsv methods --- --- --- */

    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        byte hint = begin + 6 /*!bind-*/ < eol ? rope.get(begin+6) : 0;
        byte[] cmd = hint == 'r' && rope.has(begin, BIND_REQUEST) ? BIND_REQUEST
                   : hint == 'e' && rope.has(begin, BIND_EMPTY_UNTIL) ? BIND_EMPTY_UNTIL
                   : null;
        if (cmd == null)
            return false;
        long n = rope.parseLong(begin + cmd.length);
        if   (cmd == BIND_REQUEST) handleBindRequest(n);
        else                       handleBindEmptyUntil(n);
        return true;
    }

    @Override protected void setTerm() {
        if (bindQuery == null || column != 0) { // normal path (not WsBindingSeq.VAR)
            super.setTerm();
            return;
        }
        // the WS server will perform the binding operation and for each bound result it will
        // send the sequence number (0-based) of the binding and the values for public vars of
        // the  right-hand operand that were not bound with values from the left-hand operand.
        // In the case of non-join BindTypes (EXISTS/NOT_EXISTS/MINUS), there will be no
        // right-side columns and only the binding sequence number column will be sent.
        //
        // This code parses the sequence number and instead of trying to set it in rowBatch,
        // will set all appropriate columns in rowBatch with values from the seq-th binding.
        //
        // The server will process bindings in the same order they are sent. Thus, we will not
        // receive a seq value below bindingSeq. The server MAY skip some sequence numbers to denote
        // that the skipped bindings yielded no results. In that case we will not observe such
        // sequence numbers here and will not output those bindings.
        long seq = WsBindingSeq.parse(termParser.localBuf(), termParser.localBegin(),
                                      termParser.localEnd);
        if (seq < bindingSeq)
            throw new InvalidSparqlResultsException("Server sent binding seq in the past");
        skipUntilBindingSeq(seq);
        if (sentBatch == null || sentBatchRow >= sentBatch.rows)
            throw new IllegalArgumentException("No sent binding, server sent binding seq in the future");
        if (!bindingNotified) {
            bindQuery.nonEmptyBinding(seq);
            bindingNotified = true;
        }
        if (!incompleteRow)
            beginRow();
        for (int col = 0; col < bindingCol2OutCol.length; col++) {
            int outCol = bindingCol2OutCol[col];
            if (outCol >= 0)
                batch.putTerm(outCol, sentBatch, sentBatchRow, col);
        }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (!serverSentTermination && !(cause instanceof FSServerException)) {
            try { //noinspection unchecked
                frameSender.sendFrame(frameSender.createSink().touch().append(CANCEL_LF).take());
            } catch (Throwable t) {
                log.info("Failed to send !cancel", t);
            }
        }
        boolean hasBindings = false;
        if (bindingsReceiver != null) {
            hasBindings = true;
            bindingsReceiver.upstream.cancel();
        } else if (bindQuery instanceof ItBindQuery<B> bq) {
            hasBindings = true;
            bq.bindings.close();
        }
        if (hasBindings) {
            // unblock sender and publish state changes from this thread, making it exit
            B_REQUESTED.getAndAddRelease(this, 1);
            LockSupport.unpark(bindingsSender);
            if (serverSentTermination && cause == null && bindQuery != null) {
                // got a friendly !end, iterate over all remaining sent bindings and notify
                // they had zero results
                while (true) {
                    if (sentBatch == null || ++sentBatchRow >= sentBatch.rows) {
                        sentBatchRow = 0;
                        boolean got = false;
                        if (sentBatch != null && sentBatch.next != null) {
                            sentBatch = sentBatch.dropHead();
                            got = sentBatch != null;
                        }
                        if (!got && (sentBatch = sentBindings.nextBatch(sentBatch)) == null)
                            break; // no more sent bindings
                    }
                    handleBindEmptyUntil(bindingSeq+(sentBatch.rows-sentBatchRow));
                }
            }
            batchType().recycle(sentBatch);
        }
    }

    /* --- --- --- parsing for !control messages  --- --- --- */

    private void handleBindRequest(long n) {
        if (bindingsReceiver != null) {
            bindingsReceiver.requestBindings(n);
        } else if (bindQuery == null) {
            throw noBindings(BIND_REQUEST);
        } else {
            if (bindingsSender == null)
                bindingsSender = Thread.startVirtualThread(this::sendBindingsThread);
            int add = (int) Math.min(MAX_VALUE-(long)plainBindingsRequested, n);
            if ((int)B_REQUESTED.getAndAddRelease(this, add) <= 0)
                LockSupport.unpark(bindingsSender);
        }
    }

    private void handleBindEmptyUntil(long seq) {
        skipUntilBindingSeq(seq);
        if (!bindingNotified && bindQuery != null) {
            bindQuery.emptyBinding(seq);
            bindingNotified = true;
        }
    }

    private void skipUntilBindingSeq(long seq) {
        if (bindQuery == null)
            throw new IllegalStateException("Not sending bindings");
        while (bindingSeq < seq) {
            if (metrics instanceof JoinMetrics m) m.beginBinding();
            if (sentBatch == null || ++sentBatchRow >= sentBatch.rows) {
                sentBatchRow = 0;
                boolean got = sentBatch != null && sentBatch.next != null
                           && (sentBatch = sentBatch.dropHead()) != null;
                if (!got) sentBatch = sentBindings.nextBatch(sentBatch);
            }
            long prev = bindingSeq++;
            if (!bindingNotified)
                bindQuery.emptyBinding(prev);
            bindingNotified = false;
        }
    }

    /* --- --- --- exception factories --- --- --- */

    private InvalidSparqlResultsException noBindings(Object what) {
        var whatStr = what instanceof byte[] b ? new ByteRope(b) : what.toString();
        return new InvalidSparqlResultsException("Cannot handle "+whatStr+": bindings not set");
    }

    /* --- --- --- bindings upload --- --- --- */

    private void sendBindingsThread() {
        Thread.currentThread().setName("sendBindingsThread");
        ResultsSender<?,?> sender = null;
        boolean sendEnd = true;
        B batch = null;
        try {
            if (bindQuery == null)
                throw noBindings("sendBindingsThread()");
            BIt<B> bindings = ((ItBindQuery<B>) bindQuery).bindings;
            sender = frameSender.createSender();

            bindings.preferred().tempEager();
            int allowed = 0; // bindings requested by the server

            sender.sendInit(bindings.vars(), usefulBindingsVars, false);
            while ((batch = bindings.nextBatch(batch)) != null) {
                sentBindings.copy(batch);
                for (var b = batch; b != null; b = b.next) {
                    for (int r = 0, rows = b.rows, taken; r < rows; r += taken) {
                        while ((allowed += (int) B_REQUESTED.getAndSetAcquire(this, 0)) == 0)
                            LockSupport.park(this);
                        if (allowed < 0) allowed = MAX_VALUE;
                        if (isTerminated())
                            throw CancelledException.INSTANCE;
                        allowed -= taken = Math.min(allowed, rows - r);
                        sender.sendSerialized(b, r, taken);
                    }
                }
            }
        } catch (TerminatedException|CancelledException|BItReadClosedException e) {
            if (!serverSentTermination && sender != null)
                sender.sendCancel();
            sendEnd = false;
        } catch (Throwable t) {
            if (!serverSentTermination && sender != null)
                sender.sendError(t);
            log.warn("sendBindingsThread() dying ", t);
            dst.cancel();
            sendEnd = false;
        } finally {
            sentBindings.complete(null);
            if (sender != null) {
                if (sendEnd && !serverSentTermination)
                    sender.sendTrailer();
                sender.close();
            }
        }
    }

    private final class BindingsReceiver implements Receiver<B> {
        private final Emitter<B> upstream;
        private final SPSCBIt<B> sentBindings;
        private final ResultsSender<?, ?> sender;
        private boolean first = true;

        public BindingsReceiver(Emitter<B> upstream) {
            this.upstream = upstream;
            this.sender = frameSender.createSender();
            this.sender.preTouch();
            this.sentBindings = WsClientParser.this.sentBindings;
            upstream.subscribe(this);
        }

        @Override public Stream<? extends StreamNode> upstream() {
            return Stream.of(upstream);
        }

        public void requestBindings(long n) {
            if (first) {
                first = false;
                sender.sendInit(upstream.vars(), usefulBindingsVars, false);
            }
            upstream.request(n);
        }

        @Override public @Nullable B onBatch(B batch) {
            try {
                sentBindings.copy(batch);
            } catch (TerminatedException | CancelledException e) {
                upstream.cancel();
            }
            sender.sendSerializedAll(batch);
            return batch;
        }

        @Override public void onComplete() {
            sentBindings.complete(null);
            sender.sendTrailer();
        }

        @Override public void onCancelled() {
            sentBindings.cancel();
            if (!serverSentTermination)
                sender.sendCancel();
        }

        @Override public void onError(Throwable cause) {
            sentBindings.cancel();
            if (!serverSentTermination)
                sender.sendError(cause);
            log.warn("bindings emitter {} failed for {}", upstream, this, cause);
            dst.complete(cause);
        }
    }
}
