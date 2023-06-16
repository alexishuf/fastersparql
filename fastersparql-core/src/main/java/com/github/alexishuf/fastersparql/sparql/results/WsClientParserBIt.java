package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.invoke.MethodHandles.lookup;

public class WsClientParserBIt<B extends Batch<B>> extends AbstractWsParserBIt<B> {
    private static final Logger log = LoggerFactory.getLogger(WsClientParserBIt.class);
    private static final int[] EMPTY_COLS = new int[0];
    private static final VarHandle B_REQUESTED;
    static {
        try {
            B_REQUESTED = lookup().findVarHandle(WsClientParserBIt.class, "plainBindingsRequested", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /* --- --- --- instance fields --- --- --- */

    private final @Nullable BindQuery<B> bindQuery;
    private final @Nullable BIt<B> bindings;
    private final Vars usefulBindingsVars;
    private final @Nullable SPSCBIt<B> sentBindings;
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
     * @param destination See {@link ResultsParserBIt#ResultsParserBIt(BatchType, CallbackBIt)}
     */
    public WsClientParserBIt(WsFrameSender<?,?> frameSender, CallbackBIt<B> destination) {
        super(frameSender, destination);
        usefulBindingsVars = Vars.EMPTY;
        sentBindings = null;
        bindingCol2OutCol = EMPTY_COLS;
        bindings = null;
        bindQuery = null;
    }

    /**
     * Create a parser for the result of a {@code !query} request in the experimental WebSockets
     * SPARQL protocol. The parser will die mid-parsing if used on the results of a {@code !bind}
     * request.
     *
     * @param frameSender object to be used when sending WebSocket frames
     * @param batchType   set of operations on {@code R} instances
     * @param vars        See {@link ResultsParserBIt#ResultsParserBIt(BatchType, Vars, int)}
     * @param maxBatches  See {@link SPSCBIt#SPSCBIt(BatchType, Vars, int)}
     */
    public WsClientParserBIt(WsFrameSender<?, ?> frameSender, BatchType<B> batchType, Vars vars, int maxBatches) {
        super(frameSender, batchType, vars, maxBatches);
        usefulBindingsVars = Vars.EMPTY;
        sentBindings = null;
        bindingCol2OutCol = EMPTY_COLS;
        bindings = null;
        bindQuery = null;
    }

    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request.
     *
     * @param frameSender       object to be used when sending WebSocket frames
     * @param vars              See {@link ResultsParserBIt#ResultsParserBIt(BatchType, Vars, int)}
     * @param bindQuery         a {@link BindQuery} specifying the bind operation
     *                          {@link BindQuery#emptyBinding(long)} and
     *                          {@link BindQuery#nonEmptyBinding(long)} will be called for every
     *                          binding, in order.
     * @param usefulBindingVars among the vars provided by {@code bindings} only these will be
     *                          sent to the server. This is equivalent to projecting
     *                          {@code bindings} just during the sending phase: for
     *                          {@link BindType#JOIN} and {@link BindType#LEFT_JOIN},
     *                          dropped vars will still be visible in the output rows.
     @param maxItems          See {@link SPSCBIt#SPSCBIt(BatchType, Vars, int)}
     */
    public WsClientParserBIt(@NonNull WsFrameSender<?, ?> frameSender,
                             @NonNull Vars vars,
                             BindQuery<B> bindQuery,
                             @Nullable Vars usefulBindingVars, int maxItems) {
        super(frameSender, bindQuery.bindings.batchType(), vars, maxItems);
        this.bindQuery = bindQuery;
        this.bindings = bindQuery.bindings;
        Vars bindingsVars = bindings.vars();
        this.sentBindings = new SPSCBIt<>(batchType, bindingsVars, BIt.DEF_MAX_BATCH);
        this.usefulBindingsVars = usefulBindingVars == null ? bindingsVars : usefulBindingVars;
        assert bindingsVars.containsAll(this.usefulBindingsVars);
        this.bindingCol2OutCol = bindingCol2OutCol(vars, bindingsVars);
        this.metrics = bindQuery.metrics;
    }

    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request
     * that will redirect all rows and the results completion to {@code destination}
     *
     * @param frameSender       object to be used when sending WebSocket frames
     * @param destination       See {@link ResultsParserBIt#ResultsParserBIt(BatchType, CallbackBIt)}
     * @param bindQuery         a {@link BindQuery} specifying the bind operation
     *                          {@link BindQuery#emptyBinding(long)} and
     *                          {@link BindQuery#nonEmptyBinding(long)} will be called for every
     *                          binding, in order.
     * @param usefulBindingVars among the vars provided by {@code bindings} only these will be
     *                          sent to the server. This is equivalent to projecting
     *                          {@code bindings} just during the sending phase: for
     *                          {@link BindType#JOIN} and {@link BindType#LEFT_JOIN},
     *                          dropped vars will still be visible in the output rows.
     */
    public WsClientParserBIt(@NonNull WsFrameSender<?, ?> frameSender,
                             @NonNull CallbackBIt<B> destination,
                             BindQuery<B> bindQuery,
                             @Nullable Vars usefulBindingVars) {
        super(frameSender, destination);
        this.bindQuery = bindQuery;
        this.bindings = bindQuery.bindings;
        Vars bindingsVars = bindings.vars();
        this.sentBindings = new SPSCBIt<>(bindings.batchType(), bindingsVars, BIt.DEF_MAX_BATCH);
        this.usefulBindingsVars = usefulBindingVars == null ? bindingsVars : usefulBindingVars;
        assert bindingsVars.containsAll(this.usefulBindingsVars);
        this.bindingCol2OutCol = bindingCol2OutCol(vars, bindingsVars);
        this.metrics = bindQuery.metrics;
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
        if (sentBindings == null || column != 0) { // normal path (not WsBindingSeq.VAR)
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
        if (!bindingNotified && bindQuery != null) {
            bindQuery.nonEmptyBinding(seq);
            bindingNotified = true;
        }
        if (!rowStarted)
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
        if (bindings != null) {
            bindings.close(); // MAY kill sender
            // unblock sender and publish state changes from this thread, making it exit
            B_REQUESTED.getAndAddRelease(this, 1);
            LockSupport.unpark(bindingsSender);
            if (serverSentTermination && cause == null && sentBindings != null && bindQuery != null) {
                // got a friendly !end, iterate over all remaining sent bindings and notify
                // they had zero results
                while (true) {
                    if (sentBatch == null || ++sentBatchRow >= sentBatch.rows) {
                        sentBatchRow = 0;
                        if ((sentBatch = sentBindings.nextBatch(sentBatch)) == null)
                            break; // no more sent bindings
                    }
                    handleBindEmptyUntil(bindingSeq+(sentBatch.rows-sentBatchRow));
                }
            }
        }
        if (metrics != null) metrics.completeAndDeliver(cause, isClosedFor(cause, this));
    }

    /* --- --- --- parsing for !control messages  --- --- --- */

    private void handleBindRequest(long n) {
        if (bindings == null) throw noBindings(BIND_REQUEST);
        if (bindingsSender == null)
            bindingsSender = Thread.startVirtualThread(this::sendBindingsThread);
        int add = (int) Math.min(MAX_VALUE-(long)plainBindingsRequested, n);
        if ((int)B_REQUESTED.getAndAddRelease(this, add) <= 0)
            LockSupport.unpark(bindingsSender);
    }

    private void handleBindEmptyUntil(long seq) {
        skipUntilBindingSeq(seq);
        if (!bindingNotified && bindQuery != null) {
            bindQuery.emptyBinding(seq);
            bindingNotified = true;
        }
    }

    private void skipUntilBindingSeq(long seq) {
        if (sentBindings == null || bindQuery == null)
            throw new IllegalStateException("Not sending bindings");
        while (bindingSeq < seq) {
            if (metrics instanceof JoinMetrics m) m.beginBinding();
            if (sentBatch == null || ++sentBatchRow >= sentBatch.rows) {
                sentBatch = sentBindings.nextBatch(sentBatch);
                sentBatchRow = 0;
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

    /* --- --- --- bindings upload virtual thread --- --- --- */

    @SuppressWarnings({"unchecked", "rawtypes"}) private void sendBindingsThread() {
        Thread.currentThread().setName("sendBindingsThread-"+id());
        var serializer = new WsSerializer();
        ByteSink sink = frameSender.createSink();
        try {
            if (bindings == null)
                throw noBindings("sendBindingsThread()");
            assert sentBindings != null;

            bindings.preferred().tempEager();
            int allowed = 0; // bindings requested by the server

            // send a frame with var names
            serializer.init(bindings.vars(), usefulBindingsVars, false, sink.touch());
            frameSender.sendFrame(sink.take());
            for (B b = null; (b = bindings.nextBatch(b)) != null; ) {
                sentBindings.copy(b);
                for (int r = 0, rows = b.rows, taken; r < rows; r += taken) {
                    while ((allowed += (int) B_REQUESTED.getAndSetAcquire(this, 0)) == 0)
                        LockSupport.park(this);
                    if (allowed < 0) allowed = MAX_VALUE;
                    if (isClosed())
                        return;
                    allowed -= taken = Math.min(allowed, rows - r);
                    serializer.serialize(b, r, taken, sink.touch());
                    frameSender.sendFrame(sink.take());
                }
            }
        } catch (Throwable t) {
            if (!(t instanceof BItReadClosedException)) {
                log.warn("sendBindingsThread() dying ", t);
                close();
            }
        } finally {
            if (sentBindings != null)
                sentBindings.complete(null);
            if (!serverSentTermination) {
                serializer.serializeTrailer(sink.touch());
                frameSender.sendFrame(sink.take());
            }
            sink.release();
        }
    }
}
