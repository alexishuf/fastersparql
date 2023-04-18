package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
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

import static java.lang.Integer.MAX_VALUE;
import static java.lang.invoke.MethodHandles.lookup;

public class WsClientParserBIt<B extends Batch<B>> extends AbstractWsParserBIt<B> {
    private static final Logger log = LoggerFactory.getLogger(WsClientParserBIt.class);
    private static final VarHandle B_REQUESTED;
    static {
        try {
            B_REQUESTED = lookup().findVarHandle(WsClientParserBIt.class, "plainBindingsRequested", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /* --- --- --- instance fields --- --- --- */

    private final @Nullable BIt<B> bindings;
    private final Vars usefulBindingsVars;
    private final @Nullable JoinMetrics metrics;
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
    public WsClientParserBIt(WsFrameSender<?> frameSender, CallbackBIt<B> destination) {
        super(frameSender, destination);
        usefulBindingsVars = Vars.EMPTY;
        bindings = null;
        metrics = null;
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
    public WsClientParserBIt(WsFrameSender<?> frameSender, BatchType<B> batchType, Vars vars, int maxBatches) {
        super(frameSender, batchType, vars, maxBatches);
        usefulBindingsVars = Vars.EMPTY;
        bindings = null;
        metrics = null;
    }

    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request.
     *
     * @param frameSender       object to be used when sending WebSocket frames
     * @param batchType         the {@link BatchType} with operations for {@code R}
     * @param vars              See {@link ResultsParserBIt#ResultsParserBIt(BatchType, Vars, int)}
     * @param bindings          a {@link BIt} of bindings that will be sent in to the server as TSV
     *                          chunks in WebSocket frames in parallel to the consumption of this
     *                          {@link BIt} and in response to server-sent {@code !bind-request n}
     *                          frames.
     * @param usefulBindingVars among the vars provided by {@code bindings} only these will be
     *                          sent to the server. This is equivalent to projecting
     *                          {@code bindings} just during the sending phase: for
     *                          {@link BindType#JOIN} and {@link BindType#LEFT_JOIN},
     *                          dropped vars will still be visible in the output rows.
     @param maxItems          See {@link SPSCBIt#SPSCBIt(BatchType, Vars, int)}
     */
    public WsClientParserBIt(@NonNull WsFrameSender<?> frameSender,
                             @NonNull BatchType<B> batchType, @NonNull Vars vars,
                             @NonNull BIt<B> bindings,
                             @Nullable Vars usefulBindingVars,
                             @Nullable JoinMetrics metrics, int maxItems) {
        super(frameSender, batchType, vars, maxItems);
        this.bindings = bindings;
        this.usefulBindingsVars = usefulBindingVars == null ? bindings.vars() : usefulBindingVars;
        assert bindings.vars().containsAll(this.usefulBindingsVars);
        this.metrics = metrics;
    }

    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request
     * that will redirect all rows and the results completion to {@code destination}
     *
     * @param frameSender object to be used when sending WebSocket frames
     * @param destination See {@link ResultsParserBIt#ResultsParserBIt(BatchType, CallbackBIt)}
     * @param bindings a {@link BIt} of bindings that will be sent in to the server as TSV
     *                 chunks in WebSocket frames in parallel to the consumption of this
     *                 {@link BIt} and in response to server-sent {@code !bind-request n}
     *                 frames.
     * @param usefulBindingVars among the vars provided by {@code bindings} only these will be
     *                          sent to the server. This is equivalent to projecting
     *                          {@code bindings} just during the sending phase: for
     *                          {@link BindType#JOIN} and {@link BindType#LEFT_JOIN},
     *                          dropped vars will still be visible in the output rows.
     */
    public WsClientParserBIt(@NonNull WsFrameSender<?> frameSender,
                             @NonNull CallbackBIt<B> destination,
                             @NonNull BIt<B> bindings,
                             @Nullable Vars usefulBindingVars,
                             @Nullable JoinMetrics metrics) {
        super(frameSender, destination);
        this.bindings = bindings;
        this.usefulBindingsVars = usefulBindingVars == null ? bindings.vars() : usefulBindingVars;
        assert bindings.vars().containsAll(this.usefulBindingsVars);
        this.metrics = metrics;
    }

    /* --- --- --- specialize SVParserBIt.Tsv methods --- --- --- */

    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        if      (rope.has(begin,   BIND_REQUEST)) handleBindRequest(rope, begin, eol);
        else                                      return false;
        return true;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (!serverSentTermination && !(cause instanceof FSServerException)) {
            try { //noinspection unchecked
                frameSender.sendFrame(frameSender.createSink().append(CANCEL_LF));
            } catch (Throwable t) {
                log.info("Failed to send !cancel", t);
            }
        }
        if (bindings != null) {
            bindings.close(); // MAY kill sender
            // unblock sender and publish state changes from this thread, making it exit
            B_REQUESTED.getAndAddRelease(this, 1);
            LockSupport.unpark(bindingsSender);
        }
        if (metrics != null) metrics.completeAndDeliver(cause, this);
    }

    @Override protected void emitRow() {
        if (metrics != null) metrics.rightRowsReceived(1);
        super.emitRow();
    }

    /* --- --- --- parsing for !control messages  --- --- --- */

    private void handleBindRequest(Rope rope, int begin, int end) {
        if (bindings == null) throw noBindings(BIND_REQUEST);
        if (bindingsSender == null)
            bindingsSender = Thread.startVirtualThread(this::sendBindingsThread);
        long n = rope.parseLong(rope.skipWS(begin + BIND_REQUEST.length, end));
        if ((int)B_REQUESTED.getAndAddRelease(this, (int)Math.max(MAX_VALUE, n)) <= 0)
            LockSupport.unpark(bindingsSender);
    }

    /* --- --- --- exception factories --- --- --- */

    private InvalidSparqlResultsException noBindings(Object what) {
        var whatStr = what instanceof byte[] b ? new ByteRope(b) : what.toString();
        return new InvalidSparqlResultsException("Cannot handle "+whatStr+": bindings not set");
    }

    /* --- --- --- bindings upload virtual thread --- --- --- */

    private void dummyBeginBinding(int n) {
        if (metrics == null) return;
        for (int i = 0; i < n; i++)
            metrics.beginBinding();
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) private void sendBindingsThread() {
        Thread.currentThread().setName("sendBindingsThread-"+id());
        var serializer = new WsSerializer();
        ByteSink buffer = null;
        try {
            if (bindings == null)
                throw noBindings("sendBindingsThread()");

            bindings.preferred().tempEager();
            int allowed = 0; // bindings requested by the server

            // send a frame with var names
            buffer = frameSender.createSink();
            serializer.init(bindings.vars(), usefulBindingsVars, false, buffer);
            frameSender.sendFrame(buffer);
            for (B b = null; (b = bindings.nextBatch(b)) != null; ) {
                for (int r = 0, rows = b.rows, taken; r < rows; r += taken) {
                    while ((allowed += (int) B_REQUESTED.getAndSetAcquire(this, 0)) == 0)
                        LockSupport.park(this);
                    if (isClosed())
                        return;
                    allowed -= taken = Math.min(allowed, rows - r);
                    buffer = frameSender.createSink();
                    serializer.serialize(b, r, taken, buffer);
                    frameSender.sendFrame(buffer);
                    if (metrics != null)
                        dummyBeginBinding(taken);
                }
            }
        } catch (Throwable t) {
            if (!(t instanceof BItReadClosedException)) {
                log.warn("sendBindingsThread() dying ", t);
                close();
            }
        } finally {
            if (!isClosed()) {
                serializer.serializeTrailer(buffer = frameSender.createSink());
                //noinspection unchecked
                frameSender.sendFrame(buffer);
            }
            if (buffer != null) frameSender.releaseSink(buffer);
        }
    }
}
