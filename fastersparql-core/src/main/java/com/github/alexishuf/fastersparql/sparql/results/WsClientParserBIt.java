package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.operators.SPSCUnitBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public class WsClientParserBIt<B extends Batch<B>> extends AbstractWsParserBIt<B> {
    /* --- --- --- dummies used to avoid null checks --- --- --- */
    private static final int[] DUMMY_COLS = new int[0];
    private static final SPSCUnitBIt<?> NO_BINDINGS_SENT = new SPSCUnitBIt<>(Batch.TERM, Vars.EMPTY);

    static final ByteRope END_FRAME = new ByteRope("!end\n");

    /* --- --- --- instance fields --- --- --- */

    private final @Nullable BIt<B> bindings;
    private final Vars usefulBindingsVars;
    /** Contains at index {@code i} the column of {@code builder} that should be set with
     *  the value at column {@code i} of {@code activeBinding}, or -1 if that binding var is
     *  not present in {@code this.vars}. */
    private final int[] bindingCol2outputCol;
    private final BindType bindType;
    private final SPSCUnitBIt<B> bindingsSent;
    private @Nullable B bindingsSentBatch = null;
    private int bindingsSentBatchRow = 0;
    private final @Nullable JoinMetrics metrics;
    private long bindingsReceived = 0;
    private boolean activeBindingEmpty = true;
    private boolean hasActiveBinding = false;

    /* --- --- --- constructors --- --- --- */

    /**
     * Create a parser for the result of a {@code !query} request in the experimental WebSockets
     * SPARQL protocol. The parser will die mid-parsing if used on the results of a {@code !bind}
     * request.
     *
     * @param frameSender object to be used when sending WebSocket frames
     * @param batchType set of operations on {@code R} instances
     * @param destination See {@link ResultsParserBIt#ResultsParserBIt(BatchType, com.github.alexishuf.fastersparql.batch.CallbackBIt)}
     */
    public WsClientParserBIt(WsFrameSender frameSender, BatchType<B> batchType, CallbackBIt<B> destination) {
        super(frameSender, batchType, destination);
        bindings = null;
        bindingCol2outputCol = DUMMY_COLS;
        usefulBindingsVars = Vars.EMPTY;
        bindType = BindType.JOIN;
        //noinspection unchecked
        bindingsSent = (SPSCUnitBIt<B>) NO_BINDINGS_SENT;
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
    public WsClientParserBIt(WsFrameSender frameSender, BatchType<B> batchType, Vars vars, int maxBatches) {
        super(frameSender, batchType, vars, maxBatches);
        bindings = null;
        bindingCol2outputCol = DUMMY_COLS;
        usefulBindingsVars = Vars.EMPTY;
        bindType = BindType.JOIN;
        //noinspection unchecked
        bindingsSent = (SPSCUnitBIt<B>) NO_BINDINGS_SENT;
        metrics = null;
    }

    private static int[] computeBindingCol2OutputCol(Vars out, BIt<?> bindings) {
        Vars bindingsVars = bindings.vars();
        var outCols = new int[bindingsVars.size()];
        for (int i = 0; i < outCols.length; i++)
            outCols[i] = out.indexOf(bindingsVars.get(i));
        return outCols;
    }

    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request.
     *
     * @param frameSender       object to be used when sending WebSocket frames
     * @param batchType         the {@link BatchType} with operations for {@code R}
     * @param vars              See {@link ResultsParserBIt#ResultsParserBIt(BatchType, Vars, int)}
     * @param bindType          the type of bind operation that is being executed with the
     *                          underlying {@code !bind} request
     * @param bindings          a {@link BIt} of bindings that will be sent in to the server as TSV
     *                          chunks in WebSocket frames in parallel to the consumption of this
     *                          {@link BIt} and in response to server-sent {@code !bind-request n}
     *                          frames.
     * @param usefulBindingVars among the vars provided by {@code bindings} only these will be
     *                          sent to the server. This is equivalent to projecting
     *                          {@code bindings} just during the sending phase: for
     *                          {@link BindType#JOIN} and {@link BindType#LEFT_JOIN},
     *                          dropped vars will still be visible in the output rows.
     @param maxBatches          See {@link SPSCBIt#SPSCBIt(BatchType, Vars, int)}
     */
    public WsClientParserBIt(@NonNull WsFrameSender frameSender,
                             @NonNull BatchType<B> batchType, @NonNull Vars vars,
                             @NonNull BindType bindType,
                             @NonNull BIt<B> bindings,
                             @Nullable Vars usefulBindingVars,
                             @Nullable JoinMetrics metrics, int maxBatches) {
        super(frameSender, batchType, vars, maxBatches);
        this.bindings = bindings;
        this.bindType = bindType;
        this.bindingsSent = new SPSCUnitBIt<>(batchType, bindings.vars());
        this.usefulBindingsVars = usefulBindingVars == null ? bindings.vars() : usefulBindingVars;
        assert bindings.vars().containsAll(this.usefulBindingsVars);
        this.bindingCol2outputCol = computeBindingCol2OutputCol(vars, bindings);
        this.metrics = metrics;
        Thread.startVirtualThread(this::sendBindingsThread);
    }

    /**
     * Create a parser for the experimental WebSocket query results of a {@code !bind} request
     * that will redirect all rows and the results completion to {@code destination}
     *
     * @param frameSender object to be used when sending WebSocket frames
     * @param batchType the {@link BatchType} with operations for {@code R}
     * @param destination See {@link ResultsParserBIt#ResultsParserBIt(BatchType, CallbackBIt)}
     * @param bindType the type of bind operation that is being executed with the
     *                 underlying {@code !bind} request
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
    public WsClientParserBIt(@NonNull WsFrameSender frameSender,
                             @NonNull BatchType<B> batchType,
                             @NonNull CallbackBIt<B> destination,
                             @NonNull BindType bindType,
                             @NonNull BIt<B> bindings,
                             @Nullable Vars usefulBindingVars,
                             @Nullable JoinMetrics metrics) {
        super(frameSender, batchType, destination);
        this.bindings = bindings;
        this.bindType = bindType;
        this.bindingsSent = new SPSCUnitBIt<>(batchType, bindings.vars());
        this.usefulBindingsVars = usefulBindingVars == null ? bindings.vars() : usefulBindingVars;
        assert bindings.vars().containsAll(this.usefulBindingsVars);
        this.bindingCol2outputCol = computeBindingCol2OutputCol(vars, bindings);
        this.metrics = metrics;
        Thread.startVirtualThread(this::sendBindingsThread);
    }

    /* --- --- --- specialize SVParserBIt.Tsv methods --- --- --- */

    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        if      (rope.has(begin,   BIND_REQUEST)) handleBindRequest();
        else if (rope.has(begin, ACTIVE_BINDING)) handleActiveBinding(rope, begin, eol);
        else                                      return false;
        return true;
    }

    @Override public void complete(@Nullable Throwable error) {
        if (error == null && bindings != null)
            emitOnBindingsEnd(0);
        super.complete(error);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (metrics != null) metrics.completeAndDeliver(cause, this);
    }

    @Override protected void emitRow() {
        boolean wasEmpty = activeBindingEmpty;
        if (wasEmpty)
            activeBindingEmpty = false;
        switch (bindType) {
            case EXISTS -> { if (wasEmpty) doEmitRow(); }
            case NOT_EXISTS, MINUS -> { }
            default -> doEmitRow();
        }
    }

    private void doEmitRow() {
        if (metrics != null) metrics.rightRowsReceived(1);
        emitRowKeep();
        if (bindings == null) Arrays.fill(row, null);
    }

    private int nextBindingSent() {
        B b = bindingsSentBatch;
        int row = ++bindingsSentBatchRow;
        if (b == null || row >= b.rows) {
            bindingsSentBatch = bindingsSent.nextBatch(b);
            bindingsSentBatchRow = row = 0;
        }
        return row;
    }

    private void emitOnBindingsEnd(long skipped) {
        bindingsReceived += skipped;
        switch (bindType) {
            case JOIN, EXISTS -> {
                while (skipped-- > 0) {
                    if (metrics != null) metrics.beginBinding();
                    nextBindingSent();
                }
            }
            case LEFT_JOIN, NOT_EXISTS, MINUS -> {
                if (activeBindingEmpty && hasActiveBinding)
                    doEmitRow();
                while (skipped-- > 0) {
                    if (metrics != null) metrics.beginBinding();
                    takeActiveBinding();
                    doEmitRow();
                }
            }
        }
        activeBindingEmpty = true;
    }

    private void takeActiveBinding() {
        hasActiveBinding = true;
        int bRow = nextBindingSent();
        B b = bindingsSentBatch;
        if (b == null)
            throw activeBindingOverflow(bindingsReceived);
        Arrays.fill(row, null);
        for (int src = 0; src < bindingCol2outputCol.length; src++) {
            int dst = bindingCol2outputCol[src];
            if (dst >= 0)
                row[dst] = b.get(bRow, src);
        }
    }

    /* --- --- --- parsing for !control messages  --- --- --- */

    private void handleActiveBinding(Rope rope, int begin, int end) {
        if (bindings == null) throw noBindings(ACTIVE_BINDING);
        long index = rope.parseLong(rope.skipWS(begin+ACTIVE_BINDING.length, end));
        long lastIndex = bindingsReceived - 1;
        if (index < lastIndex) throw pastActiveBinding(index);
        if (index == lastIndex) return; // no-op !active-binding

        emitOnBindingsEnd(index - bindingsReceived);
        takeActiveBinding();
        if (metrics != null) metrics.beginBinding();
        bindingsReceived = index+1;
    }

    private void handleBindRequest() {
        if (bindings == null) throw noBindings(BIND_REQUEST);
//        long n = rope.parseLong(rope.skipWS(begin + BIND_REQUEST.length, end));
        bindings.tempEager();
//        lock();
//        try {
//            bindingsRequested += n;
//            signal();
//        } finally { unlock(); }
    }

    /* --- --- --- exception factories --- --- --- */

    private InvalidSparqlResultsException noBindings(Object what) {
        var whatStr = what instanceof byte[] b ? new ByteRope(b) : what.toString();
        return new InvalidSparqlResultsException("Cannot handle "+whatStr+": bindings not set");
    }

    private InvalidSparqlResultsException pastActiveBinding(long index) {
        String msg = "Server sent !active-binding "+index+" in the past. Expected >= "
                   + bindingsReceived;
        return new InvalidSparqlResultsException(msg);
    }

    private InvalidSparqlResultsException activeBindingOverflow(long index) {
        String msg = "Server sent !active-binding " + index + ", but only "
                   + bindingsReceived + " bindings were sent";
        return new InvalidSparqlResultsException(msg);
    }

    /* --- --- --- bindings upload virtual thread --- --- --- */

    private void sendBindingsThread() {
        Thread.currentThread().setName("sendBindingsThread-"+id());
        if (bindings == null)
            throw noBindings("sendBindingsThread()");
        bindings.preferred().tempEager();
        var serializer = new WsSerializer<B>(bindings.vars(), usefulBindingsVars);
        try {
            for (B b = null; (b = bindings.nextBatch(b)) != null; ) {
                frameSender.sendFrame(serializer.serialize(b));
                bindingsSent.copy(b);
            }
        } finally {
            frameSender.sendFrame(END_FRAME);
        }
    }
}
