package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.WebSocketResultsParser;
import com.github.alexishuf.fastersparql.client.parser.results.WebSocketResultsParserConsumer;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.reactive.CallbackPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.concurrent.EventExecutor;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.alexishuf.fastersparql.client.BindType.*;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.emptyList;


public class NettyWebSocketSparqlClient<R, F> implements SparqlClient<R, F> {
    private static final AtomicInteger NEXT_QUERY_HANDLER_ID = new AtomicInteger(1);
    private static final AtomicInteger NEXT_BIND_HANDLER_ID = new AtomicInteger(1);
    private static final StringArrayOperations ARRAY_OPS = StringArrayOperations.get();

    private static final Logger log = LoggerFactory.getLogger(NettyWebSocketSparqlClient.class);
    private final SparqlEndpoint endpoint;
    private final RowParser<R> rowParser;
    private final FragmentParser<F> fragmentParser;
    private final NettyWsClient netty;
    private final RowOperations rowOps;

    public NettyWebSocketSparqlClient(@lombok.NonNull SparqlEndpoint endpoint,
                                      @lombok.NonNull RowParser<R> rowParser,
                                      @lombok.NonNull FragmentParser<F> fragmentParser) {
        this.rowParser = rowParser;
        this.rowOps = RowOperationsRegistry.get().forClass(rowParser.rowClass());
        this.fragmentParser = fragmentParser;
        SparqlConfiguration request = endpoint.configuration();
        SparqlConfiguration offer = request.toBuilder()
                .clearMethods().method(SparqlMethod.WS)
                .clearResultsAccepts().resultsAccept(SparqlResultFormat.TSV)
                .build();
        String cause = null;
        if (!request.resultsAccepts().contains(SparqlResultFormat.TSV))
            cause = "Only TSV results are supported in the Websocket protocol";
        else if (!request.methods().contains(SparqlMethod.WS))
            cause = "NettyWebSocketSparqlClient only supports websocket method";
        else if (!request.headers().isEmpty() || !request.appendHeaders().isEmpty())
            cause = "There is no support for headers in the Websocket protocol";
        if (cause != null)
            throw new UnacceptableSparqlConfiguration(endpoint.uri(), offer, request, cause);
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        for (Map.Entry<String, String> e : offer.headers().entrySet())
            headers.add(e.getKey(), e.getValue());
        for (Map.Entry<String, List<String>> e : offer.appendHeaders().entrySet())
            headers.add(e.getKey(), e.getValue());
        this.endpoint = new SparqlEndpoint(endpoint.uri(), offer);
        try {
            this.netty = new NettyClientBuilder().buildWs(endpoint.protocol(), endpoint.toURI(), headers);
        } catch (SSLException e) {
            throw new SparqlClientException("Could not initialize SSL context", e);
        }

    }

    @Override public Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowParser.rowClass();
    }

    @Override public Class<F> fragmentClass() {
        //noinspection unchecked
        return (Class<F>) fragmentParser.fragmentClass();
    }

    @Override public SparqlEndpoint endpoint() {
        return endpoint;
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration,
                            @Nullable Results<R> bindings, @Nullable BindType bindType) {
        if (bindings == null)
            return query(sparql, configuration);
        else if (bindType == null)
            throw new NullPointerException("bindings != null, but bindType is null!");
        if (bindType == MINUS && !VarUtils.hasIntersection(bindings.vars(), bindings.vars()))
            return bindings;
        BindHandler h = null;
        try {
            h = new BindHandler(sparql, bindings, bindType);
            return asResults(h);
        } catch (Throwable t) {
            return Results.error(h == null ? emptyList() : h.vars, rowClass(), t);
        }
    }

    @Override public boolean usesBindingAwareProtocol() {
        return true;
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        QueryHandler h = null;
        try {
            h = new QueryHandler(sparql);
            return asResults(h);
        } catch (Throwable e) {
            return Results.error(h == null ? emptyList() : h.vars, rowClass(), e);
        }
    }

    @NonNull private Results<R> asResults(Handler h) {
        Results<String[]> raw = new Results<>(h.vars, String[].class, h.publisher);
        FSPublisher<R> parsedPub = rowParser.parseStringsArray(raw);
        if (parsedPub == raw.publisher()) //noinspection unchecked
            return (Results<R>) raw;
        return new Results<>(h.vars, rowParser.rowClass(), parsedPub);
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        throw new UnsupportedOperationException("The websocket protocol extension only allows " +
                                                "ASK/SELECT queries");
    }

    @Override public void close() {
        netty.close();
    }

    @Override public String toString() {
        int id = System.identityHashCode(this);
        return String.format("NettyWSSparqlClient[%s]@%x", endpoint.uri(), id);
    }

    private enum HandlerState {
        CREATED,
        ACTIVE,
        COMPLETED,
        FAILED,
        CANCELLED;

        boolean isTerminal() { return this == FAILED || this == COMPLETED; }
    }

    private abstract class Handler implements WsClientHandler {
        private final int id;
        private final boolean tracing = log.isTraceEnabled();
        protected final List<String> vars;
        protected @MonotonicNonNull ChannelHandlerContext ctx;
        private @MonotonicNonNull WsRecycler recycler;
        protected final CallbackPublisher<String[]> publisher;
        private final AtomicReference<HandlerState> state = new AtomicReference<>(HandlerState.CREATED);

        public Handler(int id, List<String> vars) {
            this.id = id;
            this.vars = vars;
            this.publisher = new Publisher();
        }

        private class Publisher extends CallbackPublisher<String[]> {
            public Publisher() { super(Handler.this.name()+".publisher"); }
            @Override protected void onRequest(long n) {
                Handler.this.request(n);
            }
            private final Runnable AR_BACKPRESSURE = () -> autoRead(false, "backpressure");
            @Override protected void onBackpressure() {
                inEventLoop(AR_BACKPRESSURE, "autoRead(false, onBackpressure)");
            }
            @Override protected void onCancel() {
                inEventLoop(() -> {
                    if (advanceState(HandlerState.CANCELLED)) {
                        if (ctx != null) {
                            if (ctx.channel().isOpen()) {
                                ctx.writeAndFlush(new TextWebSocketFrame("!cancel\n")).addListener(f -> {
                                    if (f.isSuccess() && ctx != null)
                                        ctx.close(); // will trigger a future detach()
                                });
                            } else {
                                log.trace("{} ignoring onCancel(): channel closed", this);
                            }
                        } else {
                            log.trace("{}.onCancel(): cancelled but before attach/after detach", this);
                        }
                    } else {
                        log.debug("{} ignoring doCancel(): already terminated/cancelled", this);
                    }
                }, "cancel");
            }
        }

        protected abstract WebSocketResultsParser resultsParser();

        /* --- --- --- WsClientHandler methods --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, WsRecycler recycler) {
            assert this.ctx == null : "Previous attach()";
            assert ctx.executor().inEventLoop() : "Called from outside event loop";
            this.ctx = ctx;
            this.recycler = recycler;
            if (state.get() == HandlerState.CANCELLED) {
                log.debug("cancelled before attach, will recycle");
                recycle();
            } else {
                this.ctx.channel().config().setAutoRead(true);
            }
        }

        @Override public void detach() {
            assert ctx != null : "No previous attach() call";
            assert ctx.executor().inEventLoop() : "Called from outside the event loop";
            this.ctx = null;
            if (state.get() == HandlerState.CANCELLED) {
                log.debug("{}.cancel() called, will not complete() on detach()", this);
            } else {
                String msg = "Remote peer closed WebSocket session before completing results";
                tryComplete(null, msg);
            }
        }

        @Override public void onError(Throwable cause) { tryComplete(cause, "Unknown"); }

        @Override public void onFrame(WebSocketFrame frame) {
            assert ctx.executor().inEventLoop() : "Called from outside the event loop";
            if (frame instanceof TextWebSocketFrame)
                resultsParser().feed(((TextWebSocketFrame) frame).text());
            else if (!(frame instanceof CloseWebSocketFrame))
                tryComplete(null, "Unexpected "+frame.getClass().getSimpleName());
        }

        private String name() {
            return NettyWebSocketSparqlClient.this + "." + getClass().getSimpleName() + "-" + id;
        }

        @Override public String toString() {
            StringBuilder sb = new StringBuilder(256);
            sb.append(name()).append('{').append(state.get()).append(", ");
            boolean hasChannel = false;
            try { // wrap on try-catch as we are racing on ctx != null
                if (ctx != null) {
                    Channel ch = ctx.channel();
                    sb.append("ch=").append(ch).append(ch.isOpen() ? ", open" : ", closed");
                    hasChannel = true;
                }
            } catch (NullPointerException ignored) {}
            return sb.append(hasChannel ? "" : "detached").append('}').toString();
        }

        /* --- --- --- state management: may be called outside the Netty event loop --- --- --- */

        protected void onStateChange(HandlerState state) {
            if (state == HandlerState.COMPLETED)
                recycle();
        }

        private boolean advanceState(HandlerState target) {
            boolean change = false;
            for (HandlerState old = state.get(); !change && !old.isTerminal(); old = state.get())
                change = advanceState(old, target);
            return change;
        }

        private boolean advanceState(HandlerState origin, HandlerState target) {
            if (state.compareAndSet(origin, target)) {
                try {
                    EventExecutor executor = safeExecutor();
                    if (executor == null || executor.inEventLoop())
                        onStateChange(target);
                    else
                        executor.execute(() -> onStateChange(target));
                } catch (Throwable t) {
                    log.error("{}.state({}): ignoring exception", this, target, t);
                }
                return true;
            }
            return false;
        }

        protected void tryComplete(@Nullable Throwable cause, @Nullable String message) {
            HandlerState target = cause != null || message != null
                                ? HandlerState.FAILED : HandlerState.COMPLETED;
            if (advanceState(target)) {
                if (cause == null && message != null)
                    cause = new SparqlClientServerException(message);
                publisher.complete(cause);
            }
        }

        private final Runnable AR_REQUEST = () -> autoRead(true, "request");
        protected void request(long ignoredN) {
            if (advanceState(HandlerState.CREATED, HandlerState.ACTIVE))
                netty.open(this);
            inEventLoop(AR_REQUEST, "autoRead(true, request)");
        }

        protected void sendFrame(String message) {
            EventExecutor executor = safeExecutor();
            if (executor == null) {
                log.debug("{}: ignoring sendFrame({}): detached", this, message);
            } else if (executor.inEventLoop()) {
                if (state.get().isTerminal() || state.get() == HandlerState.CANCELLED) {
                    log.debug("{}: ignoring sendFrame({}): terminated/cancelled", this, message);
                } else {
                    if (tracing)
                        log.trace("{}: sendFrame({})", this, message.replace("\n", "\\n"));
                    ctx.writeAndFlush(new TextWebSocketFrame(message));
                }
            } else {
                executor.execute(() -> sendFrame(message));
            }
        }

        /* --- --- --- event loop helpers --- --- --- */

        private @Nullable EventExecutor safeExecutor() {
            try {
                return ctx != null ? ctx.executor() : null;
            } catch (NullPointerException ignored) {}
            return null;
        }

        protected final void inEventLoop(Runnable runnable, String name) {
            EventExecutor el = safeExecutor();
            if (el != null) {
                if (el.inEventLoop())
                    runnable.run();
                else
                    el.execute(runnable);
            } else if (state.get().isTerminal()) {
                log.trace("{} ignoring {}: detached", this, name);
            }
        }

        /* --- --- --- methods that must be called from the event loop --- --- --- */

        protected void recycle() {
            if (ctx != null) {
                assert ctx.executor().inEventLoop() : "Called from outside the event loop";
                autoRead(true, "recycle");
                recycler.recycle(ctx.channel());
                this.ctx = null;
            }
        }

        protected void autoRead(boolean value, String reason) {
            if (ctx != null) {
                assert ctx.executor().inEventLoop() : "Called from outside the event loop";
                if (ctx.channel().isOpen()) {
                    ChannelConfig cfg = ctx.channel().config();
                    if (cfg.isAutoRead() != value) {
                        log.debug("{}: changing autoRead to {}. Reason: {}", this, value, reason);
                        cfg.setAutoRead(value);
                    }
                } else {
                    log.debug("{} ignoring autoRead({}, {}): channel closed", this, value, reason);
                }
            } else if (state.get().isTerminal()) {
                log.trace("{} ignoring autoRead({}, {}): detached", this, value, reason);
            }
        }

        private void cancel() {
            assert ctx.executor().inEventLoop() : "called from outside the event loop";
            if (advanceState(HandlerState.CANCELLED)) {
                if (ctx != null) {
                    if (ctx.channel().isOpen()) {
                        ctx.writeAndFlush(new TextWebSocketFrame("!cancel\n")).addListener(f -> {
                            if (f.isSuccess() && ctx != null)
                                ctx.close(); // will trigger a future detach()
                        });
                    } else {
                        log.debug("{} ignoring doCancel(): channel closed", this);
                    }
                } else {
                    log.debug("{}.onCancel(): cancelled but before attach/after detach", this);
                }
            } else {
                log.debug("{} ignoring doCancel(): already cancelled", this);
            }
        }
    }

    private class QueryHandler extends Handler {
        private @Nullable String queryMessage;
        private final WebSocketResultsParser resultsParser
                = new WebSocketResultsParser(new WebSocketResultsParserConsumer() {
            private Merger<String[]> projector = Merger.identity(ARRAY_OPS, QueryHandler.this.vars);
            @Override public void bindRequest(long n, boolean incremental) {
                tryComplete(null, "Unexpected !bind-request");
            }
            @Override public void actionQueue(int n) {
                tryComplete(null, "Unexpected !action-queue-cap");
            }
            @Override public void activeBinding(String[] row) {
                tryComplete(null, "Unexpected !active-binding");
            }
            @Override public void vars(List<String> vars) {
                projector = Merger.forProjection(ARRAY_OPS, QueryHandler.this.vars, vars);
            }
            @Override public void row(@Nullable String[] row) {
                publisher.feed(projector.merge(row, null));
            }
            @Override public void     end()               { tryComplete(null, null); }
            @Override public void onError(String message) { tryComplete(null, message); }
        });

        public QueryHandler(CharSequence query) {
            super(NEXT_QUERY_HANDLER_ID.getAndIncrement(), SparqlUtils.publicVars(query));
            boolean needsSuffix = query.length() > 0 && query.charAt(query.length() - 1) != '\n';
            this.queryMessage = "!query " + query + (needsSuffix ? "\n" : "");
        }

        @Override protected WebSocketResultsParser resultsParser() { return resultsParser; }

        @Override public void attach(ChannelHandlerContext ctx, WsRecycler recycler) {
            super.attach(ctx, recycler);
            if (this.ctx != null) {
                assert queryMessage != null;
                sendFrame(queryMessage);
                queryMessage = null;
            }
        }
    }

    private static class BindingProjector {
        private final RowOperations rowOps;
        private final List<String> inVars;
        private final int[] srcCol;
        private final String[] outVars;
        private final StringBuilder sb = new StringBuilder();
        private final ConcurrentLinkedQueue<String[]> sent = new ConcurrentLinkedQueue<>();

        public BindingProjector(RowOperations rowOps, List<String> inVars,
                                CharSequence sparql) {
            this.inVars = inVars;
            this.rowOps = rowOps;
            List<@MinLen(1) String> wantedVars = SparqlUtils.allVars(sparql);
            int size = 0, capacity = Math.min(inVars.size(), wantedVars.size());
            String[] outVars = new String[capacity];
            int[] srcCol = new int[capacity];
            for (int i = 0, offeredVarsSize = inVars.size(); i < offeredVarsSize; i++) {
                String name = inVars.get(i);
                if (wantedVars.contains(name)) {
                    srcCol[size] = i;
                    outVars[size++] = name;
                }
            }
            this.srcCol = size == srcCol.length ? srcCol : copyOfRange(srcCol, 0, size);
            this.outVars = size == outVars.length ? outVars : copyOfRange(outVars, 0, size);
        }

        public String tsvHeaders() {
            sb.setLength(0);
            for (String name : outVars) sb.append('?').append(name).append('\t');
            sb.setLength(Math.max(0, sb.length()-1));
            sb.append('\n');
            return sb.toString();
        }

        private static String sanitizeNT(@Nullable String nt) {
            if (nt == null) return "";
            if (nt.indexOf('\t') >= 0) nt = nt.replace("\t", "\\t");
            if (nt.indexOf('\n') >= 0) nt = nt.replace("\n", "\\n");
            return nt;
        }

        public String tsvRow(Object row) {
            if (row instanceof String[]) {
                sent.add((String[]) row);
            } else {
                String[] full = new String[inVars.size()];
                for (int i = 0; i < full.length; i++)
                    full[i] = rowOps.getNT(row, i, inVars.get(i));
                sent.add(full);
            }
            sb.setLength(0);
            for (int i = 0; i < srcCol.length; i++)
                sb.append(sanitizeNT(rowOps.getNT(row, srcCol[i], outVars[i]))).append('\t');
            if (srcCol.length > 0)
                sb.setLength(sb.length()-1);
            return sb.append('\n').toString();
        }

        public String[] fullRow(String[] projected) {
            String[] full = sent.poll();
            if (full == null)
                throw new IllegalStateException("Received more projected bindings than sent");
            assert matches(full, projected) : "not a projection";
            return full;
        }

        private boolean matches(String[] full, String[] projected) {
            for (int i = 0; i < srcCol.length; i++) {
                if (!Objects.equals(full[srcCol[i]], projected[i]))
                    return false;
            }
            return true;
        }
    }

    private class BindHandler extends Handler {
        private final Results<R> bindings;
        private final BindType bindType;
        private final BindingProjector bProjector;
        private @Nullable String firstMessage;
        private @MonotonicNonNull Subscription bindingsSubscription;

        private final WebSocketResultsParser resultsParser = new WebSocketResultsParser(new WebSocketResultsParserConsumer() {
            private @MonotonicNonNull String[] binding;
            private boolean empty = true;
            private @MonotonicNonNull Merger<String[]> merger;
            private boolean sentHeaders = false;

            private void endBinding() {
                if (binding != null) {
                    if (empty) {
                        if (bindType == MINUS || bindType == NOT_EXISTS)
                            publisher.feed(binding);
                        else if (bindType == LEFT_JOIN)
                            publisher.feed(merger.merge(binding, null));
                    }
                    binding = null;
                }
            }

            @Override public void bindRequest(long n, boolean incremental) {
                if (!sentHeaders) {
                    sentHeaders = true;
                    sendFrame(bProjector.tsvHeaders());
                }
                if (bindingsSubscription != null)
                    bindingsSubscription.request(n);
                else
                    assert false : "bindingsSubscription is still after !bind-request";
            }

            @Override public void vars(List<String> vars) {
                assert merger == null : "vars() called more than once";
                merger = Merger.forMerge(ARRAY_OPS, bindings.vars(), vars, bindType);
            }

            @Override public void row(@Nullable String[] row) {
                empty = false;
                if (!bindType.isJoin()) {
                    if (bindType == EXISTS)
                        publisher.feed(binding);
                    // else: do not emit and set empty=false, preventing emission by endBinding()
                } else {
                    if (merger == null)
                        tryComplete(new IllegalStateException("row() called before vars()"), null);
                    publisher.feed(merger.merge(binding, row));
                }
            }

            @Override public void activeBinding(String[] row) {
                endBinding();
                if (bindings.vars().isEmpty()) {
                    binding = new String[0];
                } else {
                    assert row.length == bProjector.outVars.length
                            : "columns in activeBinding != bProjector.outVars.length";
                    binding = bProjector.fullRow(row);
                }
                empty = true;
            }
            @Override public void end()                       {
                endBinding();
                tryComplete(null, null);
            }
            @Override public void actionQueue(int n)          { }
            @Override public void onError(String message)     { tryComplete(null, message); }
        });

        private final Subscriber<R> bindingsSubscriber = new Subscriber<R>() {
            @Override public void onNext(R r)                 { sendFrame(bProjector.tsvRow(r)); }
            @Override public void onSubscribe(Subscription s) { bindingsSubscription = s; }
            @Override public void onError(Throwable t)        { tryComplete(t, null); }
            @Override public void onComplete()                { sendFrame("!end\n"); }
        };

        public BindHandler(CharSequence sparql, Results<R> bindings, BindType bindType) {
            super(NEXT_BIND_HANDLER_ID.getAndIncrement(),
                  bindType.resultVars(bindings.vars(), SparqlUtils.publicVars(sparql)));
            this.bindings = bindings;
            this.bindType = bindType;
            if (!bindType.isJoin())
                sparql = SparqlUtils.toAsk(sparql);
            this.bProjector = new BindingProjector(rowOps, bindings.vars(), sparql);
            boolean hasNewline = sparql.charAt(sparql.length() - 1) == '\n';
            this.firstMessage = "!bind " + sparql + (hasNewline ? "" : "\n");
        }

        @Override protected WebSocketResultsParser resultsParser() { return resultsParser; }

        @Override protected void onStateChange(HandlerState state) {
            super.onStateChange(state);
            switch (state) {
                case ACTIVE:
                    if (bindingsSubscription == null)
                        bindings.publisher().subscribe(bindingsSubscriber);
                    else
                        assert false : "transitioned to ACTIVE with non-null bindingsSubscription";
                    break;
                case COMPLETED:
                case FAILED:
                case CANCELLED:
                    if (bindingsSubscription != null)
                        bindingsSubscription.cancel();
                    break;
            }
        }

        @Override public void attach(ChannelHandlerContext ctx, WsRecycler recycler) {
            super.attach(ctx, recycler);
            if (this.ctx != null) {
                assert firstMessage != null;
                sendFrame(firstMessage);
                firstMessage = null;
            }
        }
    }
}
