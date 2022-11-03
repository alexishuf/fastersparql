package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItClosedException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.client.netty.util.NettyCallbackBIt;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.client.parser.results.WebSocketResultsParser;
import com.github.alexishuf.fastersparql.client.parser.results.WebSocketResultsParserConsumer;
import com.github.alexishuf.fastersparql.client.util.FSProperties;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.client.BindType.MINUS;
import static com.github.alexishuf.fastersparql.client.util.Merger.forMerge;
import static com.github.alexishuf.fastersparql.client.util.Merger.forProjection;
import static java.lang.Thread.ofVirtual;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class NettyWebSocketSparqlClient<R, I, F> extends AbstractNettySparqlClient<R, I, F> {
    private static final Logger log = LoggerFactory.getLogger(NettyWebSocketSparqlClient.class);
    private final NettyWsClient netty;

    private static SparqlEndpoint restrictConfig(SparqlEndpoint endpoint) {
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
        return new SparqlEndpoint(endpoint.uri(), offer);
    }

    public NettyWebSocketSparqlClient(SparqlEndpoint ep, RowType<R, I> rowType,
                                      FragmentParser<F> fragmentParser) {
        super(restrictConfig(ep), rowType, fragmentParser);
        var headers = new DefaultHttpHeaders();
        var config = ep.configuration();
        for (Map.Entry<String, String> e : config.headers().entrySet())
            headers.add(e.getKey(), e.getValue());
        for (Map.Entry<String, List<String>> e : config.appendHeaders().entrySet())
            headers.add(e.getKey(), e.getValue());
        try {
            this.netty = new NettyClientBuilder().buildWs(ep.protocol(), ep.toURI(), headers);
        } catch (SSLException e) {
            throw new SparqlClientException("Could not initialize SSL context", e);
        }
    }

    @Override protected String endpointString() {
        return endpoint.toString();
    }

    @Override public boolean        usesBindingAwareProtocol() { return true; }

    @Override
    public BIt<R> query(SparqlQuery sparql) {
        if (sparql.isGraph)
            throw new SparqlClientInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            return new QueryBIt(sparql);
        } catch (Throwable t) { throw SparqlClientException.wrap(endpoint, t); }
    }

    @Override
    public BIt<R> query(SparqlQuery sp, @Nullable BIt<R> bindings, @Nullable BindType bindType) {
        if (bindings == null || bindings instanceof EmptyBIt)
            return query(sp);
        if (bindType == null)
            throw new SparqlClientInvalidArgument("bindType is null with non-null bindings");
        if (sp.isGraph)
            throw new SparqlClientInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            if (bindType == MINUS && !bindings.vars().intersects(sp.allVars))
                return bindings;
            else if (!bindType.isJoin())
                sp = sp.toAsk();
            Vars exVars = bindType.resultVars(bindings.vars(), sp.publicVars);
            return new BindBIt(exVars, bindings, bindType, sp, sp.publicVars, sp.allVars);
        } catch (Throwable t) { throw SparqlClientException.wrap(endpoint, t); }
    }

    @Override
    public Graph<F> queryGraph(SparqlQuery ignored) {
        throw new UnsupportedOperationException("The websocket protocol extension only allows ASK/SELECT queries");
    }

    @Override public void close() { this.netty.close(); }

    /* --- --- --- helper methods --- --- --- */

    private static final String QUERY_VERB = "!query ";
    private static final String BIND_VERB = "!bind ";
    private static String createRequest(String verb, String sparql) {
        return verb + sparql + (sparql.charAt(sparql.length()-1) == '\n' ? "" : "\n");
    }

    /* --- --- --- BIt implementations --- --- --- */

    private abstract class ClientBIt extends NettyCallbackBIt<R>
            implements WsClientHandler, WebSocketResultsParserConsumer {
        private final String requestMessage;
        protected final Vars expectedVars;
        private final WebSocketResultsParser wsResultsParser;
        private boolean gotFrames = false;
        protected @MonotonicNonNull WsRecycler recycler;

        public ClientBIt(String requestMessage, Vars expectedVars) {
            super(rowClass(), expectedVars);
            this.requestMessage = requestMessage;
            this.expectedVars = expectedVars;
            this.wsResultsParser = new WebSocketResultsParser(this);
        }

        /* --- --- --- NettyCallbackBIt methods --- --- --- */

        @Override public    SparqlClient<?, ?, ?>  client() { return NettyWebSocketSparqlClient.this; }
        @Override protected void                  request() { netty.open(this); }
        @Override protected void      afterNormalComplete() { recycler.recycle(channel); }

        /* --- --- --- WsClientHandler methods --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, WsRecycler recycler) {
            assert channel == null : "previous attach()";
            lock.lock();
            try {
                this.recycler = recycler;
                if (ended) {
                    recycler.recycle(ctx.channel());
                    return;
                }
                this.channel = ctx.channel();
            } finally { lock.unlock(); }
            ctx.writeAndFlush(new TextWebSocketFrame(requestMessage));
        }

        @Override public void detach() {
            if (!ended) // flush parser, which may call end() or onError(String)
                wsResultsParser.endMessage();
            lock.lock();
            try {
                if (!ended) {
                    var suffix = gotFrames ? "!end but after " : "" + "starting a response";
                    var ex = new SparqlClientServerException("Connection closed before "+suffix);
                    complete(ex.shouldRetry(!gotFrames));
                }
            } finally { lock.unlock(); }
        }

        @Override public void onError(Throwable error) { complete(error); }

        @Override public void onFrame(WebSocketFrame frame) {
            gotFrames = true;
            if (frame instanceof TextWebSocketFrame t) {
                wsResultsParser.feed(t.text());
            } else if (!ended && !(frame instanceof CloseWebSocketFrame)) {
                var suffix = frame == null ? "null frame" : frame.getClass().getSimpleName();
                complete(new SparqlClientServerException("Unexpected "+suffix));
            }
        }

        /* --- --- --- WebSocketResultsParserConsumer methods --- --- --- */
        @Override public void actionQueue(int n)             { onError("Unexpected !action-queue-cap"); }

        @Override public void   pingAck()   { }
        @Override public void   cancelled() { onError("Server-sent !cancel"); }
        @Override public void   ping()      { channel.writeAndFlush("!ping-ack\n"); }
        @Override public void   end()       { if (!ended) complete(null); }
        @Override public void onError(String message) { complete(new SparqlClientServerException(message)); }
    }

    private class QueryBIt extends ClientBIt {
        private Merger<R, I> projector;
        private Function<String[], R> converter;

        public QueryBIt(SparqlQuery sp) {
            super(createRequest(QUERY_VERB, sp.sparql), sp.publicVars);
            projector = Merger.identity(rowType, sp.publicVars);
            converter = rowType.converter(ArrayRow.STRING, vars);
        }

        @Override public void bindRequest(long n, boolean i) { onError("Unexpected !bind-request"); }
        @Override public void activeBinding(String[] row)    { onError("Unexpected !active-binding"); }

        @Override public void vars(Vars vars) {
            converter = rowType.converter(ArrayRow.STRING, vars);
            projector = forProjection(rowType, expectedVars, vars);
        }
        @Override public void row(@Nullable String[] row) {
            feed(projector.merge(converter.apply(row), null));
        }
    }

    private class BindBIt extends ClientBIt {
        private final BIt<R> bindings;
        private final Vars bindingsVars;
        private final BindType bindType;
        private final Batch<R> fetchedBindings = new Batch<>(elementClass, 22);
        private final ArrayDeque<R> sentBindings = new ArrayDeque<>();
        private final Condition needsBindings = lock.newCondition();
        private final Merger<R, I> usefulBindingsProjector;
        private Function<String[], R> rConverter;
        private Merger<R, I> merger;
        private @Nullable R activeBinding;
        private boolean activeBindingEmpty;
        private int bindingsRequested;

        public BindBIt(Vars expectedVars, BIt<R> bindings, BindType bindType,
                       SparqlQuery sp, Vars pubSparqlVars, Vars allSparqlVars) {
            super(createRequest(BIND_VERB, sp.sparql), expectedVars);
            this.bindings = bindings;
            this.bindingsVars = bindings.vars();
            this.bindType = bindType;
            Vars usefulBindingsVars = bindingsVars.intersection(allSparqlVars);
            this.usefulBindingsProjector = forProjection(rowType, usefulBindingsVars, bindingsVars);
            this.merger = forMerge(rowType, bindingsVars, pubSparqlVars, bindType, expectedVars);
            this.rConverter = rowType.converter(ArrayRow.STRING, merger.rightFreeVars());
        }

        @Override protected void run() {
            ofVirtual().name("BindingsFetcher-"+toStringNoArgs()).start(this::fetchBindings);
            super.run();
        }

        @Override public void complete(@Nullable Throwable error) {
            lock.lock();
            try {
                if (!ended) {
                    try {
                        endActiveBinding();
                    } catch (Throwable t) {
                        if (error == null) {
                            error = t;
                        } else {
                            log.error("{}: ignoring {} from endBinding() called by complete({})",
                                      this, t, error.toString());
                        }
                    }
                }
                super.complete(error);
                if (ended) { // super.complete() may trigger a retry, returning with !ended
                    bindings.close();
                    needsBindings.signal();
                }
            } finally { lock.unlock(); }
        }

        /* --- --- --- binding fetching/sending --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, WsRecycler recycler) {
            super.attach(ctx, recycler);
            if (ctx != null) { // send bindings headers
                var vars = usefulBindingsProjector.outVars();
                int varsSize = vars.size();
                ByteBuf bb = ctx.alloc().buffer(16 * varsSize);
                for (String name : vars) {
                    bb.writeByte('?');
                    bb.writeCharSequence(name, UTF_8);
                    bb.writeByte('\t');
                }
                if (varsSize > 0)
                    bb.writerIndex(bb.writerIndex()-1);
                bb.writeByte('\n');
                ctx.writeAndFlush(new TextWebSocketFrame(bb));
            }
        }

        private void fetchBindings() {
            try {
                long floor = FSProperties.consumedBatchMinWait(MILLISECONDS);
                if (bindings.minWait(MILLISECONDS) < floor)
                    bindings.minWait(floor, MILLISECONDS);
                if (bindings.maxBatch() < 1024)
                    bindings.maxBatch(1024);
                bindings.tempEager();
                for (var b = bindings.nextBatch(); !ended && b.size > 0; b = bindings.nextBatch()) {
                    lock.lock();
                    try {
                        fetchedBindings.add(b.array, 0, b.size);
                        if (bindingsRequested > 0)
                            sendBindings();
                        else
                            needsBindings.awaitUninterruptibly();
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Throwable t) {
                if (ended && BItClosedException.isClosedExceptionFor(t, bindings))
                    log.trace("{}: ignoring {}", this, t.toString());
                else
                    complete(t);
            } finally {
                lock.lock();
                try {
                    if (channel != null)
                        channel.writeAndFlush(new TextWebSocketFrame("!end\n"));
                } finally { lock.unlock(); }
                bindings.close();
            }
        }

        private void sendBindings() {
            int rowsSent = 0;
            while (fetchedBindings.size > 0 && bindingsRequested > 0) {
                int taken = 0, eligible = Math.min(fetchedBindings.size, bindingsRequested);
                ByteBuf bb = channel.alloc().buffer(Math.min(65_536, eligible*256));
                List<String> usefulVars = usefulBindingsProjector.outVars();
                int usefulVarsSize = usefulVars.size();
                R[] rows = fetchedBindings.array;
                do { // serialize rows
                    R fullRow = rows[taken++];
                    sentBindings.add(fullRow);
                    R usefulRow = usefulBindingsProjector.merge(fullRow, null);
                    for (int i = 0; i < usefulVarsSize; i++) {
                        String nt = rowType.getNT(usefulRow, i);
                        if (nt != null)
                            bb.writeCharSequence(nt, UTF_8);
                        bb.writeByte('\t');
                    }
                    if (usefulVarsSize > 0)
                        bb.writerIndex(bb.writerIndex()-1);
                    bb.writeByte('\n');
                } while (taken < eligible && bb.readableBytes() < 64_512);
                channel.write(new TextWebSocketFrame(bb));
                fetchedBindings.remove(0, taken);
                rowsSent += taken;
            }
            if (rowsSent > 0)
                channel.flush();
        }

        @Override public void bindRequest(long n, boolean incremental) {
            lock.lock();
            try {
                bindingsRequested += n;
                needsBindings.signal();
                sendBindings();
            } finally { lock.unlock(); }
        }

        /* --- --- --- result row processing --- --- --- */

        private void endActiveBinding() {
            if (activeBinding != null) {
                if (activeBindingEmpty) {
                    if (bindType == MINUS || bindType == BindType.NOT_EXISTS)
                        feed(activeBinding);
                    else if (bindType == BindType.LEFT_JOIN)
                        feed(merger.merge(activeBinding, null));
                }
                activeBinding = null;
            }
        }

        @Override public void activeBinding(String[] row) {
            lock.lock();
            try {
                endActiveBinding();
                activeBinding = sentBindings.poll();
                activeBindingEmpty = true;
                if (activeBinding == null) {
                    throw new InvalidSparqlResultsException(endpoint,
                            "More !active-bindings than sent binding rows");
                }
                assert currentBindingIsFullRowOf(row) : "active-binding != sentBinding";
            } finally { lock.unlock(); }
        }

        private boolean currentBindingIsFullRowOf(String[] row) {
            if (usefulBindingsProjector.outVars().size() == 0) {
                return row.length == 0 || (row.length == 1 && row[0] == null);
            } else {
                R activeProjected = usefulBindingsProjector.merge(activeBinding, null);
                R parsed = rowType.convert(ArrayRow.STRING, usefulBindingsProjector.outVars, row);
                return rowType.equalsSameVars(activeProjected, parsed);
            }
        }

        @Override public void row(@Nullable String[] row) {
            activeBindingEmpty = false;
            feed(bindType.isJoin() ? merger.merge(activeBinding, rConverter.apply(row))
                                   : activeBinding);
        }

        @Override public void vars(Vars vars) {
            rConverter = rowType.converter(ArrayRow.STRING, vars);
            merger = forMerge(rowType, bindingsVars, vars, bindType, this.vars);
        }
    }
}
