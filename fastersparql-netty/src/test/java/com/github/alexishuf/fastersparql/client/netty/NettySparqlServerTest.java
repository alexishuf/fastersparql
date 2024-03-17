package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.client.ResultsSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.FlowModel.EMIT;
import static com.github.alexishuf.fastersparql.FlowModel.ITERATE;
import static com.github.alexishuf.fastersparql.batch.Timestamp.nanoTime;
import static com.github.alexishuf.fastersparql.client.model.SparqlMethod.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.JSON;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NettySparqlServerTest {
    private NettySparqlServer createServer(FlowModel flowModel, Results results,
                                           @Nullable ResultsSparqlClient innerClient) {
        boolean shared = innerClient != null;
        if (innerClient == null) {//noinspection resource
            innerClient = new ResultsSparqlClient(false);
            innerClient.answerWith(results.query(), results);
        }
        return new NettySparqlServer(flowModel, innerClient, shared, "0.0.0.0", 0);
    }

    private SparqlClient createClient(NettySparqlServer server, SparqlResultFormat fmt,
                                      SparqlMethod meth) {
        int port = server.port();
        var ep = SparqlEndpoint.parse((meth == WS ? "ws://" : fmt.lowercase()+","+meth+"@http://")
                + "127.0.0.1:"+port+"/sparql");
        return FS.clientFor(ep);
    }

    record Scenario(FlowModel flowModel, Results results, ResultsSparqlClient innerClient,
                    SparqlResultFormat fmt, SparqlMethod meth, BatchType<?> bt) { }

    private static ResultsSparqlClient rsClient(boolean nativeBind) {
        String ep = "http://"+nextRSClientId.getAndAdd(1)+".example.org/sparql";
        var client = new ResultsSparqlClient(nativeBind, ep);
        GUARDS.add(client.retain());
        return client;
    }

    private static final AtomicInteger nextRSClientId = new AtomicInteger(1);
    private static final List<SparqlClient.Guard> GUARDS = new ArrayList<>();
    private static final List<Scenario> SCENARIOS = new ArrayList<>();

    @BeforeAll @SuppressWarnings("resource") static void beforeAll() {
        var askFalse = Results.negativeResult()
                .query("ASK { exns:Bob foaf:name \"bob\"}");
        var askTrue = Results.positiveResult()
                .query("ASK { ?x a foaf:Person }");
        var wide = results("?x ?y ?z", "exns:Bob", "\"bob\"@en", 7)
                .query("SELECT * WHERE { ?x foaf:name ?y; :order ?z }");
        var tall = results("?x", "<rel>", "\"bob\"", "_:a0")
                .query("SELECT ?x WHERE { ?s :p ?x }");
        List<Results> base = List.of(askFalse, askTrue, wide, tall);
        record Proto(Results results, @Nullable ResultsSparqlClient innerClient) {
            public Proto(Results results) { this(results, null); }
        }
        var data = base.stream().map(Proto::new).collect(toCollection(ArrayList::new));
        //empty bindings -> empty results
        for (Results r : base) {
            // no vars empty bindings -> no results
            Results withNegative = results(r.vars()).query(r.query()).bindings(List.of());
            data.add(new Proto(withNegative,
                               rsClient(true)
                                    .answerWith(r.query(), withNegative.noBindings())
                                    .forBindings(r.query(), Vars.EMPTY, r.vars()).end()));
            // positive ask bindings -> same results
            Results withPositive = r.bindings(List.of(List.of()));
            data.add(new Proto(withPositive,
                               rsClient(true)
                                    .answerWith(r.query(), withPositive.noBindings())
                                    .forBindings(r.query(), Vars.EMPTY, r.vars())
                                       .answer().with(withPositive.expected()).end()));
        }

        //test JOIN bind
        data.add(new Proto(wide.bindings("?x", ":Alice", "exns:Bob", ":Charlie"),
                           rsClient(true)
                                   .answerWith(wide.query(), wide)
                                   .forBindings(wide.query(), Vars.of("x"), Vars.of("y", "z"))
                                        .answer(":Alice").with()
                                        .answer("exns:Bob").with("\"bob\"@en", 7)
                                        .answer(":Charlie").with()
                                        .end()));

        //test LEFT_JOIN bind
        var wideLeft = results(
                "?x ?y ?z",
                ":Alice", null, null,
                "exns:Bob", "\"bob\"@en", 7,
                ":Charlie", null, null).query(wide.query())
                                       .bindings("?x", ":Alice", "exns:Bob", ":Charlie")
                                       .bindType(BindType.LEFT_JOIN);
        data.add(new Proto(wideLeft,
                            rsClient(true)
                                    .answerWith(wideLeft.query(), wideLeft)
                                    .forBindings(wideLeft.query(), Vars.of("x"), Vars.of("y", "z"))
                                        .answer(":Alice").with(null, null)
                                        .answer("exns:Bob").with("\"bob\"@en", 7)
                                        .answer(":Charlie").with(null, null)
                                        .end()));

        // generate variations on results (format, method, batch type)
        var methods = List.of(GET, POST, FORM, WS);
        for (var proto  : data) {
            var r = proto.results;
            var ic = proto.innerClient;
            for (var bType : List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED)) {
                for (SparqlMethod meth : methods) {
                    if (meth == WS) continue;
                    for (var fmt : List.of(TSV, JSON))
                        SCENARIOS.add(new Scenario(EMIT, r, ic, fmt, meth, bType));
                }
                if (ic == null) {
                    ic = rsClient(false)
                            .forBindings(r.query(), Vars.EMPTY, r.query().publicVars())
                                .answer()
                                    .with(r.expected())
                                .end();
                }
                SCENARIOS.add(new Scenario(EMIT, r, ic.asEmulatingWs(),
                                           SparqlResultFormat.WS, WS, bType));
            }
        }
        for (int i = 0, n = SCENARIOS.size(); i < n; i++) {
            var s = SCENARIOS.get(i);
            SCENARIOS.add(new Scenario(ITERATE, s.results, s.innerClient, s.fmt, s.meth, s.bt));
        }
    }

    @AfterAll
    static void afterAll() {
        GUARDS.forEach(SparqlClient.Guard::close);
        GUARDS.clear();
        SCENARIOS.clear();
    }

    static Stream<Arguments> test() {
        return SCENARIOS.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(Scenario s) {
        if (s.meth == WS && s.fmt != SparqlResultFormat.WS)
            return;
        if (s.innerClient == null)
            assertFalse(s.results.hasBindings());
        int concurrent = Runtime.getRuntime().availableProcessors();
        long minNs = 400_000_000L;
        try (var server = createServer(s.flowModel, s.results, s.innerClient);
             var client = createClient(server, s.fmt, s.meth)) {
            Watchdog.reset();
            s.results.check(client, s.bt);
            long startNs = nanoTime(); // repeat same test serially
            for (int i = 0; i < 200 || nanoTime()-startNs < minNs; i++) {
                Watchdog.reset();
                s.results.check(client, s.bt);
            }
            startNs = nanoTime(); // saturate CPUs with same test
            for (int i = 0; i < 10 || nanoTime()-startNs < minNs; i++) {
                Watchdog.reset();
                assertEquals(List.of(), range(0, concurrent).parallel().mapToObj(ignored -> {
                    try {
                        s.results.check(client, s.bt);
                        return null;
                    } catch (Throwable t) { return t; }
                }).filter(Objects::nonNull).toList());
            }
        }
    }

    static Stream<Arguments> methodsAndFormats() {
        return Arrays.stream(FlowModel.values()).flatMap(fm -> Stream.of(
                arguments(fm, GET,  TSV),
                arguments(fm, GET,  JSON),
                arguments(fm, POST, TSV),
                arguments(fm, POST, JSON),
                arguments(fm, FORM, TSV),
                arguments(fm, FORM, JSON),
                arguments(fm, WS,   SparqlResultFormat.WS)
        ));
    }

    public static class TestException extends RuntimeException {
        public TestException(String message) {
            super(message);
        }
    }

    @ParameterizedTest @MethodSource("methodsAndFormats")
    void testThrowInServer(FlowModel flowModel, SparqlMethod meth,  SparqlResultFormat fmt) {
        Results res = results("?x").error(FSServerException.class)
                                   .query("SELECT * WHERE { ?x a foaf:Person }");
        try (var inner = new ResultsSparqlClient(true)
                .answerWith(res.query(), new TestException("throw-in-server"));
             var server = createServer(flowModel, res, inner);
             var client = createClient(server, fmt, meth)) {
            res.check(client);
            res.check(client); // repeatable
        }
    }

    record ServerScenario(FlowModel flowModel, Results results, ResultsSparqlClient innerClient) {
        public ServerScenario(Scenario s) {this(s.flowModel, s.results, s.innerClient);}

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            return obj instanceof ServerScenario r
                    && Objects.equals(this.flowModel, r.flowModel)
                    && this.results == r.results && this.innerClient == r.innerClient;
        }
    }

    @Test void parallelTest() throws Exception {
        int nIterations = Runtime.getRuntime().availableProcessors();
        Map<ServerScenario, List<Scenario>> groups = new HashMap<>();
        for (Scenario s : SCENARIOS) {
            groups.computeIfAbsent(new ServerScenario(s), ignored -> new ArrayList<>()).add(s);
        }
        //noinspection MismatchedQueryAndUpdateOfCollection
        try (var servers = new AutoCloseableSet<>();
             var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            for (var entry : groups.entrySet()) {
                ServerScenario ss = entry.getKey();
                var server = createServer(ss.flowModel, ss.results, ss.innerClient);
                servers.add(server);
                for (Scenario s : entry.getValue()) {
                    tasks.add(() -> {
                        try (var client = createClient(server, s.fmt, s.meth)) {
                            for (int i = 0; i < nIterations; i++)
                                s.results.check(client, s.bt);
                        }
                    });
                }
                tasks.awaitAndReset();
            }
        }
    }

}