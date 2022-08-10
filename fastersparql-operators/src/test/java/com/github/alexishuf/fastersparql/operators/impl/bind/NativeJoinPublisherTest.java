package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.CallbackPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.DummySparqlClient;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.async.Async.async;
import static com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher.bindToAny;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NativeJoinPublisherTest {
    private static class MockClient extends DummySparqlClient<String[], byte[]> {
        private static final AtomicInteger nextId = new AtomicInteger(1);
        private final Map<String, List<List<String>>> results = new HashMap<>();
        private final Map<String, List<List<String>>> qry2expectedBindings = new HashMap<>();
        private final List<AssertionFailedError> errors = new ArrayList<>();
        private final SparqlEndpoint endpoint;

        public MockClient() {
            super(String[].class, byte[].class);
            endpoint = SparqlEndpoint.parse("http://"+nextId.getAndIncrement()+".example.org/sparql");
        }

        public MockClient expect(String sparql, List<List<String>> results,
                                 List<List<String>> expectedBindings) {
            this.results.put(sparql, results);
            this.qry2expectedBindings.put(sparql, expectedBindings);
            return this;
        }

        public void assertNoErrors() {
            int size = errors.size();
            if (size == 1) {
                throw errors.get(0);
            } else if (size > 1) {
                AssertionFailedError oldest = errors.get(0);
                String msg = size + " tests failed, oldest error: " + oldest.getMessage();
                throw new AssertionFailedError(msg, oldest);
            }
        }

        @Override public Class<String[]> rowClass()                 { return String[].class; }
        @Override public Class<byte[]>   fragmentClass()            { return byte[].class; }
        @Override public boolean         usesBindingAwareProtocol() { return true; }
        @Override public SparqlEndpoint  endpoint()                 { return endpoint; }
        @Override public String          toString()                 { return endpoint.uri(); }

        @Override
        public Results<String[]> query(CharSequence sparqlCS,
                                       @Nullable SparqlConfiguration configuration,
                                       @Nullable Results<String[]> bindings,
                                       @Nullable BindType bindType) {
            String sparql = sparqlCS.toString();
            if (!results.containsKey(sparql))
                return Results.error(String[].class, error("unexpected query: %s", sparql));

            List<List<String>> expectedBindings = qry2expectedBindings.get(sparql);
            if (bindings == null) {
                if (!expectedBindings.isEmpty())
                    return Results.error(String[].class, error("bindings == null"));
                return new Results<>(SparqlUtils.publicVars(sparql), String[].class,
                                     bindToAny(Flux.fromIterable(results.get(sparql))
                                                   .map(l -> l.toArray(new String[0]))));
            }
            CallbackPublisher<String[]> cbp = new CallbackPublisher<String[]>("mock-cbp") {
                private Thread thread = null;
                private void generateResults() {
                    List<List<String>> acBindings = new ArrayList<>();
                    try (IterableAdapter<String[]> adapter = new IterableAdapter<>(bindings.publisher())) {
                        adapter.forEach(r -> acBindings.add(asList(r)));
                        if (adapter.hasError()) {
                            complete(adapter.error());
                        } else if (expectedBindings.size() != acBindings.size()) {
                            complete(error("acBindings.size()=%d, expected %d",
                                    acBindings.size(), expectedBindings.size()));
                        } else if (!new HashSet<>(expectedBindings).equals(new HashSet<>(acBindings))) {
                            complete(error("Bad bindings: %s\n  Expected: %s",
                                    acBindings, expectedBindings));
                        } else {
                            for (List<String> row : results.get(sparql))
                                feed(row.toArray(new String[0]));
                            complete(null);
                        }
                    }
                }
                @Override protected void onRequest(long n) {
                    if (thread == null) {
                        thread = new Thread(this::generateResults, "mock-cbp-feeder");
                        thread.start();
                    }
                }
                @Override protected void onBackpressure() { }
                @Override protected void onCancel() { }
            };
            List<String> vars;
            if (bindType == null)
                vars = SparqlUtils.publicVars(sparql);
            else
                vars = bindType.resultVars(bindings.vars(), SparqlUtils.publicVars(sparql));
            return new Results<>(vars, String[].class, cbp);
        }

        private AssertionFailedError error(String msg, Object... args) {
            AssertionFailedError error = new AssertionFailedError(format(msg, args));
            errors.add(error);
            return error;
        }
    }

    static Stream<Arguments> test() {
        List<List<String>> joinResults = asList(asList("<http://example.org/S1>", "<http://example.org/O1A>"),
                asList("<http://example.org/S1>", "<http://example.org/O1B>"),
                asList("<http://example.org/S3>", "<http://example.org/O3A>"));
        List<List<String>> leftJoinResults = asList(asList("<http://example.org/S1>", "<http://example.org/O1A>"),
                asList("<http://example.org/S1>", "<http://example.org/O1B>"),
                asList("<http://example.org/S2>", null),
                asList("<http://example.org/S3>", "<http://example.org/O3A>"));
        List<List<String>> existsResults = asList(singletonList("<http://example.org/S1>"),
                singletonList("<http://example.org/S3>"));
        List<List<String>> notExistsResults = singletonList(singletonList("<http://example.org/S2>"));
        List<Arguments> list = new ArrayList<>();
        for (int operands : asList(1, 2, 3, 1024)) {
            for (boolean useMerge : asList(false, true)) {
                list.add(arguments(operands, useMerge, BindType.JOIN,
                        joinResults,
                        asList(singletonList("<http://example.org/S1>"),
                                singletonList("<http://example.org/S2>"),
                                singletonList("<http://example.org/S3>"))));

                list.add(arguments(operands, useMerge, BindType.LEFT_JOIN,
                        leftJoinResults,
                        asList(singletonList("<http://example.org/S1>"),
                                singletonList("<http://example.org/S2>"),
                                singletonList("<http://example.org/S3>"))));

                list.add(arguments(operands, useMerge, BindType.EXISTS,
                        existsResults,
                        asList(singletonList("<http://example.org/S1>"),
                                singletonList("<http://example.org/S2>"),
                                singletonList("<http://example.org/S3>"))));

                list.add(arguments(operands, useMerge, BindType.NOT_EXISTS,
                        notExistsResults,
                        asList(singletonList("<http://example.org/S1>"),
                                singletonList("<http://example.org/S2>"),
                                singletonList("<http://example.org/S3>"))));

                list.add(arguments(operands, useMerge, BindType.MINUS,
                        notExistsResults,
                        asList(singletonList("<http://example.org/S1>"),
                                singletonList("<http://example.org/S2>"),
                                singletonList("<http://example.org/S3>"))));
            }
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void test(int operands, boolean useMerge, BindType bindType,
              List<List<String>> nativeJoinResults,
              List<List<String>> bindings) throws ExecutionException {
        // execute the test on a single thread
        doTest(operands, useMerge, bindType, nativeJoinResults, bindings);
        // to induce non-determinism, aim to use 150% of available threads
        int threads = (int)(Runtime.getRuntime().availableProcessors() * 1.5);
        // only spawn doTest() tasks required to achieve 150% of available threads
        int nTasks = Math.max(2, threads-operands);
        List<AsyncTask<?>> tasks = new ArrayList<>();
        for (int i = 0; i < nTasks; i++)
            tasks.add(async(() -> doTest(operands, useMerge, bindType, nativeJoinResults, bindings)));
        for (AsyncTask<?> task : tasks) task.get();
    }

    void doTest(int operands, boolean useMerge, BindType bindType,
                List<List<String>> nativeJoinResults, List<List<String>> bindings) {
        String leftSparql = "SELECT ?x WHERE { ?x a <http://example.org/L> }";
        MockClient leftClient = new MockClient().expect(leftSparql, bindings, emptyList());
        Plan<String[]> leftPlan = query(leftClient, leftSparql).build();

        String rightSparql = "SELECT ?y WHERE { ?x <http://example.org/p> ?y }";
        List<MockClient> clients = new ArrayList<>();
        for (int i = 0; i < operands; i++) {
            MockClient client = new MockClient();
            client.expect(rightSparql, nativeJoinResults, bindings);
            clients.add(client);
        }
        List<LeafPlan<String[]>> rightOperands = clients.stream().map(c -> query(c, rightSparql).build()).collect(toList());
        Plan<String[]> rightPlan = useMerge ? merge(rightOperands).build()
                                            : union(rightOperands).build();
        FSPublisher<String[]> leftPublisher = bindToAny(Flux.fromIterable(bindings)
                                                      .map(l -> l.toArray(new String[0])));
        Results<String[]> left = new Results<>(singletonList("x"), String[].class, leftPublisher);

        Plan<String[]> joinPlan;
        switch (bindType) {
            case JOIN:
                joinPlan = join(leftPlan, rightPlan).build();
                break;
            case LEFT_JOIN:
                joinPlan = leftJoin(leftPlan, rightPlan).build();
                break;
            case EXISTS:
                joinPlan = exists(leftPlan, false, rightPlan).build();
                break;
            case NOT_EXISTS:
                joinPlan = exists(leftPlan, true, rightPlan).build();
                break;
            case MINUS:
                joinPlan = minus(leftPlan, rightPlan).build();
                break;
            default:
                throw new AssertionFailedError("Unexpected bindType="+bindType);
        }

        NativeJoinPublisher<String[]> njPub = NativeJoinPublisher.tryCreate(joinPlan, left);
        assertNotNull(njPub);
        assertResults(njPub, useMerge, nativeJoinResults, operands);
        for (MockClient client : clients)
            client.assertNoErrors();
        leftClient.assertNoErrors();
    }

    private void assertResults(Publisher<String[]> pub, boolean useMerge,
                               List<List<String>> nativeJoinResults, int rightOperands) {
        List<List<String>> actual = new ArrayList<>();
        try (IterableAdapter<String[]> adapter = new IterableAdapter<>(pub)) {
            for (String[] row : adapter) actual.add(asList(row));
            if (adapter.hasError())
                fail(adapter.error());
        }
        if (useMerge && rightOperands > 1) {
            assertEquals(new HashSet<>(nativeJoinResults), new HashSet<>(actual));
            int mergeWindow = FasterSparqlOpProperties.mergeWindow();
            if (mergeWindow > nativeJoinResults.size()) {
                assertEquals(nativeJoinResults.size(), actual.size(),
                             "Did not deduplicate across right-side operands");
            }
        } else {
            assertEquals(nativeJoinResults.size() * rightOperands, actual.size());
            Map<List<String>, Integer> expectedCount = new HashMap<>();
            for (List<String> row : nativeJoinResults)
                expectedCount.put(row, expectedCount.computeIfAbsent(row, k -> 0) + rightOperands);
            for (Map.Entry<List<String>, Integer> e : expectedCount.entrySet()) {
                long count = actual.stream().filter(e.getKey()::equals).count();
                String msg = format("Expected %d instances, found %d for row %s",
                        e.getValue(), count, e.getKey());
                assertEquals((long) e.getValue(), count, msg);
            }
        }
    }
}