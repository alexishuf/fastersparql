package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.DummySparqlClient;
import com.github.alexishuf.fastersparql.operators.bind.NativeBind;
import com.github.alexishuf.fastersparql.operators.bind.PlanBindingBIt;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.operators.FSOps.*;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.STRONG;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.WEAK;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.*;

class NativeBindTest {
    private static class MockClient extends DummySparqlClient<String[], String, byte[]> {
        private static final AtomicInteger nextId = new AtomicInteger(1);
        private final Map<SparqlQuery, List<List<String>>> results = new HashMap<>();
        private final Map<SparqlQuery, List<List<String>>> qry2expectedBindings = new HashMap<>();
        private final List<AssertionFailedError> errors = new ArrayList<>();
        private final SparqlEndpoint endpoint;

        public MockClient() {
            super(ArrayRow.STRING, byte[].class);
            endpoint = SparqlEndpoint.parse("http://"+nextId.getAndIncrement()+".example.org/sparql");
        }

        public MockClient expect(SparqlQuery sparql, List<List<String>> results,
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
        public BIt<String[]> query(SparqlQuery sq,
                                   @Nullable BIt<String[]> bindings,
                                   @Nullable BindType bindType) {
            List<List<String>> results = this.results.get(sq);
            if (results == null)
                throw error("unexpected query: %s", sq);
            List<List<String>> exBindings = qry2expectedBindings.get(sq);
            if (bindings == null) {
                if (!exBindings.isEmpty())
                    throw error("bindings == null");
                var it = results.stream().map(l -> l.toArray(new String[0])).iterator();
                return new IteratorBIt<>(it, String[].class, sq.publicVars());
            }
            // we must consume bindings from another thread as doing so durectly from query()
            // would cause a deadlock: (NativeBind's scatter thread is blocked because
            // second queue is not being consumed and consumption of that queue will not start
            // because the scatter thread will not finish feeding the first queue.
            //
            // While this could be fixed in NativeBind, query() implementations really
            // should'nt consume bindings fully before returning as that will only work on trivial
            // scenarios.
            var vars = requireNonNull(bindType).resultVars(bindings.vars(), sq.publicVars());
            var cb = new CallbackBIt<>(String[].class, vars);
            Thread.startVirtualThread(() -> {
                var acBindings = bindings.stream().map(Arrays::asList).toList();
                if (acBindings.size() != exBindings.size()) {
                    cb.complete(error("Expected %d bindings, got %d",
                                      exBindings.size(), acBindings.size()));
                } else if (!new HashSet<>(acBindings).equals(new HashSet<>(exBindings))) {
                    cb.complete(error("Bad bindings: %s\nExpected: %s",
                                      acBindings, exBindings));
                }
                results.stream().map(l -> l.toArray(new String[0])).forEach(cb::feed);
                cb.complete(null);
            });
            return cb;
        }

        private AssertionFailedError error(String msg, Object... args) {
            AssertionFailedError error = new AssertionFailedError(format(msg, args));
            errors.add(error);
            return error;
        }
    }

    record D(int operands, boolean dedup, boolean crossDedup, BindType bindType,
             List<List<String>> results,
             List<List<String>> bindings) implements Runnable {
        @Override public void run() {
            var leftSparql = new OpaqueSparqlQuery("SELECT ?x WHERE { ?x a <http://example.org/L> }");
            if (dedup)
                leftSparql = leftSparql.toDistinct(STRONG);
            var leftClient = new MockClient().expect(leftSparql, bindings, List.of());
            var left = query(leftClient, leftSparql);

            var rightSparqlTmp = new OpaqueSparqlQuery("SELECT ?y WHERE { ?x <http://example.org/p> ?y }");
            var rightSparql = dedup ? rightSparqlTmp.toDistinct(WEAK) : rightSparqlTmp;
            var clients = new ArrayList<MockClient>();
            for (int i = 0; i < operands; i++) //noinspection resource
                clients.add(new MockClient().expect(rightSparql, results, bindings));
            var rightOperands = clients.stream().map(c -> query(c, rightSparql)).toList();
            var right = crossDedup ? crossDedupUnion(rightOperands) : union(rightOperands);

            var join = switch (bindType) {
                case JOIN -> join(left, right);
                case LEFT_JOIN -> leftJoin(left, right);
                case EXISTS -> exists(left, false, right);
                case NOT_EXISTS -> exists(left, true, right);
                case MINUS -> minus(left, right);
            };

            try (BIt<String[]> it = NativeBind.preferNative(join, dedup)) {
                List<List<String>> list = it.stream().map(Arrays::asList).toList();
                Set<List<String>> set = new HashSet<>(list), expected = new HashSet<>(results);
                assertFalse(it instanceof PlanBindingBIt, "not using native joins");
                assertEquals(set, expected);
                int maxRows = results.size() * operands;
                if ((crossDedup || dedup) && operands > 1)
                    assertTrue(list.size() <= maxRows, "bogus rows introduced");
                else
                    assertEquals(maxRows, list.size());
            }

            for (MockClient client : clients)
                client.assertNoErrors();
            leftClient.assertNoErrors();
        }
    }

    static List<D> data() {
        List<List<String>> joinResults = asList(
                asList("<http://example.org/S1>", "<http://example.org/O1A>"),
                asList("<http://example.org/S1>", "<http://example.org/O1B>"),
                asList("<http://example.org/S3>", "<http://example.org/O3A>"));
        List<List<String>> leftJoinResults = asList(asList("<http://example.org/S1>", "<http://example.org/O1A>"),
                asList("<http://example.org/S1>", "<http://example.org/O1B>"),
                asList("<http://example.org/S2>", null),
                asList("<http://example.org/S3>", "<http://example.org/O3A>"));
        List<List<String>> existsResults = asList(singletonList("<http://example.org/S1>"),
                singletonList("<http://example.org/S3>"));
        List<List<String>> notExistsResults = singletonList(singletonList("<http://example.org/S2>"));
        List<D> list = new ArrayList<>();
        for (int operands : List.of(1, 2, 3, 96)) {
            for (boolean dedup : List.of(false, true)) {
                for (Boolean crossDedup : List.of(false, true)) {
                    list.add(new D(operands, dedup, crossDedup, BindType.JOIN,
                            joinResults,
                            List.of(singletonList("<http://example.org/S1>"),
                                    singletonList("<http://example.org/S2>"),
                                    singletonList("<http://example.org/S3>"))));

                    list.add(new D(operands, dedup, crossDedup, BindType.LEFT_JOIN,
                            leftJoinResults,
                            List.of(singletonList("<http://example.org/S1>"),
                                    singletonList("<http://example.org/S2>"),
                                    singletonList("<http://example.org/S3>"))));

                    list.add(new D(operands, dedup, crossDedup, BindType.EXISTS,
                            existsResults,
                            List.of(singletonList("<http://example.org/S1>"),
                                    singletonList("<http://example.org/S2>"),
                                    singletonList("<http://example.org/S3>"))));

                    list.add(new D(operands, dedup, crossDedup, BindType.NOT_EXISTS,
                            notExistsResults,
                            List.of(singletonList("<http://example.org/S1>"),
                                    singletonList("<http://example.org/S2>"),
                                    singletonList("<http://example.org/S3>"))));

                    list.add(new D(operands, dedup, crossDedup, BindType.MINUS,
                            notExistsResults,
                            List.of(singletonList("<http://example.org/S1>"),
                                    singletonList("<http://example.org/S2>"),
                                    singletonList("<http://example.org/S3>"))));
                }
            }
        }
        return list;
    }

    @Test
    void test() throws Exception {
        List<D> data = data();
        for (D d : data)
            d.run();
        int threads = Math.max(2, (int)(Runtime.getRuntime().availableProcessors() * 1.5));
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            data.forEach(d -> tasks.repeat(threads, d));
        }
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public static void main(String[] args) throws IOException {
        List<D> cases = data();
        cases.forEach(D::run);
        System.out.println("Press [ENTER] to start...");
        System.in.read();
        double secs = 10;
        long end = System.nanoTime() + (long)(secs*1_000_000_000L);
        int ops = 0;
        while (System.nanoTime() < end) {
            cases.forEach(D::run);
            System.out.print('.');
            if (ops % 80 == 0)
                System.out.println();
            ++ops;
        }
        System.out.printf("%.3f ops/s\nPress [ENTER] to exit...\n", ops/secs);
        System.in.read();
    }
}