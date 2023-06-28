package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.ResultsSparqlClient;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.operators.bit.NativeBind;
import com.github.alexishuf.fastersparql.operators.bit.PlanBindingBIt;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.model.BindType.*;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.STRONG;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.WEAK;
import static com.github.alexishuf.fastersparql.util.Results.DuplicatesPolicy.ALLOW_DEDUP;
import static com.github.alexishuf.fastersparql.util.Results.DuplicatesPolicy.EXACT;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

class NativeBindTest {
    private static final AtomicInteger nextClientId = new AtomicInteger(1);

    private static ResultsSparqlClient client(SparqlQuery query, Results expected) {
        String uri = "http://" + nextClientId.getAndIncrement() + ".example.org/sparql";
        //noinspection resource
        return new ResultsSparqlClient(true, uri).answerWith(query, expected);
    }

    record D(int operands, boolean dedup, boolean crossDedup,
             Results results) implements Runnable {
        @Override public void run() {
            var leftSparql = new OpaqueSparqlQuery("SELECT ?x WHERE { ?x a <http://example.org/L> }");
            if (dedup)
                leftSparql = leftSparql.toDistinct(STRONG);
            try (var leftClient = client(leftSparql, results.bindingsAsResults());
                 var clients = new AutoCloseableSet<ResultsSparqlClient>()) {
                var left = FS.query(leftClient, leftSparql);

                var rightSparqlTmp = new OpaqueSparqlQuery("SELECT ?y WHERE { ?x <http://example.org/p> ?y }");
                var rightSparql = dedup ? rightSparqlTmp.toDistinct(WEAK) : rightSparqlTmp;
                for (int i = 0; i < operands; i++)
                    clients.add(client(rightSparql, results));
                var rightOperands = clients.stream().map(c -> FS.query(c, rightSparql)).toArray(Plan[]::new);
                var right = crossDedup ? FS.crossDedupUnion(rightOperands) : FS.union(rightOperands);

                var join = switch (results.bindType()) {
                    case JOIN -> FS.join(left, right);
                    case LEFT_JOIN -> FS.leftJoin(left, right);
                    case EXISTS -> FS.exists(left, false, right);
                    case NOT_EXISTS -> FS.exists(left, true, right);
                    case MINUS -> FS.minus(left, right);
                    case null -> throw new IllegalArgumentException("missing bindType");
                };

                var finalRows = new ArrayList<>(results.expected());
                for (int i = 0; i < operands - 1; i++)
                    finalRows.addAll(results.expected());
                var finalResults = Results.results(results.vars(), finalRows)
                                          .duplicates(dedup || crossDedup ? ALLOW_DEDUP : EXACT);

                try (var it = NativeBind.preferNative(Batch.TERM, join, null, false)) {
                    assertFalse(it instanceof PlanBindingBIt, "not using native joins");
                    finalResults.check(it);
                }

                for (var client : clients)
                    client.assertNoErrors();
                leftClient.assertNoErrors();
            }
        }
    }

    static List<D> data() {
        Object[] bindingsSpec = {"?x", ":S1", ":S2", ":S3"};
        var join = results("?x",  "?y",
                           ":S1", ":O1A",
                           ":S1", ":O1B",
                           ":S3", ":O3A").bindings(bindingsSpec);
        var leftJoin = results("?x",  "?y",
                               ":S1", ":O1A",
                               ":S1", ":O1B",
                               ":S2", null,
                               ":S3", ":O3A").bindType(LEFT_JOIN).bindings(bindingsSpec);
        var exists = results("?x", ":S1", ":S3").bindType(EXISTS).bindings(bindingsSpec);
        var notExists = results("?x", ":S2").bindType(NOT_EXISTS).bindings(bindingsSpec);
        List<D> list = new ArrayList<>();
        for (int operands : List.of(1, 2, 3, 96)) {
            for (boolean dedup : List.of(false, true)) {
                for (Boolean crossDedup : List.of(false, true)) {
                    list.add(new D(operands, dedup, crossDedup, join));
                    list.add(new D(operands, dedup, crossDedup, leftJoin));
                    list.add(new D(operands, dedup, crossDedup, exists));
                    list.add(new D(operands, dedup, crossDedup, notExists));
                    list.add(new D(operands, dedup, crossDedup, notExists.bindType(MINUS)));
                }
            }
        }
        return list;
    }

    @Test
    void test() throws Exception {
        List<D> data = data();
        for (int i = 0, n = data.size(); i < n; i++) {
            D d = data.get(i);
            try {
                d.run();
            } catch (AssertionError e) {
                throw new AssertionFailedError(e.getMessage()+" at data["+i+"]", e.getCause());
            } catch (Throwable t) {
                fail("Unexpected exception at data["+i+"]", t);
            }
        }
        int threads = Runtime.getRuntime().availableProcessors();
        try (var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName())) {
            for (int i = 0; i < data.size(); i++) {
                tasks.repeat(threads, data.get(i));
                try {
                    tasks.await();
                } catch (Throwable t) {
                    fail("data["+i+"] failed with "+threads+" threads", t);
                }
            }
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