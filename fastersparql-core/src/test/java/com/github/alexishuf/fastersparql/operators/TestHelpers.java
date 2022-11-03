package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHelpers {

    public static Plan<List<String>, String> asPlan(Vars vars, Collection<List<String>> collection) {
        return FSOps.values(ListRow.STRING, vars, collection);
    }

    public static Plan<List<String>, String> asPlan(Collection<List<String>> collection) {
        return asPlan(generateVars(collection), collection);
    }

    public static Vars generateVars(Iterable<? extends List<String>> rows) {
        var it = rows.iterator();
        return generateVars(it.hasNext() ? it.next().size() : 0);
    }
    public static Vars generateVars(int width) {
        return Vars.from(IntStream.range(0, width).mapToObj(i -> "x" + i).toList());
    }

    private record IterationResult(LinkedHashMap<List<String>, Integer> row2count,
                                   List<String> vars,
                                   @Nullable Throwable error) {
        private static void count(LinkedHashMap<List<String>, Integer> map,
                                  Iterator<? extends List<String>> it) {
            while (it.hasNext()) {
                List<String> row = it.next();
                map.put(row, map.getOrDefault(row, 0)+1);
            }
        }

        public static IterationResult from(BIt<? extends List<String>> it) {
            Throwable error = null;
            LinkedHashMap<List<String>, Integer> map = new LinkedHashMap<>();
            try { count(map, it); } catch (Throwable t) { error = t; }
            return new IterationResult(map, it.vars(), error);
        }

        public void assertExpected(Collection<? extends List<String>> expected,
                                   boolean ordered,
                                   List<String> expectedVars,
                                   Class<? extends Throwable> expectedError) {
            if (error != null) {
                if (expectedError != null)
                    assertEquals(expectedError, requireNonNull(error).getClass());
                else
                    fail("Unexpected error: "+error, error);
            } else if (expectedError != null) {
                fail("Expected "+expectedError+" to be thrown");
            }
            if (expectedVars != null)
                assertEquals(expectedVars, vars, "vars mismatch");
            var expectedMap = new LinkedHashMap<List<String>, Integer>();
            count(expectedMap, expected.iterator());
            checkMissing(expectedMap, row2count);
            checkUnexpected(expectedMap, row2count);
            checkCounts(expectedMap, row2count);
            if (ordered) {
                var expectedOrder = new ArrayList<>(expectedMap.keySet());
                var actualOrder = new ArrayList<>(row2count.keySet());
                assertEquals(expectedOrder, actualOrder);
            }
        }
    }

    public static void checkRows(Collection<? extends List<String>> expected,
                                 List<String> expectedVars,
                                 Class<? extends Throwable> expectedError,
                                 Plan<? extends List<String>, ?> plan,
                                 boolean checkOrder) {
        checkRows(expected, expectedVars, expectedError, plan.execute(false), checkOrder);
    }

    public static void checkRows(Collection<? extends List<String>> expected,
                                 List<String> expectedVars,
                                 Class<? extends Throwable> expectedError,
                                 BIt<? extends List<String>> actual,
                                 boolean checkOrder) {
        IterationResult r = IterationResult.from(actual);
        r.assertExpected(expected, checkOrder, expectedVars, expectedError);
    }


    private static void checkCounts(Map<List<String>, Integer> exMap,
                                    Map<List<String>, Integer> acMap) {
        for (Map.Entry<List<String>, Integer> e : exMap.entrySet()) {
            int actual = acMap.getOrDefault(e.getKey(), 0);
            assertEquals(e.getValue(), actual, "Count mismatch for row "+e.getKey());
        }
    }

    private static void checkMissing(Map<List<String>, Integer> exMap,
                                     Map<List<String>, Integer> acMap) {
        List<List<String>> missing = new ArrayList<>();
        for (List<String> row : exMap.keySet()) {
            if (!acMap.containsKey(row)) missing.add(row);
        }
        reportFailure("missing", missing);
    }

    private static void checkUnexpected(Map<List<String>, Integer> exMap,
                                        Map<List<String>, Integer> acMap) {
        List<List<String>> unexpected = new ArrayList<>();
        for (List<String> row : acMap.keySet()) {
            if (!exMap.containsKey(row)) unexpected.add(row);
        }
        reportFailure("unexpected", unexpected);
    }

    private static void reportFailure(String offense, List<List<String>> offending) {
        if (!offending.isEmpty()) {
            StringBuilder b = new StringBuilder();
            b.append(offending.size()).append(' ').append(offense).append(" rows:\n");
            for (List<String> row : offending)
                b.append("    ").append(row).append('\n');
            fail(b.toString());
        }
    }
}
