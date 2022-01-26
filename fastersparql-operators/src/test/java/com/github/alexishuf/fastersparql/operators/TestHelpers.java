package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.SafeAsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import com.github.alexishuf.fastersparql.client.util.reactive.IterablePublisher;
import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHelpers {

    public static Plan<List<String>> asPlan(Collection<List<String>> collection) {
        return new Plan<List<String>>() {
            @Override public Results<List<String>> execute() {
                return asResults(collection);
            }

            @Override public Plan<List<String>> bind(Map<String, String> var2ntValue) {
                return this;
            }
        };
    }

    public static Results<List<String>> asResults(Collection<List<String>> collection) {
        int width = collection.isEmpty() ? 0 : collection.iterator().next().size();
        List<String> vars = IntStream.range(0, width).mapToObj(i -> "x" + i).collect(toList());
        SafeAsyncTask<List<String>> varsTask = Async.wrap(vars);
        return new Results<>(varsTask, List.class, new IterablePublisher<>(collection));
    }

    private static LinkedHashMap<List<String>, Integer> countMap(Iterable<List<String>> lists) {
        LinkedHashMap<List<String>, Integer> map = new LinkedHashMap<>();
        for (List<String> list : lists)
            map.put(list, map.getOrDefault(list, 0)+1);
        return map;
    }

    public static void assertExpectedRows(Collection<List<String>> expected,
                                          Class<? extends Throwable> expectedError,
                                          Results<List<String>> actual,
                                          boolean checkOrder) {
        IterableAdapter<List<String>> adapter = new IterableAdapter<>(actual.publisher());
        LinkedHashMap<List<String>, Integer> exMap = countMap(expected);
        LinkedHashMap<List<String>, Integer> acMap = countMap(adapter);
        if (adapter.hasError()) {
            if (expectedError != null)
                assertEquals(expectedError, requireNonNull(adapter.error()).getClass());
            else
                fail("Unexpected error from actual.publisher()", adapter.error());
        } else if (expectedError != null) {
            fail("Expected "+expectedError+" to be thrown by actual.publisher()");
        }
        checkMissing(exMap, acMap);
        checkUnexpected(exMap, acMap);
        checkCounts(exMap, acMap);
        if (checkOrder)
            assertEquals(new ArrayList<>(exMap.keySet()), new ArrayList<>(acMap.keySet()));
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
