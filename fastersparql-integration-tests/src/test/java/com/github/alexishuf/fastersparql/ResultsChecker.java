package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.util.Skip;
import org.opentest4j.AssertionFailedError;

import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

public class ResultsChecker {
    protected static final String PREFIX;
    protected static final Map<String, String> PREDEFINED;

    protected List<String> vars;
    protected List<List<String>> expected;
    protected boolean checkOrder = false;
    protected Class<? extends Throwable> expectedError;

    public ResultsChecker(ResultsChecker other) {
        this.vars = other.vars;
        this.expected = other.expected.stream().map(ArrayList::new).collect(toList());
        this.checkOrder = other.checkOrder;
        this.expectedError = other.expectedError;
    }

    public ResultsChecker(boolean value) {
        this.vars = Collections.emptyList();
        this.expected = value ? singletonList(emptyList()) : emptyList();
    }

    public ResultsChecker(List<String> vars, String... values) {
        this.vars = vars;
        this.expected = new ArrayList<>();
        int columns = vars.size();
        if (columns == 0) {
            if (values.length > 1) {
                throw new IllegalArgumentException("Too many values given for zero vars");
            } else if (values.length == 1) {
                if (!values[0].trim().isEmpty())
                    throw new IllegalArgumentException("Non-empty string for zero vars");
                this.expected.add(Collections.emptyList());
            }
        } else {
            List<String> row = new ArrayList<>(columns);
            for (String value : values) {
                row.add(expandVars(value));
                if (row.size() == columns) {
                    this.expected.add(row);
                    row = new ArrayList<>(columns);
                }
            }
            if (!row.isEmpty())
                throw new IllegalArgumentException("column / terms mismatch");
        }
    }

    public static ResultsChecker results(boolean value) {
        return new ResultsChecker(value);
    }
    public static ResultsChecker results(List<String> vars, String... values) {
        return new ResultsChecker(vars, values);
    }

    public List<String>               vars()          { return vars; }
    public List<List<String>>         expected()      { return expected; }

    public void assertExpected(BIt<?> results) {
        List<Object> actual = new ArrayList<>();
        Throwable error = null;
        try {
            while (results.hasNext()) actual.add(results.next());
        } catch (Throwable t) { error = t; }
        if (error != null) {
            if (expectedError != null)
                assertTrue(expectedError.isAssignableFrom(error.getClass()));
            else
                fail(error);
        }
        if (checkOrder) {
            assertEquals(expected, actual);
        } else {
            compareRows(actual);
        }
        assertEquals(vars(), results.vars());
    }

    /* --- --- --- helper methods --- --- --- */

    static {
        Map<String, String> predefined = new HashMap<>();
        predefined.put(":",     "http://example.org/");
        predefined.put("xsd:",  "http://www.w3.org/2001/XMLSchema#");
        predefined.put("rdf:",  "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        predefined.put("rdfs:", "http://www.w3.org/2000/01/rdf-schema#");
        predefined.put("owl:",  "http://www.w3.org/2002/07/owl#");
        predefined.put("foaf:", "http://xmlns.com/foaf/0.1/");
        PREDEFINED = predefined;
        StringBuilder b = new StringBuilder();
        for (Map.Entry<String, String> e : PREDEFINED.entrySet()) {
            if (!e.getKey().endsWith(":")) continue;
            b.append("PREFIX ").append(e.getKey()).append(" <").append(e.getValue()).append(">\n");
        }
        PREFIX = b.toString();
    }

    private static String makeStringImplicit(String str) {
        if (str == null || str.isEmpty() || str.charAt(0) != '"')
            return str;
        final String suffix = "^^<http://www.w3.org/2001/XMLSchema#string>";
        return str.endsWith(suffix) ? str.substring(0, str.length()-suffix.length()) : str;
    }

    private static List<String> asCanonRow(Object row) {
        Collection<?> input;
        switch (row) {
            case null:
                return null;
            case Collection<?> collection:
                input = collection;
                break;
            case Object[] objects:
                input = Arrays.asList(objects);
                break;
            default:
                throw new AssertionFailedError("Unexpected " + row.getClass());
        }
        ArrayList<String> list = new ArrayList<>(input.size());
        for (Object o : input)
            list.add(ResultsChecker.makeStringImplicit(TestUtils.decodeOrToString(o)));
        return list;
    }

    private static Map<List<String>, Integer> countMap(Collection<?> collection) {
        Map<List<String>, Integer> map = new HashMap<>();
        for (Object o : collection) {
            List<String> row = asCanonRow(o);
            map.put(row, map.getOrDefault(row, 0)+1);
        }
        return map;
    }

    private void compareRows(List<Object> actual) {
        Map<List<String>, Integer> expectedCounts = ResultsChecker.countMap(expected);
        Map<List<String>, Integer> actualCounts = ResultsChecker.countMap(actual);
        List<String> expectedSorted = expectedCounts.keySet().stream().map(Object::toString)
                .sorted().collect(toList());
        List<String> actualSorted = actualCounts.keySet().stream().map(Object::toString)
                .sorted().collect(toList());
        assertEquals(expectedSorted, actualSorted);
        for (var e : expectedCounts.entrySet()) {
            int count = actualCounts.getOrDefault(e.getKey(), 0);
            assertEquals(e.getValue(), count,
                    "expected "+e.getValue()+" "+e.getKey()+", got "+count);
        }
        for (var e : actualCounts.entrySet()) {
            if (expectedCounts.containsKey(e.getKey())) continue;
            fail(e.getValue()+" unexpected occurrences of "+e.getKey());
        }
    }

    protected static String expandVars(String string) {
        if (string == null || string.equals("$null"))
            return null;
        int len = string.length();
        StringBuilder b = new StringBuilder(len + 128);
        for (int i = 0, j; i < len; i = j+1) {
            j = Skip.skipUntil(string, i, string.length(), '$');
            boolean replaced = false;
            for (Map.Entry<String, String> e : PREDEFINED.entrySet()) {
                replaced = string.regionMatches(j+1, e.getKey(), 0, e.getKey().length());
                if (replaced) {
                    b.append(string, i, j).append(e.getValue());
                    j += e.getKey().length();
                    break;
                }
            }
            if (!replaced) b.append(string, i, Math.min(j+1, len));
        }
        return b.toString();
    }
}
