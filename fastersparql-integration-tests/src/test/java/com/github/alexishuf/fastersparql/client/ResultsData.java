package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.CSUtils;
import com.github.alexishuf.fastersparql.client.util.IterableAdapter;
import lombok.Data;
import lombok.experimental.Accessors;
import org.opentest4j.AssertionFailedError;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

@Data
@Accessors(fluent = true, chain = true)
class ResultsData {
    private static final String PREFIX;
    private static final Map<String, String> PREDEFINED;

    private String sparql;
    private List<List<String>> expected;
    private boolean checkOrder = false;
    private Class<? extends Throwable> expectedError;
    private SparqlConfiguration config = SparqlConfiguration.EMPTY;

    public ResultsData(ResultsData other) {
        this.sparql = other.sparql;
        this.expected = other.expected.stream().map(ArrayList::new).collect(toList());
        this.checkOrder = other.checkOrder;
        this.expectedError = other.expectedError;
        this.config = other.config;
    }

    public static ResultsData results(String sparql, boolean value) {
        return new ResultsData(sparql, value);
    }
    public static ResultsData results(String sparql, String... values) {
        return new ResultsData(sparql, values);
    }

    public ResultsData(String sparql, boolean value) {
        this.sparql = PREFIX+sparql;
        assertTrue(Pattern.compile("ASK +\\{").matcher(sparql).find());
        expected = value ? singletonList(emptyList()) : emptyList();
    }
    public ResultsData(String sparql, String... values) {
        this.sparql = PREFIX+sparql;
        int columns = vars().size();
        expected = new ArrayList<>();
        List<String> row = new ArrayList<>(columns);
        for (String value : values) {
            row.add(expandVars(value));
            if (row.size() == columns) {
                expected.add(row);
                row = new ArrayList<>(columns);
            }
        }
    }

    public void assertExpected(Results<?> results) {
        List<Object> actual = new ArrayList<>();
        IterableAdapter<?> adapter = new IterableAdapter<>(results.publisher());
        adapter.forEach(r -> actual.add(asCanonRow(r)));
        if (adapter.hasError()) {
            if (expectedError != null) {
                Class<? extends Throwable> errorClass = requireNonNull(adapter.error()).getClass();
                assertTrue(expectedError.isAssignableFrom(errorClass));
            } else {
                fail(adapter.error());
            }
        }
        if (checkOrder) {
            assertEquals(expected, actual);
        } else {
            compareRows(actual);
        }
        assertEquals(vars(), results.vars().get());
    }

    public List<String> vars() {
        if (Pattern.compile("ASK +\\{").matcher(sparql).find())
            return emptyList();
        Matcher m = Pattern.compile("SELECT +((?:\\?\\w+ )*) *WHERE *\\{").matcher(sparql);
        assertTrue(m.find());
        return Arrays.stream(m.group(1).split(" +"))
                .map(s -> s.replaceAll("^\\?", "")).collect(toList());
    }

    public ResultsData with(Consumer<SparqlConfiguration.SparqlConfigurationBuilder> configurator) {
        SparqlConfiguration.SparqlConfigurationBuilder builder = config.toBuilder();
        configurator.accept(builder);
        return new ResultsData(this).config(builder.build());
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
        for (Map.Entry<String, String> e : predefined.entrySet()) {
            if (!e.getKey().endsWith(":")) continue;
            b.append("PREFIX ").append(e.getKey()).append(" <").append(e.getValue()).append(">\n");
        }
        PREFIX = b.toString();
    }

    private String expandVars(String string) {
        if (string.equals("$null"))
            return null;
        int len = string.length();
        StringBuilder b = new StringBuilder(len + 128);
        for (int i = 0, j; i < len; i = j+1) {
            j = CSUtils.skipUntil(string, i, '$');
            boolean replaced = false;
            for (Map.Entry<String, String> e : PREDEFINED.entrySet()) {
                replaced = CSUtils.startsWith(string, j+1, len, e.getKey());
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

    private void compareRows(List<Object> actual) {
        Map<?, Integer> expectedCounts = countMap(expected);
        Map<?, Integer> actualCounts = countMap(actual);
        List<String> expectedSorted = expectedCounts.keySet().stream().map(Object::toString)
                .sorted().collect(toList());
        List<String> actualSorted = actualCounts.keySet().stream().map(Object::toString)
                .sorted().collect(toList());
        assertEquals(expectedSorted, actualSorted);
        for (Map.Entry<?, Integer> e : expectedCounts.entrySet()) {
            Integer count = actualCounts.getOrDefault(e.getKey(), 0);
            assertEquals(e.getValue(), count,
                    "expected "+e.getValue()+" "+e.getKey()+", got "+count);
        }
        for (Map.Entry<?, Integer> e : actualCounts.entrySet()) {
            if (expectedCounts.containsKey(e.getKey())) continue;
            fail(e.getValue()+" unexpected occurrences of "+e.getKey());
        }
    }

    private static String makeStringImplicit(String str) {
        if (str == null || str.isEmpty() || str.charAt(0) != '"')
            return str;
        final String suffix = "^^<http://www.w3.org/2001/XMLSchema#string>";
        return str.endsWith(suffix) ? str.substring(0, str.length()-suffix.length()) : str;
    }

    private static List<String> asCanonRow(Object row) {
        Collection<?> input;
        if (row == null)
            return null;
        else if (row instanceof Collection)
            input = (Collection<?>) row;
        else if (row instanceof Object[])
            input = Arrays.asList((Object[]) row);
        else
            throw new AssertionFailedError("Unexpected "+row.getClass());
        ArrayList<String> list = new ArrayList<>(input.size());
        for (Object o : input)
            list.add(makeStringImplicit(TestUtils.decodeOrToString(o)));
        return list;
    }

    private static <T> Map<T, Integer> countMap(Collection<T> collection) {
        Map<T, Integer> map = new HashMap<>();
        for (T o : collection)
            map.put(o, map.getOrDefault(o, 0)+1);
        return map;
    }
}
