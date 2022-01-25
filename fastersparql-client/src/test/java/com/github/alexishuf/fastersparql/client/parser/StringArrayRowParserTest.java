package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.parser.row.StringArrayRowParser.INSTANCE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

class StringArrayRowParserTest {
    private static final List<String[]> STRING_ARRAYS = asList(
            new String[]{"\"1\"", "\"2\""},
            new String[]{"<a>", "\"\uD83E\uDE02\""}
    );
    private static final List<List<String>> STRING_LISTS =
            STRING_ARRAYS.stream().map(Arrays::asList).collect(toList());

    private SafeCompletableAsyncTask<List<String>> varsFuture;

    @BeforeEach
    void setUp() {
        varsFuture = new SafeCompletableAsyncTask<>();
    }

    private void checkResults(Publisher<String[]> publisher, boolean expectSame) {
        varsFuture.complete(asList("x", "y"));
        List<String[]> actual = Flux.from(publisher).collectList().block();

        assertNotNull(actual);
        for (int i = 0; i < actual.size(); i++)
            assertArrayEquals(STRING_ARRAYS.get(i), actual.get(i));
        if (expectSame) {
            for (int i = 0; i < actual.size(); i++)
                assertSame(STRING_ARRAYS.get(i), actual.get(i));
        }
    }

    @Test
    void testParseStringArray() {
        Flux<String[]> inputFlux = Flux.fromIterable(STRING_ARRAYS);
        Results<String[]> results = new Results<>(varsFuture, String[].class, inputFlux);
        checkResults(INSTANCE.parseStringsArray(results), true);
    }

    @Test
    void testParseStringList() {
        Flux<List<String>> inputFlux = Flux.fromIterable(STRING_LISTS);
        Results<List<String>> results = new Results<>(varsFuture, List.class, inputFlux);
        checkResults(INSTANCE.parseStringsList(results), false);
    }

    @Test
    void testParseBytesArray() {
        List<byte[][]> input = STRING_LISTS.stream().map(l -> {
            byte[][] row = new byte[l.size()][];
            for (int i = 0; i < l.size(); i++) row[i] = l.get(i).getBytes(UTF_8);
            return row;
        }).collect(toList());
        Flux<byte[][]> inputFlux = Flux.fromIterable(input);
        Results<byte[][]> results = new Results<>(varsFuture, byte[][].class, inputFlux);
        checkResults(INSTANCE.parseBytesArray(results), false);
    }

    @Test
    void testParseBytesList() {
        List<List<byte[]>> input = STRING_LISTS.stream()
                .map(l -> l.stream().map(s -> s.getBytes(UTF_8)).collect(toList()))
                .collect(toList());
        Flux<List<byte[]>> inputFlux = Flux.fromIterable(input);
        Results<List<byte[]>> results = new Results<>(varsFuture, List.class, inputFlux);
        checkResults(INSTANCE.parseBytesList(results), false);
    }
}