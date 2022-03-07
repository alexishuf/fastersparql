package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.parser.row.StringListRowParser.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

class StringListRowParserTest {
    private static final List<List<String>> STRING_LISTS = asList(
            asList("\"1\"", "\"2\""),
            asList("<a>", "\"\uD83E\uDE02\"")
    );
    private static final List<String[]> STRING_ARRAYS =
            STRING_LISTS.stream().map(l -> l.toArray(new String[0])).collect(toList());
    private static final List<byte[][]> BYTES_ARRAYS =
            STRING_LISTS.stream().map(l -> {
                byte[][] array = new byte[l.size()][];
                for (int i = 0; i < array.length; i++)
                    array[i] = l.get(i).getBytes(StandardCharsets.UTF_8);
                return array;
            }).collect(toList());
    private static final List<List<byte[]>> BYTES_LISTS = STRING_LISTS.stream().map(l -> {
        List<byte[]> bytesList = new ArrayList<>();
        for (String s : l) bytesList.add(s.getBytes(StandardCharsets.UTF_8));
        return bytesList;
    }).collect(toList());

    private static final List<String> vasrList = asList("x", "y");

    private void checkResults(Publisher<List<String>> publisher) {
        checkResults(publisher, false);
    }

    private void checkResults(Publisher<List<String>> publisher, boolean expectSame) {
        List<List<String>> actual = Flux.from(publisher).collectList().block();
        assertNotNull(actual);
        assertEquals(actual, STRING_LISTS);
        if (expectSame) {
            for (int i = 0; i < actual.size(); i++)
                assertSame(STRING_LISTS.get(i), actual.get(i), "i="+i);
        }
    }

    @Test
    void testParseStringArray() {
        FSPublisher<String[]> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(STRING_ARRAYS));
        Results<String[]> results = new Results<>(vasrList, String[].class, inputFlux);
        checkResults(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseStringList() {
        FSPublisher<List<String>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(STRING_LISTS));
        Results<List<String>> results = new Results<>(vasrList, List.class, inputFlux);
        checkResults(INSTANCE.parseStringsList(results), true);
    }

    @Test
    void testParseBytesArray() {
        FSPublisher<byte[][]> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(BYTES_ARRAYS));
        Results<byte[][]> results = new Results<>(vasrList, byte[][].class, inputFlux);
        checkResults(INSTANCE.parseBytesArray(results));
    }

    @Test
    void testParseBytesList() {
        FSPublisher<List<byte[]>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(BYTES_LISTS));
        Results<List<byte[]>> results = new Results<>(vasrList, List.class, inputFlux);
        checkResults(INSTANCE.parseBytesList(results));
    }
}