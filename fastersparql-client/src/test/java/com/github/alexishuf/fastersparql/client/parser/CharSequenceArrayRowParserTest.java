package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.parser.row.CharSequenceArrayRowParser.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

class CharSequenceArrayRowParserTest {
    private static final List<List<String>> STRING_LISTS = asList(
            asList("\"1\"", "\"2\""),
            asList("<a>", "\"\uD83E\uDE02\"")
    );
    private static final List<List<CharSequence>> CS_LISTS = STRING_LISTS.stream()
            .map(l -> l.stream().map(s -> (CharSequence)new StringBuilder(s)).collect(toList()))
            .collect(toList());
    private static final List<CharSequence[]> STRING_ARRAYS =
            STRING_LISTS.stream().map(l -> l.toArray(new String[0])).collect(toList());
    private static final List<CharSequence[]> CS_ARRAYS =
            STRING_LISTS.stream().map(l -> l.toArray(new CharSequence[0])).collect(toList());
    private static final List<byte[][]> BYTES_ARRAYS = STRING_LISTS.stream().map(l -> {
        byte[][] array = new byte[l.size()][];
        for (int i = 0; i < array.length; i++)
            array[i] = l.get(i).getBytes(StandardCharsets.UTF_8);
        return array;
    }).collect(toList());
    private static final List<List<byte[]>> BYTES_LISTS = STRING_LISTS.stream()
            .map(l -> l.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).collect(toList()))
            .collect(toList());

    SafeCompletableAsyncTask<List<String>> varsFuture;

    @BeforeEach
    void setUp() {
        varsFuture = new SafeCompletableAsyncTask<>();
    }

    private void checkPublisher(Publisher<CharSequence[]> publisher) {
        checkPublisher(publisher, null);
    }

    private void checkPublisher(Publisher<CharSequence[]> publisher, @Nullable List<?> expectSame) {
        List<CharSequence[]> actual = Flux.from(publisher).collectList().block();
        assertNotNull(actual);
        assertEquals(STRING_LISTS.size(), actual.size());
        for (int i = 0; i < STRING_LISTS.size(); i++) {
            List<String> exList = STRING_LISTS.get(i);
            CharSequence[] acArray = actual.get(i);
            assertEquals(exList.size(), acArray.length, "i="+i);
            for (int j = 0; j < exList.size(); j++)
                assertEquals(exList.get(j), acArray[j].toString(), "i="+i+", j="+j);
            if (expectSame != null)
                assertSame(expectSame.get(i), acArray);
        }
    }

    @Test @SuppressWarnings("unchecked")
    void testParseStringArray() {
        Flux<String[]> input = (Flux<String[]>)(Flux<?>) Flux.fromIterable(STRING_ARRAYS);
        Results<CharSequence[]> results = new Results<>(varsFuture, CharSequence[].class, input);
        checkPublisher(INSTANCE.parseStringsArray(results), STRING_ARRAYS);
    }

    @Test
    void testParseStringArrayAsCS() {
        Flux<CharSequence[]> input = Flux.fromIterable(STRING_ARRAYS);
        Results<CharSequence[]> results = new Results<>(varsFuture, CharSequence[].class, input);
        checkPublisher(INSTANCE.parseStringsArray(results), STRING_ARRAYS);
    }

    @Test
    void testParseCSArray() {
        Flux<CharSequence[]> input = Flux.fromIterable(CS_ARRAYS);
        Results<CharSequence[]> results = new Results<>(varsFuture, CharSequence[].class, input);
        checkPublisher(INSTANCE.parseStringsArray(results), CS_ARRAYS);
    }

    @Test
    void testParseStringList() {
        Flux<List<String>> input = Flux.fromIterable(STRING_LISTS);
        Results<List<String>> results = new Results<>(varsFuture, List.class, input);
        checkPublisher(INSTANCE.parseStringsList(results));
    }

    @Test
    void testParseStringListAsCollection() {
        Flux<List<String>> input = Flux.fromIterable(STRING_LISTS);
        Results<List<String>> results = new Results<>(varsFuture, Collection.class, input);
        checkPublisher(INSTANCE.parseStringsList(results));
    }

    @Test
    void testParseCSList() {
        Flux<List<CharSequence>> input = Flux.fromIterable(CS_LISTS);
        Results<List<CharSequence>> results = new Results<>(varsFuture, List.class, input);
        checkPublisher(INSTANCE.parseStringsList(results));
    }

    @Test
    void testParseCSListAsCollection() {
        Flux<List<CharSequence>> input = Flux.fromIterable(CS_LISTS);
        Results<List<CharSequence>> results = new Results<>(varsFuture, Collection.class, input);
        checkPublisher(INSTANCE.parseStringsList(results));
    }

    @Test
    void testParseBytesArray() {
        Flux<byte[][]> input = Flux.fromIterable(BYTES_ARRAYS);
        Results<byte[][]> results = new Results<>(varsFuture, byte[][].class, input);
        checkPublisher(INSTANCE.parseBytesArray(results));
    }

    @Test
    void testParseBytesList() {
        Flux<List<byte[]>> input = Flux.fromIterable(BYTES_LISTS);
        Results<List<byte[]>> results = new Results<>(varsFuture, List.class, input);
        checkPublisher(INSTANCE.parseBytesList(results));
    }

    @Test
    void testParseBytesListAsCollection() {
        Flux<List<byte[]>> input = Flux.fromIterable(BYTES_LISTS);
        Results<List<byte[]>> results = new Results<>(varsFuture, Collection.class, input);
        checkPublisher(INSTANCE.parseBytesList(results));
    }
}