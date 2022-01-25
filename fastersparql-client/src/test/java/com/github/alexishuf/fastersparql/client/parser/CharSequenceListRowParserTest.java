package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.parser.row.CharSequenceListRowParser.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

class CharSequenceListRowParserTest {
    private static final List<List<String>> STRING_LISTS = asList(
            asList("\"1\"", "\"2\""),
            asList("<a>", "\"\uD83E\uDE02\"")
    );
    private static final List<String[]> STRING_ARRAYS =
            STRING_LISTS.stream().map(l -> l.toArray(new String[0])).collect(toList());
    private static final List<List<CharSequence>> CS_LISTS = STRING_LISTS.stream().map(in -> {
        List<CharSequence> out = new ArrayList<>(in.size());
        for (String s : in) out.add(new StringBuilder().append(s));
        return out;
    }).collect(toList());
    private static final List<CharSequence[]> CS_ARRAYS =
            CS_LISTS.stream().map(l -> l.toArray(new CharSequence[0])).collect(toList());
    private static final List<byte[][]> BYTES_ARRAYS =
            STRING_LISTS.stream().map(l -> {
                byte[][] array = new byte[l.size()][];
                for (int i = 0; i < array.length; i++)
                    array[i] = l.get(i).getBytes(StandardCharsets.UTF_8);
                return array;
            }).collect(toList());
    private static final List<List<byte[]>> BYTES_LISTS = STRING_LISTS.stream()
            .map(l -> l.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).collect(toList()))
            .collect(toList());
    private SafeCompletableAsyncTask<List<String>> varsTask;

    @BeforeEach
    void setUp() {
        varsTask = new SafeCompletableAsyncTask<>();
    }

    private void checkPublisher(Publisher<List<CharSequence>> publisher) {
        checkPublisher(publisher, null);
    }

    private void checkPublisher(Publisher<List<CharSequence>> publisher,
                                @Nullable List<?> expectSame) {
        varsTask.complete(asList("x", "y"));
        List<List<CharSequence>> actual = Flux.from(publisher).collectList().block();
        assertNotNull(actual);
        for (int i = 0; i < STRING_LISTS.size(); i++) {
            List<String> exList = STRING_LISTS.get(i);
            List<CharSequence> acList = actual.get(i);
            assertEquals(exList.size(), acList.size(), "i="+i);
            for (int j = 0; j < exList.size(); j++)
                assertEquals(exList.get(j), acList.get(j).toString(), "i="+", j="+j);
            if (expectSame != null)
                assertSame(expectSame.get(i), acList, "i="+i);
        }
    }

    @Test
    void testParseStringArray() {
        Flux<String[]> inputFlux = Flux.fromIterable(STRING_ARRAYS);
        Results<String[]> results = new Results<>(varsTask, String[].class, inputFlux);
        checkPublisher(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseStringArrayAsCSArray() {
        Flux<String[]> inputFlux = Flux.fromIterable(STRING_ARRAYS);
        Results<String[]> results = new Results<>(varsTask, CharSequence[].class, inputFlux);
        checkPublisher(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseCSArray() {
        Flux<CharSequence[]> inputFlux = Flux.fromIterable(CS_ARRAYS);
        Results<CharSequence[]> results = new Results<>(varsTask, CharSequence[].class, inputFlux);
        checkPublisher(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseStringList() {
        Flux<List<String>> inputFlux = Flux.fromIterable(STRING_LISTS);
        Results<List<String>> results = new Results<>(varsTask, List.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results));
    }

    @Test
    void testParseCSList() {
        Flux<List<CharSequence>> inputFlux = Flux.fromIterable(CS_LISTS);
        Results<List<CharSequence>> results = new Results<>(varsTask, List.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results), CS_LISTS);
    }

    @Test
    void testParseCSListAsCollection() {
        Flux<List<CharSequence>> inputFlux = Flux.fromIterable(CS_LISTS);
        Results<Collection<CharSequence>> results = new Results<>(varsTask, Collection.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results), CS_LISTS);
    }

    @Test @SuppressWarnings("unchecked")
    void testParseStringListAsCollection() {
        Flux<Collection<CharSequence>> inputFlux =
                (Flux<Collection<CharSequence>>)(Flux<?>) Flux.fromIterable(STRING_LISTS);
        Results<Collection<CharSequence>> results = new Results<>(varsTask, Collection.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results), STRING_LISTS);
    }

    @Test
    void testParseBytesArray() {
        Flux<byte[][]> inputFlux = Flux.fromIterable(BYTES_ARRAYS);
        Results<byte[][]> results = new Results<>(varsTask, byte[][].class, inputFlux);
        checkPublisher(INSTANCE.parseBytesArray(results));
    }

    @Test
    void testParseBytesList() {
        Flux<List<byte[]>> inputFlux = Flux.fromIterable(BYTES_LISTS);
        Results<List<byte[]>> results = new Results<>(varsTask, List.class, inputFlux);
        checkPublisher(INSTANCE.parseBytesList(results));
    }


    @Test
    void testParseBytesListAsCollection() {
        Flux<List<byte[]>> inputFlux = Flux.fromIterable(BYTES_LISTS);
        Results<Collection<byte[]>> results = new Results<>(varsTask, Collection.class, inputFlux);
        checkPublisher(INSTANCE.parseBytesList(results));
    }
}