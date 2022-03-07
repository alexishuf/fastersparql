package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import org.checkerframework.checker.nullness.qual.Nullable;
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
    private static final List<String> varsList = asList("x", "y");

    private void checkPublisher(Publisher<List<CharSequence>> publisher) {
        checkPublisher(publisher, null);
    }

    private void checkPublisher(Publisher<List<CharSequence>> publisher,
                                @Nullable List<?> expectSame) {
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
        FSPublisher<String[]> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(STRING_ARRAYS));
        Results<String[]> results = new Results<>(varsList, String[].class, inputFlux);
        checkPublisher(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseStringArrayAsCSArray() {
        FSPublisher<String[]> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(STRING_ARRAYS));
        Results<String[]> results = new Results<>(varsList, CharSequence[].class, inputFlux);
        checkPublisher(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseCSArray() {
        FSPublisher<CharSequence[]> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(CS_ARRAYS));
        Results<CharSequence[]> results = new Results<>(varsList, CharSequence[].class, inputFlux);
        checkPublisher(INSTANCE.parseStringsArray(results));
    }

    @Test
    void testParseStringList() {
        FSPublisher<List<String>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(STRING_LISTS));
        Results<List<String>> results = new Results<>(varsList, List.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results));
    }

    @Test
    void testParseCSList() {
        FSPublisher<List<CharSequence>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(CS_LISTS));
        Results<List<CharSequence>> results = new Results<>(varsList, List.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results), CS_LISTS);
    }

    @Test
    void testParseCSListAsCollection() {
        FSPublisher<List<CharSequence>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(CS_LISTS));
        Results<Collection<CharSequence>> results = new Results<>(varsList, Collection.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results), CS_LISTS);
    }

    @Test @SuppressWarnings("unchecked")
    void testParseStringListAsCollection() {
        FSPublisher<Collection<CharSequence>> inputFlux = FSPublisher.bindToAny(                (Flux<Collection<CharSequence>>)(Flux<?>) Flux.fromIterable(STRING_LISTS));
        Results<Collection<CharSequence>> results = new Results<>(varsList, Collection.class, inputFlux);
        checkPublisher(INSTANCE.parseStringsList(results), STRING_LISTS);
    }

    @Test
    void testParseBytesArray() {
        FSPublisher<byte[][]> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(BYTES_ARRAYS));
        Results<byte[][]> results = new Results<>(varsList, byte[][].class, inputFlux);
        checkPublisher(INSTANCE.parseBytesArray(results));
    }

    @Test
    void testParseBytesList() {
        FSPublisher<List<byte[]>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(BYTES_LISTS));
        Results<List<byte[]>> results = new Results<>(varsList, List.class, inputFlux);
        checkPublisher(INSTANCE.parseBytesList(results));
    }


    @Test
    void testParseBytesListAsCollection() {
        FSPublisher<List<byte[]>> inputFlux = FSPublisher.bindToAny(Flux.fromIterable(BYTES_LISTS));
        Results<Collection<byte[]>> results = new Results<>(varsList, Collection.class, inputFlux);
        checkPublisher(INSTANCE.parseBytesList(results));
    }
}