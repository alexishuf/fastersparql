package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.RDFMediaTypes;
import com.github.alexishuf.fastersparql.client.parser.fragment.CharSequenceFragmentParser;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CharSequenceFragmentParserTest {
    private static final List<String> STRINGS = Arrays.asList("test", "rook \uD83E\uDE02");

    @Test
    void testParseStrings() {
        SafeCompletableAsyncTask<MediaType> mtFuture = new SafeCompletableAsyncTask<>();
        mtFuture.complete(RDFMediaTypes.TTL);
        FSPublisher<String> pub = FSPublisher.bindToAny(Flux.fromIterable(STRINGS));
        Graph<String> graph = new Graph<>(mtFuture, String.class, pub);
        Publisher<CharSequence> publisher = CharSequenceFragmentParser.INSTANCE.parseStrings(graph);
        List<CharSequence> parsed = Flux.from(publisher).collectList().block();
        assertNotNull(parsed);
        assertEquals(STRINGS, parsed);
        for (int i = 0; i < STRINGS.size(); i++)
            assertEquals(STRINGS.get(i), parsed.get(i));
    }

    @ParameterizedTest @ValueSource(strings = {"utf-8", "utf-16"})
    void testParseBytes(String csName) {
        Charset cs = Charset.forName(csName);
        SafeCompletableAsyncTask<MediaType> mtFuture = new SafeCompletableAsyncTask<>();
        mtFuture.complete(RDFMediaTypes.TTL.toBuilder().param("charset", csName).build());
        List<byte[]> bytes = STRINGS.stream().map(s -> s.getBytes(cs)).collect(Collectors.toList());
        FSPublisher<byte[]> pub = FSPublisher.bindToAny(Flux.fromIterable(bytes));
        Graph<byte[]> graph = new Graph<>(mtFuture, byte[].class, pub);
        Publisher<CharSequence> publisher = CharSequenceFragmentParser.INSTANCE.parseBytes(graph);
        List<CharSequence> actual = Flux.from(publisher).collectList().block();
        assertEquals(STRINGS, actual);
    }
}