package com.github.alexishuf.fastersparql.client.parser;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.RDFMediaTypes;
import com.github.alexishuf.fastersparql.client.parser.fragment.StringFragmentParser;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

class StringFragmentParserTest {
    private static final List<String> STRINGS = asList("test", "rook \uD83E\uDE02");

    @Test
    void testParseStrings() {
        SafeCompletableAsyncTask<MediaType> mtFuture = new SafeCompletableAsyncTask<>();
        MediaType mt = RDFMediaTypes.TTL.toBuilder()
                .param("charset", "utf-8").build();
        mtFuture.complete(mt);
        Graph<String> graph = new Graph<>(mtFuture, String.class, Flux.fromIterable(STRINGS));
        Flux<String> flux = Flux.from(StringFragmentParser.INSTANCE.parseStrings(graph));
        List<String> actual = flux.collectList().block();
        assertNotNull(actual);
        assertEquals(STRINGS, actual);
        for (int i = 0; i < STRINGS.size(); i++)
            assertSame(STRINGS.get(i), actual.get(i));
    }

    @ParameterizedTest @ValueSource(strings = {"utf-8", "utf-16"})
    void testParseBytes(String csName) {
        Charset cs = Charset.forName(csName);
        SafeCompletableAsyncTask<MediaType> mtFuture = new SafeCompletableAsyncTask<>();
        mtFuture.complete(RDFMediaTypes.TTL.toBuilder().param("charset", csName).build());
        List<byte[]> bytes = STRINGS.stream().map(s -> s.getBytes(cs)).collect(Collectors.toList());
        Graph<byte[]> graph = new Graph<>(mtFuture, byte[].class, Flux.fromIterable(bytes));
        Publisher<String> publisher = StringFragmentParser.INSTANCE.parseBytes(graph);
        assertEquals(STRINGS, Flux.from(publisher).collectList().block());
    }
}