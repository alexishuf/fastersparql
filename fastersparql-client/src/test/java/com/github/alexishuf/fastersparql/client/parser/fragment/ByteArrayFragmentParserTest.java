package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser.Encoder;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ByteArrayFragmentParserTest {
    @Test
    void testParseBytes() {
        byte[] data = "test".getBytes(UTF_8);
        SafeCompletableAsyncTask<MediaType> mtFuture = new SafeCompletableAsyncTask<>();
        Graph<byte[]> graph = new Graph<>(mtFuture, byte[].class, Mono.just(data));
        Publisher<byte[]> publisher = ByteArrayFragmentParser.INSTANCE.parseBytes(graph);
        List<byte[]> expected = Collections.singletonList(data);
        assertEquals(expected, Flux.from(publisher).collectList().block());
    }

    static Stream<Arguments> encoderData() {
        List<Arguments> list = new ArrayList<>();
        for (Charset cs : asList(UTF_8, UTF_16LE, UTF_16BE, ISO_8859_1)) {
            for (String string : asList("test", "ação", "\uD83E\uDE02"))
                list.add(arguments(string, cs));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource("encoderData")
    void testEncoder(String string, Charset cs) throws Throwable {
        byte[] expected = string.getBytes(cs);
        SafeCompletableAsyncTask<Charset> csFuture = new SafeCompletableAsyncTask<>();
        csFuture.complete(cs);
        Encoder encoder = new Encoder(csFuture);
        assertArrayEquals(expected, encoder.apply(string));
    }

    @ParameterizedTest @MethodSource("encoderData")
    void testFragmentedEncoder(String string, Charset cs) throws Throwable {
        byte[] expected = string.getBytes(cs);
        SafeCompletableAsyncTask<Charset> csFuture = new SafeCompletableAsyncTask<>();
        csFuture.complete(cs);
        Encoder encoder = new Encoder(csFuture);
        ByteBuffer bb = ByteBuffer.allocate(string.length()*4);
        for (char c : string.toCharArray())
            bb.put(encoder.apply(new StringBuilder().append(c)));
        assertArrayEquals(expected, Arrays.copyOf(bb.array(), bb.position()));
    }

    @ParameterizedTest @ValueSource(strings = {
            "text/turtle; charset=iso-8859-1 | ISO-8859-1",
            "text/turtle | UTF-8"
    })
    void testParseStrings(String dataString) {
        String[] data = dataString.split(" *\\| *");
        MediaType mediaType = MediaType.parse(data[0]);
        Charset effCharset = Charset.forName(data[1]);
        List<CharSequence> fragmentStrings = asList("test", "ação");
        List<byte[]> fragmentBytes = fragmentStrings.stream()
                .map(s -> s.toString().getBytes(effCharset)).collect(Collectors.toList());

        for (int i = 0; i < 10; i++) {
            SafeCompletableAsyncTask<MediaType> mtFuture = new SafeCompletableAsyncTask<>();
            Flux<CharSequence> fragmentFlux = Flux.fromIterable(fragmentStrings);
            Graph<CharSequence> graph = new Graph<>(mtFuture, CharSequence.class, fragmentFlux);
            Publisher<byte[]> bytesPub = ByteArrayFragmentParser.INSTANCE.parseStrings(graph);
            Thread completer = new Thread(() -> mtFuture.complete(mediaType));
            completer.start();
            List<byte[]> actual = Flux.from(bytesPub).collectList().block();
            assertNotNull(actual);
            assertEquals(fragmentBytes.size(), actual.size());
            for (int j = 0; j < fragmentBytes.size(); j++)
                assertArrayEquals(fragmentBytes.get(j), actual.get(j), "j="+j);
            assertTimeout(Duration.ofMinutes(10), () -> completer.join(10));
            assertFalse(completer.isAlive());
        }
    }

}