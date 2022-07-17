package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.async.Async.asyncThrowing;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparqlEndpointTest {
    @ParameterizedTest @ValueSource(strings = {
            "http://example.org/sparql|HTTP",
            "https://example.org/sparql|HTTPS",
    })
    void testProtocol(String dataString) {
        String[] data = dataString.split("\\|");
        assertEquals(Protocol.valueOf(data[1]), new SparqlEndpoint(data[0]).protocol());
    }

    @ParameterizedTest @ValueSource(strings = {
            "http://example.org/sparql|80",
            "https://example.org/sparql|443",
            "https://example.org:8080/sparql|8080",
            "http://example.org:8080/sparql|8080",
    })
    void testPort(String dataString) {
        String[] data = dataString.split("\\|");
        assertEquals(Integer.parseInt(data[1]), new SparqlEndpoint(data[0]).port());
    }

    @ParameterizedTest @ValueSource(strings = {
            "http://example.org/sparql|example.org",
            "http://www.example.org/sparql|www.example.org",
            "http://127.0.0.1/sparql|127.0.0.1",
            "http://[::1]/sparql|[::1]",
    })
    void testHost(String dataString) {
        String[] data = dataString.split("\\|");
        assertEquals(data[1], new SparqlEndpoint(data[0]).host());
    }

    @ParameterizedTest @ValueSource(strings = {
            "http://example.org/sparql|/sparql",
            "http://example.org//sparql|/sparql",
            "http://example.org|/",
            "http://example.org:8080|/",
            "http://example.org////|/",
            "http://example.org/~bob/sparql|/~bob/sparql",
            "http://example.org/~bob/sparql/|/~bob/sparql/",
            "http://example.org/sparql/query|/sparql/query",
            "http://example.org/sparql/query/|/sparql/query/",
            "http://example.org/sparql?profile=1|/sparql?profile=1",
            "http://example.org/sparql?profile=1%202|/sparql?profile=1%202",
    })
    void testRawPathWithQuery(String dataString) {
        String[] data = dataString.split("\\|");
        assertEquals(data[1], new SparqlEndpoint(data[0]).rawPathWithQuery());
    }

    @Test
    void testUnresolvableHost() throws ExecutionException {
        unresolvableHost.get();
    }
    private static final AsyncTask<?> unresolvableHost =
            asyncThrowing(SparqlEndpointTest::doTestUnresolvableHost);
    private static void doTestUnresolvableHost() {
        //load class and initialize static fields
        SparqlEndpoint ep = new SparqlEndpoint("http://example.org/sparql");
        assertFalse(ep.hasQuery()); // use above value

        // constructor must not block resolving address
        long start = System.nanoTime();
        ep = new SparqlEndpoint("http://bad.thistlddoesnotexist/sparql");
        long ms = (System.nanoTime() - start) / 1000000;
        assertTrue(ms < 100, "constructor too slow ("+ms+" >100ms)");


        try {
            start = System.nanoTime();
            assertNull(ep.resolvedHost().orElse(null, 200, TimeUnit.MILLISECONDS));
            // ok if returns null: timeout expired
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnknownHostException);
            // ok if thrown: failed fast
        }
        ms = (System.nanoTime()-start)/1000000;
        assertTrue(ms < 300, "exceeded 200ms timeout: "+ms);
    }

    @Test @Timeout(5)
    void testResolveHost() throws UnknownHostException, ExecutionException {
        long start = System.nanoTime();
        SparqlEndpoint ep = new SparqlEndpoint("http://localhost:8080/sparql");
        assertTrue(System.nanoTime() - start < 20*1000*1000, "too slow (>20ms)");

        InetSocketAddress ac = ep.resolvedHost().get();
        InetSocketAddress ex = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080);
        assertEquals(ex, ac);
    }

    @ParameterizedTest @ValueSource(strings = {
            "http://example.org/sparql|false",
            "http://example.org/sparql/query|false",
            "http://example.org/sparql/query/|false",
            "http://example.org/sparql?x=1|true",
            "http://example.org/sparql?x=1|true",
            "http://example.org/sparql?x=1&y=2|true",
            "http://example.org/sparql?x=1%202|true",
            "http://example.org/sparql%3Fx=1%26y=2|false",
    })
    void testHasQuery(String dataString) {
        String[] data = dataString.split("\\|");
        assertEquals(Boolean.parseBoolean(data[1]), new SparqlEndpoint(data[0]).hasQuery());
    }

    @Test
    void testConfigurationKept() {
        SparqlConfiguration configuration = SparqlConfiguration.builder().method(SparqlMethod.FORM).resultsAccept(SparqlResultFormat.JSON).build();
        SparqlEndpoint ep = new SparqlEndpoint("http://example.org/sparql", configuration);
        assertSame(ep.configuration(), configuration);
    }

    static Stream<Arguments> testAugmentedURI() {
        String ex = "http://example.org/sparql";
        SparqlConfiguration tsv = SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.TSV).build();
        SparqlConfiguration tsvTtlJsonld = SparqlConfiguration.builder()
                .resultsAccept(SparqlResultFormat.TSV)
                .rdfAccept(RDFMediaTypes.TTL).rdfAccept(RDFMediaTypes.JSONLD).build();
        SparqlConfiguration tsvJsonldTtl = SparqlConfiguration.builder()
                .resultsAccept(SparqlResultFormat.TSV)
                .rdfAccept(RDFMediaTypes.JSONLD).rdfAccept(RDFMediaTypes.TTL).build();
        SparqlConfiguration post = SparqlConfiguration.builder().method(SparqlMethod.POST).build();
        SparqlConfiguration get = SparqlConfiguration.builder().method(SparqlMethod.GET).build();
        SparqlConfiguration form = SparqlConfiguration.builder().method(SparqlMethod.FORM).build();
        SparqlConfiguration getTsv = SparqlConfiguration.builder().method(SparqlMethod.GET).resultsAccept(SparqlResultFormat.TSV).build();
        return Stream.of(
                arguments(ex, ex, SparqlConfiguration.EMPTY),
                arguments("tsv@"+ex, ex, tsv),
                arguments("tsv,ttl,jsonld@"+ex, ex, tsvTtlJsonld),
                arguments("jsonld,tsv,ttl@"+ex, ex, tsvJsonldTtl),
                arguments("tsv,jsonld,ttl@"+ex, ex, tsvJsonldTtl),
                arguments("ttl,tsv,jsonld@"+ex, ex, tsvTtlJsonld),
                arguments("post@"+ex, ex, post),
                arguments("get@"+ex, ex, get),
                arguments("form@"+ex, ex, form),
                arguments("get,tsv@"+ex, ex, getTsv),
                arguments("tsv,get@"+ex, ex, getTsv)
        );
    }

    @ParameterizedTest @MethodSource
    void testAugmentedURI(String uri, String plainUri, SparqlConfiguration expectedConfig) {
        SparqlEndpoint parsed = SparqlEndpoint.parse(uri);
        assertEquals(parsed.uri(), plainUri);
        assertEquals(parsed.configuration(), expectedConfig);
    }

    static Stream<Arguments> testEquals() {
        String a = "http://a.example.org/sparql";
        String b = "http://b.example.org/sparql";
        return Stream.of(
                arguments(new SparqlEndpoint(a, SparqlConfiguration.EMPTY),
                          new SparqlEndpoint(a, SparqlConfiguration.EMPTY),
                          true),
                arguments(new SparqlEndpoint(a, SparqlConfiguration.EMPTY),
                          new SparqlEndpoint(b, SparqlConfiguration.EMPTY),
                          false),
                arguments(new SparqlEndpoint(a, SparqlConfiguration.builder().method(SparqlMethod.GET).build()),
                          new SparqlEndpoint(a, SparqlConfiguration.builder().method(SparqlMethod.GET).build()),
                          true),
                arguments(new SparqlEndpoint(a, SparqlConfiguration.builder().method(SparqlMethod.GET).build()),
                          new SparqlEndpoint(b, SparqlConfiguration.builder().method(SparqlMethod.GET).build()),
                          false),
                arguments(new SparqlEndpoint(a, SparqlConfiguration.builder().method(SparqlMethod.GET).build()),
                          new SparqlEndpoint(a, SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.JSON).build()),
                          false)
        );
    }

    @SuppressWarnings({"EqualsWithItself", "SimplifiableAssertion"}) @ParameterizedTest @MethodSource
    void testEquals(SparqlEndpoint a, SparqlEndpoint b, boolean expected) {
        assertEquals(expected, a.equals(b));
        assertEquals(expected, b.equals(a));
        assertTrue(a.equals(a));
        assertTrue(b.equals(b));
    }

    @ParameterizedTest @ValueSource(strings = {
            "ws://example.org/sparql",
            "post,tsv@http://example.org/sparql",
            "http://example.org/sparql",
            "get,tsv,json@https://example.org/sparql",
    })
    void testToStringFromAugmented(String augmented) {
        assertEquals(augmented, SparqlEndpoint.parse(augmented).toString());
    }
}