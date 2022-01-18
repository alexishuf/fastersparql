package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.model.SparqlConfiguration.builder;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
class SparqlConfigurationTest {
    private static final MediaType TURTLE = MediaType.parse("text/turtle");
    private static final MediaType JSONLD = MediaType.parse("application/ld+json");
    private static final MediaType JSONLD_COMPACTED = MediaType.parse("application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"");
    private static final MediaType JSONLD_FLATTENED = MediaType.parse("application/ld+json; profile=\"http://www.w3.org/ns/json-ld#flattened\"");

    static Stream<Arguments> testNonEmptyNonNullDistinct() {
        List<String> fallback = asList("5", "23");
        return Stream.of(
                arguments(null, fallback, fallback),
                arguments(emptyList(), fallback, fallback),
                arguments(singletonList("1"), fallback, singletonList("1")),
                arguments(asList("1", "11"), fallback, asList("1", "11")),
                arguments(asList("1", "11", "1"), fallback, asList("1", "11")),
                arguments(asList("1", "11", "1", "11"), fallback, asList("1", "11")),
                arguments(asList("2", "11", "2", "1"), fallback, asList("2", "11", "1")),
                arguments(asList("1", null), fallback, singletonList("1")),
                arguments(asList("1", null, "2"), fallback, asList("1", "2")),
                arguments(asList(null, "1", null, "2"), fallback, asList("1", "2"))
        );
    }

    @ParameterizedTest @MethodSource
    void testNonEmptyNonNullDistinct(List<String> in, List<String> fallback, List<String> expected) {
        List<String> ac = SparqlConfiguration.nonEmptyNonNullDistinct(in, fallback, "itemName");
        assertEquals(expected, ac);
    }

    static Stream<Arguments> testInvalidNonEmptyNonNullDistinct() {
        return Stream.of(
                arguments(singletonList(null)),
                arguments(asList(null, null))
        );
    }

    @ParameterizedTest @MethodSource
    void testInvalidNonEmptyNonNullDistinct(List<String> input) {
        assertThrows(SparqlClientInvalidArgument.class,
                () -> SparqlConfiguration.nonEmptyNonNullDistinct(input, emptyList(), "itemName"));
    }

    static Stream<Arguments> testSanitizeHeaders() {
        List<Map<@Nullable String, @Nullable String>> in = new ArrayList<>();
        List<Map<String, String>> ex = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            in.add(new HashMap<>());
            ex.add(new HashMap<>());
        }

        in.set(0, null); // null map promoted to empty
        ex.set(0, Collections.emptyMap());

        in.get(1).put(null, null); // null key removed

        in.get(2).put(null, ""); // null key removed

        in.get(3).put("authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l");
        ex.get(3).put("authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l");

        //sanitize key
        in.get(4).put("Authorization : ", "Basic YWxhZGRpbjpvcGVuc2VzYW1l");
        ex.get(4).put("authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l");

        //sanitize value
        in.get(5).put("authorization", "\t Basic YWxhZGRpbjpvcGVuc2VzYW1l \r\n");
        ex.get(5).put("authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l");

        return IntStream.range(0, in.size()).mapToObj(i -> arguments(in.get(i), ex.get(i)));
    }

    @ParameterizedTest @MethodSource
    void testSanitizeHeaders(@Nullable Map<@Nullable String, @Nullable String> in,
                             Map<String, String> expected) {
        assertEquals(expected, SparqlConfiguration.sanitizeHeaders(in));
    }

    static Stream<Arguments> testSanitizeInvalidHeaders() {
        return Stream.of(
                singletonMap(null, "x=1"),
                singletonMap("", "x=1"),
                singletonMap(" ", "x=1"),
                singletonMap("\r\n", "x=1"),
                singletonMap("rate=limit", "10"),
                singletonMap("Accept:", "text/csv"),
                singletonMap("accept", "text/csv"),
                singletonMap("accept", "text/turtle")
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testSanitizeInvalidHeaders(Map<@Nullable String, @Nullable String> in) {
        assertThrows(SparqlClientInvalidArgument.class,
                () -> SparqlConfiguration.sanitizeHeaders(in));
    }

    static Stream<Arguments> testSanitizeAppendHeaders() {
        List<Map<@Nullable String, @Nullable List<@Nullable String>>> in = new ArrayList<>();
        List<Map<String, List<String>>> ex = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            in.add(new HashMap<>());
            ex.add(new HashMap<>());
        }

        in.set(0, null); // null map upgrades to empty map

        in.get(1).put(null, null); // remove null -> null mapping
        in.get(2).put(null, emptyList()); // remove null -> [] mapping
        in.get(3).put(null, singletonList(null)); // remove null -> [null] mapping
        in.get(4).put(null, asList(null, null)); // remove null -> [null, null] mapping

        in.get(4).put(null, asList(null, null)); //null key removal does not affect others
        in.get(4).put("cookie", singletonList("x=1"));
        ex.get(4).put("cookie", singletonList("x=1"));

        in.get(5).put("", asList(null, null)); //treat empty key as null
        in.get(5).put("cookie", singletonList("x=1"));
        ex.get(5).put("cookie", singletonList("x=1"));

        in.get(6).put("cookie", emptyList());
        ex.get(6).put("cookie", emptyList());

        in.get(7).put("cookie", null);
        ex.get(7).put("cookie", emptyList());

        in.get(8).put("\t \r\n", asList(null, null)); //treat whitespace key as null
        in.get(8).put("cookie", singletonList("x=1"));
        ex.get(8).put("cookie", singletonList("x=1"));

        in.get(9).put("\tCookie : \r\n", asList("x=1", "y=2"));
        ex.get(9).put("cookie", asList("x=1", "y=2"));

        in.get(10).put("cookie", asList(null, "y=2"));
        ex.get(10).put("cookie", singletonList("y=2"));

        in.get(11).put("cookie", asList("", "y=2"));
        ex.get(11).put("cookie", singletonList("y=2"));

        in.get(12).put("cookie", asList("\t \r\n", "y=2"));
        ex.get(12).put("cookie", singletonList("y=2"));

        return IntStream.range(0, in.size()).mapToObj(i -> arguments(in.get(i), ex.get(i)));
    }

    @ParameterizedTest @MethodSource
    void testSanitizeAppendHeaders(Map<@Nullable String, @Nullable List<@Nullable String>> in,
                                   Map<String, List<String>> ex) {
        assertEquals(ex, SparqlConfiguration.sanitizeAppendHeaders(in));
    }

    static Stream<Arguments> testSanitizeInvalidAppendHeaders() {
        return Stream.of(
                singletonMap(null, singletonList("x=1")),
                singletonMap(null, asList(null, "x=1")),
                singletonMap("", singletonList("x=1")),
                singletonMap("\t \r\n", singletonList("x=1")),
                singletonMap("\t \r\n", asList(null, "x=1")),
                singletonMap("rate=limit", singletonList("10")),
                singletonMap("cookie", singletonList(null)),
                singletonMap("cookie", asList(null, null)),
                singletonMap("accept", singletonList("text/csv")),
                singletonMap("Accept", singletonList("text/csv")),
                singletonMap("Accept:", singletonList("text/csv")),
                singletonMap("accept", asList("text/csv", "text/turtle"))
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testSanitizeInvalidAppendHeaders(Map<@Nullable String, @Nullable List<@Nullable String>> in) {
        assertThrows(SparqlClientInvalidArgument.class,
                () -> SparqlConfiguration.sanitizeAppendHeaders(in));
    }

    static Stream<Arguments> testSanitizeParams() {
        List<Map<@Nullable String, @Nullable List<@Nullable String>>> in = new ArrayList<>();
        List<Map<String, List<String>>> ex = new ArrayList<>();
        for (int i = 0; i < 14; i++) {
            in.add(new HashMap<>());
            ex.add(new HashMap<>());
        }

        in.set(0, null);

        in.get(1).put(null, null);
        in.get(2).put(null, emptyList());
        in.get(3).put(null, singletonList(null));
        in.get(4).put(null, asList(null, null));

        in.get(5).put("x", singletonList("1")); //normal use
        ex.get(5).put("x", singletonList("1"));

        in.get(6).put("x", null); //upgrade to empty list
        ex.get(6).put("x", emptyList()); //upgrade to empty list

        in.get(7).put("x", asList("1", "2")); //allow multiple values
        ex.get(7).put("x", asList("1", "2"));

        in.get(8).put("", null); // allow empty param names
        ex.get(8).put("", emptyList());

        in.get(9).put("", singletonList("1")); // allow empty param names
        ex.get(9).put("", singletonList("1"));

        in.get(10).put(" ", emptyList()); // allow whitespace param names, but escape
        ex.get(10).put("%20", emptyList());

        in.get(11).put(" ", singletonList("1")); // allow whitespace param names, but escape
        ex.get(11).put("%20", singletonList("1"));

        in.get(12).put("x&y", asList("%20", "1%2")); //escape on key and value, but preserve already escaped
        ex.get(12).put("x%26y", asList("%20", "1%252"));

        in.get(13).put("1%20%2", singletonList("%20%")); // handle partially escaped strings
        ex.get(13).put("1%2520%252", singletonList("%2520%25"));

        return IntStream.range(0, in.size()).mapToObj(i -> arguments(in.get(i), ex.get(i)));
    }

    @ParameterizedTest @MethodSource
    void testSanitizeParams(@Nullable Map<@Nullable String, @Nullable List<@Nullable String>> in,
                            Map<String, List<String>> expected) {
        assertEquals(expected, SparqlConfiguration.sanitizeParams(in));
    }

    static Stream<Arguments> testSanitizeInvalidParams() {
        return Stream.of(
                singletonMap(null, singletonList("1")),
                singletonMap(null, singletonList("")), // the empty string is a valid value
                singletonMap("x", singletonList(null)),
                singletonMap("x", asList(null, null))
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testSanitizeInvalidParams(Map<@Nullable String, @Nullable List<@Nullable String>> in) {
        assertThrows(SparqlClientInvalidArgument.class,
                () -> SparqlConfiguration.sanitizeParams(in));
    }

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(
                        builder().build(),
                        builder().build(),
                        true
                ),
                arguments(
                        builder().method(SparqlMethod.GET).build(),
                        builder().method(SparqlMethod.GET).build(),
                        true
                ),
                arguments(
                        builder().method(SparqlMethod.POST).build(),
                        builder().method(SparqlMethod.GET).build(),
                        false
                ),
                arguments(
                        builder().method(SparqlMethod.POST).build(),
                        builder().build(),
                        false
                ),
                arguments(
                        builder().resultsAccept(SparqlResultFormat.TSV).build(),
                        builder().resultsAccept(SparqlResultFormat.TSV).build(),
                        true
                ),
                arguments(
                        builder().resultsAccept(SparqlResultFormat.TSV).build(),
                        builder().resultsAccept(SparqlResultFormat.CSV).build(),
                        false
                ),

                arguments(
                        builder().resultsAccept(SparqlResultFormat.TSV).build(),
                        builder().build(),
                        false
                ),
                arguments(
                        builder().rdfAccept(TURTLE).build(),
                        builder().rdfAccept(TURTLE).build(),
                        true
                ),
                arguments(
                        builder().rdfAccept(TURTLE).build(),
                        builder().rdfAccept(JSONLD).build(),
                        false
                ),
                arguments(
                        builder().rdfAccept(TURTLE).build(),
                        builder().rdfAccept(TURTLE).rdfAccept(JSONLD).build(),
                        false
                ),
                arguments(
                        builder().rdfAccept(TURTLE).build(),
                        builder().build(),
                        false
                ),
                arguments(
                        builder().param("x", singletonList("1")).build(),
                        builder().param("x", singletonList("1")).build(),
                        true
                ),
                arguments(
                        builder().param("x", asList("1", "2")).build(),
                        builder().param("x", asList("2", "1")).build(),
                        false
                ),
                arguments(
                        builder().param("x", singletonList("1")).build(),
                        builder().build(),
                        false
                ),
                arguments(
                        builder().header("Cookie", "x=1").build(),
                        builder().header("cookie", "x=1").build(),
                        true
                ),
                arguments(
                        builder().header("cookie", "x=1").build(),
                        builder().header("cookie", "x=2").build(),
                        false
                ),
                arguments(
                        builder().header("cookie", "x=1").build(),
                        builder().build(),
                        false
                ),
                arguments(
                        builder().appendHeader("cookie", singletonList("x=1")).build(),
                        builder().appendHeader("cookie", singletonList("x=1")).build(),
                        true
                ),
                arguments(
                        builder().appendHeader("Cookie", singletonList("x=1")).build(),
                        builder().appendHeader("cookie", singletonList("x=1")).build(),
                        true
                ),
                arguments(
                        builder().appendHeader("cookie", singletonList("x=1")).build(),
                        builder().header("cookie", "x=1").build(),
                        false
                ),
                arguments(
                        builder().appendHeader("cookie", singletonList("1")).build(),
                        builder().build(),
                        false
                )
        );
    }

    @SuppressWarnings({"SimplifiableAssertion", "EqualsWithItself"})
    @ParameterizedTest @MethodSource
    void testEquals(SparqlConfiguration a, SparqlConfiguration b, boolean expected) {
        assertEquals(expected, a.equals(b));
        assertEquals(expected, b.equals(a));
        assertTrue(a.equals(a));
        assertTrue(b.equals(b));
    }

    static Stream<Arguments> testOverlay() {
        return Stream.of(
   /*  1 */     arguments(builder().build(), builder().build(), builder().build()),
   /*  2 */     arguments(null, builder().build(), builder().build()),
   /*  3 */     arguments(builder().build(), null, builder().build()),
   /*  4 */     arguments(null, null, builder().build()),
   /*  5 */     arguments(
                        builder().build(),
                        builder().method(SparqlMethod.GET).build(),
                        builder().method(SparqlMethod.GET).build()
                ),
   /*  6 */     arguments(
                        builder().method(SparqlMethod.GET).build(),
                        builder().build(),
                        builder().method(SparqlMethod.GET).build()
                ),
   /*  7 */     arguments(
                        builder().method(SparqlMethod.GET).build(),
                        builder().method(SparqlMethod.FORM).build(),
                        builder().method(SparqlMethod.FORM).build()
                ),
   /*  8 */     arguments(
                        builder().build(),
                        builder().resultsAccept(SparqlResultFormat.JSON).build(),
                        builder().resultsAccept(SparqlResultFormat.JSON).build()
                ),
   /*  9 */     arguments(
                        builder().resultsAccept(SparqlResultFormat.JSON).build(),
                        builder().build(),
                        builder().resultsAccept(SparqlResultFormat.JSON).build()
                ),
   /* 10 */     arguments(
                        builder().resultsAccept(SparqlResultFormat.JSON).build(),
                        builder().resultsAccept(SparqlResultFormat.TSV).build(),
                        builder().resultsAccept(SparqlResultFormat.TSV).build()
                ),
   /* 11 */     arguments(
                        builder().build(),
                        builder().rdfAccept(TURTLE).rdfAccept(JSONLD).build(),
                        builder().rdfAccept(TURTLE).rdfAccept(JSONLD).build()
                ),
   /* 12 */     arguments(
                        builder().rdfAccept(TURTLE).rdfAccept(JSONLD).build(),
                        builder().build(),
                        builder().rdfAccept(TURTLE).rdfAccept(JSONLD).build()
                ),
   /* 13 */     arguments(
                        builder().rdfAccept(TURTLE).build(),
                        builder().rdfAccept(JSONLD).build(),
                        builder().rdfAccept(JSONLD).build()
                ),
   /* 14 */     arguments(
                        builder().param("x", singletonList("1")).build(),
                        builder().build(),
                        builder().param("x", singletonList("1")).build()
                ),
   /* 15 */     arguments(
                        builder().build(),
                        builder().param("x", singletonList("1")).build(),
                        builder().param("x", singletonList("1")).build()
                ),
   /* 16 */     arguments(
                        builder().param("x", singletonList("1")).build(),
                        builder().param("y", singletonList("2")).build(),
                        builder().param("x", singletonList("1"))
                                 .param("y", singletonList("2")).build()
                ),
   /* 17 */     arguments(
                        builder().param("x", singletonList("1")).build(),
                        builder().param("x", singletonList("2")).build(),
                        builder().param("x", asList("1", "2")).build()
                ),
   /* 18 */     arguments(
                        builder().header("Forwarded", "for=192.168.0.1").build(),
                        builder().build(),
                        builder().header("forwarded", "for=192.168.0.1").build()
                ),
   /* 19 */     arguments(
                        builder().build(),
                        builder().header("Forwarded", "for=192.168.0.1").build(),
                        builder().header("forwarded", "for=192.168.0.1").build()
                ),
   /* 20 */     arguments(
                        builder().header("Forwarded", "for=192.168.0.1").build(),
                        builder().header("forwarded", "for=192.168.0.2").build(),
                        builder().header("forwarded", "for=192.168.0.2").build()
                ),
   /* 21 */     arguments(
                        builder().header("forwarded", "for=192.168.0.1").build(),
                        builder().header("cookie", "x=1").build(),
                        builder().header("forwarded", "for=192.168.0.1")
                                 .header("cookie", "x=1").build()
                ),
   /* 22 */     arguments(
                        builder().header("cookie", "x=1").build(),
                        builder().appendHeader("cookie", singletonList("x=2")).build(),
                        builder().appendHeader("cookie", singletonList("x=2")).build()
                ),
   /* 23 */     arguments(
                        builder().appendHeader("cookie", asList("x=2","y=3")).build(),
                        builder().header("cookie", "x=1").build(),
                        builder().header("cookie", "x=1").build()
                ),
   /* 24 */     arguments(
                        builder().appendHeader("cookie", singletonList("x=1")).build(),
                        builder().appendHeader("cookie", singletonList("y=2")).build(),
                        builder().appendHeader("cookie", asList("y=2", "x=1")).build()
                ),
   /* 25 */     arguments(
                        builder().appendHeader("cookie", singletonList("y=2")).build(),
                        builder().appendHeader("cookie", singletonList("x=1")).build(),
                        builder().appendHeader("cookie", asList("x=1", "y=2")).build()
                )
        );
    }

    @ParameterizedTest @MethodSource
    void testOverlay(@Nullable SparqlConfiguration lower, @Nullable SparqlConfiguration higher,
                     SparqlConfiguration expected) {
        if (lower != null)
            assertEquals(expected, lower.overlayWith(higher));
        if (higher != null)
            assertEquals(expected, higher.overlayAbove(lower));
        assertEquals(expected, SparqlConfiguration.overlay(lower, higher));
    }

    static Stream<Arguments> testAccepts() {
        return Stream.of(
                arguments(
                        SparqlConfiguration.builder().method(SparqlMethod.GET).build(),
                        null
                ),
                arguments(
                        SparqlConfiguration.builder().method(SparqlMethod.GET).method(SparqlMethod.FORM).build(),
                        SparqlConfiguration.builder().method(SparqlMethod.FORM).build()
                ),
                arguments(
                        null,
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.JSON).build()
                ),
                arguments(
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.JSON).resultsAccept(SparqlResultFormat.TSV).build(),
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.TSV).build()
                ),
                arguments(
                        null,
                        SparqlConfiguration.builder().rdfAccept(TURTLE).build()
                ),
                arguments(
                        SparqlConfiguration.builder().rdfAccept(TURTLE).rdfAccept(JSONLD).build(),
                        SparqlConfiguration.builder().rdfAccept(TURTLE).build()
                ),
                arguments(
                        SparqlConfiguration.builder().rdfAccept(JSONLD).build(),
                        SparqlConfiguration.builder().rdfAccept(JSONLD_COMPACTED).build()
                ),
                arguments(
                        SparqlConfiguration.builder().rdfAccept(JSONLD_COMPACTED).build(),
                        SparqlConfiguration.builder().rdfAccept(JSONLD_FLATTENED).rdfAccept(JSONLD_COMPACTED).build()
                )
        );
    }

    @ParameterizedTest @MethodSource
    void testAccepts(@Nullable SparqlConfiguration req, SparqlConfiguration offer) {
        if (offer != null)
            assertTrue(offer.isAcceptedBy(req));
        if (req != null)
            assertTrue(req.accepts(offer));
    }

    static Stream<Arguments> testNotAccepts() {
        return Stream.of(
                arguments(
                        SparqlConfiguration.builder().method(SparqlMethod.GET).build(),
                        SparqlConfiguration.builder().method(SparqlMethod.POST).build()
                ),
                arguments(
                        SparqlConfiguration.builder().method(SparqlMethod.GET).method(SparqlMethod.FORM).build(),
                        SparqlConfiguration.builder().method(SparqlMethod.POST).build()
                ),
                arguments(
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.JSON).build(),
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.TSV).build()
                ),
                arguments(
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.CSV).resultsAccept(SparqlResultFormat.JSON).build(),
                        SparqlConfiguration.builder().resultsAccept(SparqlResultFormat.TSV).build()
                ),
                arguments(
                        SparqlConfiguration.builder().rdfAccept(JSONLD_COMPACTED).build(),
                        SparqlConfiguration.builder().rdfAccept(JSONLD).build()
                ),
                arguments(
                        SparqlConfiguration.builder().rdfAccept(JSONLD_COMPACTED).build(),
                        SparqlConfiguration.builder().rdfAccept(JSONLD_FLATTENED).build()
                )
        );
    }

    @ParameterizedTest @MethodSource
    void testNotAccepts(@Nullable SparqlConfiguration req,
                        @Nullable SparqlConfiguration offer) {
        if (offer != null)
            assertFalse(offer.isAcceptedBy(req));
        if (req != null)
            assertFalse(req.accepts(offer));
    }
}