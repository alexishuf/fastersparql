package com.github.alexishuf.fastersparql.model;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ContentNegotiatorTest {
    static Stream<Arguments> test() {
        MediaType u8 = MediaType.parse("text/plain; charset=utf-8");
        MediaType ex1 = MediaType.parse("text/ex-1");
        MediaType ex2 = MediaType.parse("application/ex-2");
        ContentNegotiator cn = new ContentNegotiator(u8, ex1, ex2);

        MediaType ex1U8 = MediaType.parse("text/ex-1 ; charset=utf-8");
        MediaType ex1IS = MediaType.parse("text/ex-1 ; charset=iso-8859-1");
        return Stream.of(
            arguments(cn, u8, List.of("text/plain")),
            arguments(cn, u8, List.of("text/plain; charset=utf-8")),
            arguments(cn, ex1, List.of("text/plain; q=0.8, text/ex-1")),
            arguments(cn, ex1U8, List.of("text/plain; q=0.8, text/ex-1; charset=utf-8")),
            arguments(cn, ex2, List.of("text/plain; q=0.8, text/ex-1; charset=utf-8;  q=0.9",
                                       "application/*")),
            arguments(cn, u8, List.of("text/*, application/ex-3")), // prioritize first offer
            arguments(cn, ex1IS, List.of("text/*; charset=iso-8859-1, application/ex-2; q=0.8")),
            arguments(cn, ex1, List.of("text/plain; charset=iso-8859-1, application/ex-2; q=0.8", "text/ex-1")),
            arguments(cn, ex2, List.of("text/plain; charset=iso-8859-1, application/ex-2; q=0.8", "text/ex-1; q=0.7")),
            arguments(cn, ex2, List.of("text/plain; charset=iso-8859-1, application/ex-2; q=0.8", "text/ex-1; q=0.8"))
        );
    }

    @ParameterizedTest @MethodSource void test(ContentNegotiator cn, MediaType expected,
                                               List<String> accepts) {
        assertEquals(expected, cn.select(accepts.iterator()));
    }

}