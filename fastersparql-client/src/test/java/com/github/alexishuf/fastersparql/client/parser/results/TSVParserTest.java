package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TSVParserTest {
    static Stream<Arguments> data() {
        return Stream.of(
    /*  1 */    arguments("empty", "", emptyList(), emptyList()),
    /*  2 */    arguments("negative ASK", "\n", emptyList(), emptyList()),
    /*  3 */    arguments("positive ASK", "\n\n", emptyList(), singletonList(emptyList())),
    /*  4 */    arguments("no-x", "?x\n", singletonList("x"), emptyList()),
    /*  5 */    arguments("opt-x", "?x\n\n", singletonList("x"),
                          singletonList(singletonList(null))),
    /*  6 */    arguments("rel-x", "?x\n<a>\n", singletonList("x"),
                          singletonList(singletonList("<a>"))),
    /*  7 */    arguments("rel-x-no-trailing", "?x\n<a>", singletonList("x"),
                          singletonList(singletonList("<a>"))),
    /*  8 */    arguments("abs-x", "?x\n<http://example.org/?page=A&x=y#[]>\n",
                          singletonList("x"),
                          singletonList(singletonList("<http://example.org/?page=A&x=y#[]>"))),
    /*  9 */    arguments("int-x", "?x\n\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>\n",
                          singletonList("x"),
                          singletonList(singletonList("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>"))),
    /* 10 */    arguments("lang-x", "?x\n\"bob\"@en-US\n", singletonList("x"),
                          singletonList(singletonList("\"bob\"@en-US"))),
    /* 11 */    arguments("plain-x", "?x\n\"bob\"\n", singletonList("x"),
                          singletonList(singletonList("\"bob\""))),
    /* 12 */    arguments("no-xy", "?x\t?y\n", asList("x", "y"), emptyList()),
    /* 13 */    arguments("no-xy-no-trailing", "?x\t?y", asList("x", "y"), emptyList()),
    /* 14 */    arguments("null-xy", "?x\t?y\n\t\n", asList("x", "y"),
                          singletonList(asList(null, null))),
    /* 15 */    arguments("2-null-xy", "?x\t?y\n\t\n\t\n", asList("x", "y"),
                          asList(asList(null, null), asList(null, null))),
    /* 16 */    arguments("1-xy", "?x\t?y\n<a>\t<b>\n", asList("x", "y"),
                          singletonList(asList("<a>", "<b>"))),
    /* 17 */    arguments("1-xy-no-trailing", "?x\t?y\n<a>\t<b>", asList("x", "y"),
                          singletonList(asList("<a>", "<b>"))),
    /* 18 */    arguments("1-xy-escapes", "?x\t?y\n\"\\ta\"\t\"\\nb\"\n", asList("x", "y"),
                          singletonList(asList("\"\\ta\"", "\"\\nb\""))),
    /* 19 */    arguments("2-xy-escapes", "?x\t?y\n\"\\ta\"\t\"\\nb\"\n<a>\t<b>\n",
                          asList("x", "y"),
                          asList(asList("\"\\ta\"", "\"\\nb\""),
                                 asList("<a>",      "<b>"))),
    /* 20 */    arguments("missing-mark", "x\n<a>", singletonList("x"),
                          singletonList(singletonList("<a>"))),
    /* 21 */    arguments("missing-2-mark", "x\ty\n\t<b>\n", asList("x", "y"),
                          singletonList(asList(null, "<b>"))),
    /* 22 */    arguments("excess-columns", "?x\n<a>\t<b>\n", singletonList("x"), null),
    /* 23 */    arguments("excess-null-columns", "?x\n\t<b>\n", singletonList("x"), null),
    /* 24 */    arguments("excess-columns-2nd-row", "?x\n<a>\n\t\t\n", singletonList("x"),
                          asList(singletonList("<a>"), null))
        ).map(a -> {
            //noinspection unchecked
            List<List<String>> lists = (List<List<String>>) a.get()[3];
            List<String[]> arrays = null;
            if (lists != null) {
                arrays = lists.stream()
                        .map(l -> l == null ? null : l.toArray(new String[0]))
                        .collect(Collectors.toList());
            }
            return arguments(a.get()[0], a.get()[1], a.get()[2], arrays);
        });
    }

    @ParameterizedTest @MethodSource("data")
    void testParseSingleFeed(String name, String input, @Nullable List<String> vars,
                             @Nullable List<@Nullable String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        TSVParser parser = new TSVParser(consumer);
        parser.feed(input);
        parser.end();
        consumer.check(vars, rows);
    }

    @ParameterizedTest @MethodSource("data")
    void testParseSingleFeedAsStringBuilder(String name, String input, @Nullable List<String> vars,
                                            @Nullable List<@Nullable String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        TSVParser parser = new TSVParser(consumer);
        parser.feed(new StringBuilder(input));
        parser.end();
        consumer.check(vars, rows);
    }

    @ParameterizedTest @MethodSource("data")
    void testParseLineFeed(String name, String input, @Nullable List<String> vars,
                           @Nullable List<@Nullable String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        TSVParser parser = new TSVParser(consumer);
        for (int last = input.length()-1, start = 0, end; start <= last; start = end) {
            end = CSUtils.skipUntilIn(input, start, last, '\n')+1;
            parser.feed(input.substring(start, end));
        }
        parser.end();
        consumer.check(vars, rows);
    }

    @ParameterizedTest @MethodSource("data")
    void testParseLineFeedAsStringBuilder(String name, String input, @Nullable List<String> vars,
                                          @Nullable List<@Nullable String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        TSVParser parser = new TSVParser(consumer);
        for (int last = input.length()-1, start = 0, end; start <= last; start = end) {
            end = CSUtils.skipUntilIn(input, start, last, '\n')+1;
            parser.feed(new StringBuilder(input.substring(start, end)));
        }
        parser.end();
        consumer.check(vars, rows);
    }

    @ParameterizedTest @MethodSource("data")
    void testParseByChar(String name, String input, @Nullable List<String> vars,
                         @Nullable List<@Nullable String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        TSVParser parser = new TSVParser(consumer);
        for (int i = 0, len = input.length(); i < len; i++)
            parser.feed("" + input.charAt(i));
        parser.end();
        consumer.check(vars, rows);
    }

    @ParameterizedTest @MethodSource("data")
    void testParseByCharAsStringBuilder(String name, String input, @Nullable List<String> vars,
                                        @Nullable List<@Nullable String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        TSVParser parser = new TSVParser(consumer);
        for (int i = 0, len = input.length(); i < len; i++)
            parser.feed(new StringBuilder().append(input.charAt(i)));
        parser.end();
        consumer.check(vars, rows);
    }
}