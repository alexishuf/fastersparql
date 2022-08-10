package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.parser.results.WebSocketResultsParser.skipUntilControl;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WebSocketResultsParserTest {

    static Stream<Arguments> testSkipUntilControl() {
        return Stream.of(
                arguments("", 0, 0),
                arguments("?x\n", 0, 3),
                arguments("?x\n<a>", 0, 6),
                arguments("?x\n<a>\n", 0, 7),
                arguments("!end\n?x\n<a>\n", 0, 0),
                arguments("?x\n!wtf\n", 0, 3),
                arguments("?x\n!wtf\n", 1, 3),
                arguments("?x\n!wtf\n", 2, 3),
                arguments("?x\n!wtf\n", 3, 3),
                arguments("?x\n!wtf\n", 4, 8),
                arguments("?x\n\"!\"\n", 0, 7)
        );
    }

    @ParameterizedTest @MethodSource
    void testSkipUntilControl(String in, int begin, int expected) {
        for (String prefix : asList("", "!", "\n!\n")) {
            String augmented = prefix + in;
            int adjBegin = begin + prefix.length(), adjExpected = expected + prefix.length();
            assertEquals(skipUntilControl(augmented, adjBegin), adjExpected);
            StringBuilder sb = new StringBuilder(augmented);
            assertEquals(skipUntilControl(sb, adjBegin), adjExpected);
        }
    }

    private static class ParseResult {
        List<String> vars;
        List<List<String>> rows;
        @Nullable String error;
        @Nullable List<List<String>> activeBindings;
        @Nullable List<String> bindRequests;
        @Nullable List<Integer> actionQueues;

        public ParseResult(List<String> vars, List<List<String>> rows, @Nullable String error, @Nullable List<List<String>> activeBindings, @Nullable List<String> bindRequests, @Nullable List<Integer> actionQueues) {
            this.vars = vars;
            this.rows = rows;
            this.error = error;
            this.activeBindings = activeBindings;
            this.bindRequests = bindRequests;
            this.actionQueues = actionQueues;
        }

        public ParseResult(List<String> vars, List<List<String>> rows) {
            this(vars, rows, null, null, null, null);
        }

        ParseResult error(@Nullable String value) { error = value; return this; }
        ParseResult activeBindings(@Nullable List<List<String>> value) { activeBindings = value; return this; }
        ParseResult bindRequests(@Nullable List<String> value) { bindRequests = value; return this; }

        public ParseResult copy() {
            return new ParseResult(new ArrayList<>(vars),
                    rows.stream().map(ArrayList::new).collect(Collectors.toList()),
                    error,
                    activeBindings == null ? null : activeBindings.stream().map(ArrayList::new).collect(Collectors.toList()),
                    bindRequests == null ? null : new ArrayList<>(bindRequests),
                    actionQueues == null ? null : new ArrayList<>(actionQueues));
        }
    }

    private static class TestConsumer implements WebSocketResultsParserConsumer {
        private final List<String> vars = new ArrayList<>();
        private final List<List<String>> rows = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();
        private int endCount = 0;
        @SuppressWarnings("unused") private int cancelledCount = 0;
        private final List<List<String>> activeBindings = new ArrayList<>();
        private final List<String> bindRequests = new ArrayList<>();
        private final List<Integer> actionQueues = new ArrayList<>();

        @Override public void bindRequest(long n, boolean incremental) {
            bindRequests.add((incremental ? "+" : "") + n);
        }
        @Override public void activeBinding(String[] row) { activeBindings.add(asList(row)); }
        @Override public void actionQueue(int n)          { actionQueues.add(n); }
        @Override public void vars(List<String> vars)     { this.vars.addAll(vars); }
        @Override public void row(@Nullable String[] row) { this.rows.add(asList(row)); }
        @Override public void end()                       { this.endCount++; }
        @Override public void cancelled()                 { this.cancelledCount++; }
        @Override public void onError(String message)     { this.errors.add(message); }

        public void check(ParseResult expected) {
            assertEquals(expected.vars, vars);
            assertEquals(expected.rows, rows);
            if (expected.error == null)
                assertEquals(emptyList(), errors);
            else
                assertEquals(Collections.singletonList(expected.error), errors);
            assertEquals(1, endCount);
            if (expected.activeBindings != null)
                assertEquals(expected.activeBindings, activeBindings);
            if (expected.bindRequests != null)
                assertEquals(expected.bindRequests, bindRequests);
            if (expected.actionQueues != null)
                assertEquals(expected.actionQueues, actionQueues);
        }
    }

    static Stream<Arguments> parseData() {
        final class D {
            final String in;
            final ParseResult ex;

            public D(String in, ParseResult ex) { this.in = in; this.ex = ex; }
        }
        List<D> base = new ArrayList<>(asList(
        /*  1 */new D("", new ParseResult(emptyList(), emptyList())),
        /*  2 */new D("?x\n", new ParseResult(singletonList("x"), emptyList())),
        /*  3 */new D("?x", new ParseResult(singletonList("x"), emptyList())),
        /*  4 */new D("?x\t?y1\n", new ParseResult(asList("x", "y1"), emptyList())),
        /*  5 */new D("?x\t?y1", new ParseResult(asList("x", "y1"), emptyList())),
        /*  6 */new D("?x\n<a>\n", new ParseResult(singletonList("x"),
                        singletonList(singletonList("<a>")))),
        /*  7 */new D("?x\n<a>\n", new ParseResult(singletonList("x"),
                        singletonList(singletonList("<a>")))),
        /*  8 */new D("?x\t?y\n<a>\t\"!x\"\n", new ParseResult(asList("x", "y"),
                        singletonList(asList("<a>", "\"!x\"")))),
        /*  9 */new D("?x\t?y\n<a>\t\"!x\"\n\t<b>", new ParseResult(asList("x", "y"),
                        asList(asList("<a>", "\"!x\""), asList(null, "<b>")))),
        /* 10 */new D("?x\n!bind-request 2\n!bind-request +1\n",
                        new ParseResult(singletonList("x"), emptyList())
                                .bindRequests(asList("2", "+1"))),
        /* 11 */new D("?x\n!bind-request 2\n!active-binding <a>\t\n<x>\n" +
                        "!active-binding \t<b>\n<y>\n",
                        new ParseResult(singletonList("x"),
                                        asList(singletonList("<x>"), singletonList("<y>")))
                                .bindRequests(singletonList("2"))
                                .activeBindings(asList(asList("<a>", null),
                                                       asList(null, "<b>")))),
        /* 12 */new D("?x\n!active-binding \n<a>\n",
                        new ParseResult(singletonList("x"), singletonList(singletonList("<a>")))
                                .activeBindings(singletonList(singletonList(null))))
        ));
        int baseRows = base.size();
        // add harmless !end
        for (int i = 0; i < baseRows; i++) {
            String in = base.get(i).in;
            base.add(new D(in + (in.endsWith("\n") ? "" : "\n") + "!end", base.get(i).ex));
        }
        // start with error
        for (int i = 0; i < baseRows; i++) {
            D d = base.get(i);
            String in = "!error test induced\n" + d.in;
            base.add(new D(in, new ParseResult(emptyList(), emptyList()).error("test induced")));
        }
        // end with error
        for (int i = 0; i < baseRows; i++) {
            D d = base.get(i);
            String in = d.in + (d.in.endsWith("\n") ? "" : "\n") + "!error test induced";
            base.add(new D(in, d.ex.copy().error("test induced")));
        }
        // ignore error after !end
        for (int i = 0; i < baseRows; i++) {
            D d = base.get(i);
            String in = d.in + (d.in.endsWith("\n") ? "" : "\n") + "!end\n!error ignored\n";
            base.add(new D(in, d.ex));
        }
        base.addAll(asList(
                new D("?x\n!error test\n",
                      new ParseResult(singletonList("x"), emptyList()).error("test")),
                new D("?x\n!error test",
                        new ParseResult(singletonList("x"), emptyList()).error("test")),
                new D("?x\n!error test\n<ignored>",
                        new ParseResult(singletonList("x"), emptyList()).error("test")),
                new D("?x\n<a>\n!error test\n<a>",
                        new ParseResult(singletonList("x"),
                                        singletonList(singletonList("<a>"))).error("test")),
                new D("?x\n<a>\n<b>\n!error test\n<a>",
                        new ParseResult(singletonList("x"),
                                asList(singletonList("<a>"), singletonList("<b>"))).error("test"))
        ));

        List<Arguments> list = new ArrayList<>();
        List<Function<String, CharSequence>> mappers = asList(s -> s, StringBuilder::new);
        for (Function<String, CharSequence> mapper : mappers) {
            for (D d : base) {
                list.add(arguments(mapper, d.in, d.ex));
            }
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource("parseData")
    void testParseCharByChar(Function<String, CharSequence> typeChanger,
                             String in, ParseResult expected) {
        TestConsumer consumer = new TestConsumer();
        WebSocketResultsParser parser = new WebSocketResultsParser(consumer);
        for (int i = 0, len = in.length(); i < len; i++)
            parser.feed(typeChanger.apply(in.substring(i, i+1)));
        parser.end();
        consumer.check(expected);
    }

    @ParameterizedTest @MethodSource("parseData")
    void testParseLineByLine(Function<String, CharSequence> typeChanger,
                             String in, ParseResult expected) {
        TestConsumer consumer = new TestConsumer();
        WebSocketResultsParser parser = new WebSocketResultsParser(consumer);
        for (int i = 0, len = in.length(), eol; i < len; i = eol+1) {
            eol = CSUtils.skipUntil(in, i, '\n');
            parser.feed(typeChanger.apply(in.substring(i, Math.min(len, eol+1))));
            parser.endMessage();
        }
        parser.end();
        consumer.check(expected);
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void testFragmentEndInMessage(boolean callEndMessage) {
        TestConsumer consumer = new TestConsumer();
        WebSocketResultsParser parser = new WebSocketResultsParser(consumer);
        parser.feed("?x\n<a>\n!en");
        parser.feed("d\n");
        if (callEndMessage)
            parser.endMessage();
        parser.end();
        consumer.check(new ParseResult(singletonList("x"),
                                       singletonList(singletonList("<a>"))));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void testFragmentErrorInMessage(boolean addNewline) {
        TestConsumer consumer = new TestConsumer();
        WebSocketResultsParser parser = new WebSocketResultsParser(consumer);
        parser.feed("?x\n<a");
        parser.feed(">\n");
        parser.endMessage();
        parser.feed("<b>\n!");
        parser.feed("error ");
        parser.feed("test induced");
        if (addNewline)
            parser.feed("\n");
        parser.endMessage();
        consumer.check(new ParseResult(singletonList("x"),
                                       asList(singletonList("<a>"), singletonList("<b>"))
                       ).error("test induced"));
    }
}