package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.alexishuf.fastersparql.client.util.Skip.skipUntil;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WebSocketResultsParserTest {

    private static class ParseResult {
        final List<String> vars;
        final List<List<String>> rows;
        @Nullable String error;
        @Nullable List<List<String>> activeBindings;
        @Nullable List<String> bindRequests;
        @Nullable final List<Integer> actionQueues;

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
        @SuppressWarnings("unused") private int cancelledCount = 0, pingCount, pingAckCount;
        private final List<List<String>> activeBindings = new ArrayList<>();
        private final List<String> bindRequests = new ArrayList<>();
        private final List<Integer> actionQueues = new ArrayList<>();

        @Override public void bindRequest(long n, boolean incremental) {
            bindRequests.add((incremental ? "+" : "") + n);
        }
        @Override public void activeBinding(String[] row) { activeBindings.add(asList(row)); }
        @Override public void actionQueue(int n)          { actionQueues.add(n); }
        @Override public void vars(Vars vars)             { this.vars.addAll(vars); }
        @Override public void row(@Nullable String[] row) { this.rows.add(asList(row)); }
        @Override public void end()                       { this.endCount++; }
        @Override public void cancelled()                 { this.cancelledCount++; }
        @Override public void onError(String message)     { this.errors.add(message); }
        @Override public void ping()                      { this.pingCount++; }
        @Override public void pingAck()                   { this.pingAckCount++; }

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

    interface StringMapper {  CharSequence map(String s); }
    private static final StringMapper NOOP_MAPPER = new StringMapper() {
        @Override public CharSequence map(String s) { return s; }
        @Override public String toString() { return "NOOP"; }
    };
    private static final StringMapper BUILDER_MAPPER = new StringMapper() {
        @Override public CharSequence map(String s) { return new StringBuilder(s); }
        @Override public String toString() { return "BUILDER"; }
    };

    record D(String in, ParseResult ex, StringMapper mapper) {
        public D(String in, ParseResult ex) { this(in, ex, NOOP_MAPPER); }
    }
    static List<D> parseData() {

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
        /* 11 */new D("""
                        ?x
                        !bind-request 2
                        !active-binding <a>\t
                        <x>
                        !active-binding \t<b>
                        <y>
                        """,
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

        List<D> withMapper = new ArrayList<>(base);
        base.forEach(d -> withMapper.add(new D(d.in, d.ex, BUILDER_MAPPER)));
        return withMapper;
    }

    @Test
    void testParseCharByChar() {
        for (D d : parseData()) {
            TestConsumer consumer = new TestConsumer();
            WebSocketResultsParser parser = new WebSocketResultsParser(consumer);
            for (int i = 0, len = d.in.length(); i < len; i++)
                parser.feed(d.mapper.map(d.in.substring(i, i+1)));
            parser.end();
            consumer.check(d.ex);
        }
    }

    @Test
    void testParseLineByLine() {
        for (D d : parseData()) {
            TestConsumer consumer = new TestConsumer();
            WebSocketResultsParser parser = new WebSocketResultsParser(consumer);
            for (int i = 0, len = d.in.length(), eol; i < len; i = eol + 1) {
                eol = skipUntil(d.in, i, d.in.length(), '\n');
                parser.feed(d.mapper.map(d.in.substring(i, Math.min(len, eol + 1))));
                parser.endMessage();
            }
            parser.end();
            consumer.check(d.ex);
        }
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