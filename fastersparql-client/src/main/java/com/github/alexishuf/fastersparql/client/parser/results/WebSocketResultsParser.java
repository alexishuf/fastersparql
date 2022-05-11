package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parser for TSV results and {@code !}control messages as used in hdtss WebSocket protocol
 */
@Slf4j
public class WebSocketResultsParser implements ResultsParser {
    private static final Pattern QUEUE_CAP_RX = Pattern.compile("!action-queue-cap\\s*=?\\s*(\\d+)");
    private static final Pattern BIND_REQUEST_RX = Pattern.compile("!bind-request\\s*(\\+?)\\s*(\\d+)");

    private final TSVParser tsvParser;
    private final WebSocketResultsParserConsumer consumer;
    private StringBuilder lineBuffer1 = new StringBuilder(), lineBuffer2 = new StringBuilder();
    private final List<String> bindingTerms = new ArrayList<>();
    private boolean ended;

    public WebSocketResultsParser(WebSocketResultsParserConsumer consumer) {
        tsvParser = new TSVParser(this.consumer = consumer);
    }

    @Override public void feed(CharSequence input) {
        if (lineBuffer1.length() > 0) {
            lineBuffer1.append(input);
            if (CSUtils.skipUntil(input, 0, '\n') < input.length()) {
                input = lineBuffer1; // use buffer as input
                (lineBuffer1 = lineBuffer2).setLength(0); // swap buffers and clear the new one
                lineBuffer2 = (StringBuilder) input; // store previous buffer to avoid collection
            } else {
                return;
            }
        }
        for (int i = 0, len = input.length(), eol; i < len; i = eol) {
            eol = Math.min(len, CSUtils.skipUntil(input, i, '\n')+1);
            if (input.charAt(eol-1) == '\n') {
                if (ended)
                    log.warn("Ignoring line after !error/!end: {}", input.subSequence(i, eol-1));
                else if (input.charAt(i) == '!')
                    readControl(input, i, eol);
                else
                    tsvParser.feed(input, i, eol);
            } else {
                lineBuffer1.append(input, i, eol);
            }
        }
    }

    /**
     * Indicates that a message end has been reached. Like {@link ResultsParser#end()} this
     * forces incomplete rows (and control lines) to complete even if no trailing newline
     * ({@code '\n'}) was included. However, unlike {@link ResultsParser#end()}, this will not call
     * {@link ResultsParserConsumer#end()}.
     */
    public void endMessage() {
        if (lineBuffer1.length() > 0) {
            boolean hasNewline = lineBuffer1.charAt(lineBuffer1.length() - 1) == '\n';
            feed(hasNewline ? "" : "\n");
        }
    }

    @Override public void end() {
        endMessage();
        if (!ended) {
            ended = true;
            tsvParser.end();
        }
    }

    static int skipUntilControl(CharSequence input, int begin) {
        int i = begin, len = input.length();
        for (int eol; i < len && input.charAt(i) != '!' ; i = eol+1)
            eol = CSUtils.skipUntilIn(input, i, len, '\n');
        return Math.min(i, len);
    }

    private void readControl(CharSequence input, int begin, int end) {
        if (input.charAt(Math.max(begin, end-1)) == '\n')
            end--; // ignore trailing \n
        if (ended) {
            CharSequence msg = input.subSequence(begin, end);
            log.warn("Ignoring control message after !error/!end: {}", msg);
        } else if (end <= begin) {
            assert false : "empty control message";
            consumer.onError("Empty control message");
            end();
        } else if (CSUtils.startsWith(input, begin, end, "!error ")) {
            if (end-begin <= 7)
                consumer.onError(input.subSequence(begin, end).toString());
            else
                consumer.onError(input.subSequence(begin+7, end).toString());
            end();
        } else if (CSUtils.startsWith(input, begin, end, "!end")) {
            end();
        } else if (CSUtils.startsWith(input, begin, end, "!action-queue-cap")) {
            Matcher m = QUEUE_CAP_RX.matcher(input);
            if (!m.find(begin) || m.start() != begin)
                log.warn("Ignoring malformed control line {}", input.subSequence(begin, end));
            else
                consumer.actionQueue((int)Math.min(Long.parseLong(m.group(1)), Integer.MAX_VALUE));
        } else if (CSUtils.startsWith(input, begin, end, "!bind-request")) {
            Matcher m = BIND_REQUEST_RX.matcher(input);
            if (!m.find(begin) || m.start() != begin)
                log.warn("Ignoring malformed control line {}", input.subSequence(begin, end));
            else
                consumer.bindRequest(Long.parseLong(m.group(2)), !m.group(1).isEmpty());
        } else if (CSUtils.startsWith(input, begin, end, "!active-binding ")) {
            assert bindingTerms.isEmpty() : "Concurrent readControl";
            String string = input.toString();
            for (int i = begin+16, eot; i < end; i = eot+1) {
                eot = CSUtils.skipUntilIn(input, i, end, '\t');
                bindingTerms.add(eot > i ? string.substring(i, eot) : null);
            }
            if (input.charAt(end-1) == '\t')
                bindingTerms.add(null);
            else if (bindingTerms.isEmpty())
                bindingTerms.add(null);
            consumer.activeBinding(bindingTerms.toArray(new String[0]));
            bindingTerms.clear();
        } else if (input.charAt(begin) == '!') {
            consumer.onError("Unexpected control message: "+ input.subSequence(begin, end));
        }
    }
}
