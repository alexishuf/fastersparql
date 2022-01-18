package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class TSVParser implements ResultsParser {
    private final ResultsParserConsumer consumer;
    private boolean headersDone;
    private final List<String> current = new ArrayList<>();
    private final StringBuilder carry = new StringBuilder();
    private int nColumns = -1, lineNumber = 1;
    private boolean ended, hadError;

    public TSVParser(ResultsParserConsumer consumer) { this.consumer = consumer; }

    @Override public void feed(CharSequence input) {
        if (hadError)
            return;
        if (ended)
            throw new IllegalStateException("feed() after end()");
        if (carry.length() > 0) {
            int capacity = carry.length() + input.length();
            input = new StringBuilder(capacity).append(carry).append(input);
            carry.setLength(0);
        }
        for (int i = 0, end, len = input.length(); !hadError && i < len; i = end+1) {
            end = CSUtils.skipUntil(input, i, '\n', '\t');
            if (end == len) {
                carry.append(input, i, input.length());
            } else {
                current.add(i == end ? null : input.subSequence(i, end).toString());
                if (input.charAt(end) == '\n')
                    emit();
            }
        }
    }

    @Override public void end() {
        boolean valid = !ended && !hadError;
        try {
            if (valid) {
                if (carry.length() > 0)
                    feed("\n");
                if (!hadError) { // feed may call consumer.error() and consumer.end()
                    if (!headersDone) {
                        consumer.vars(emptyList());
                        headersDone = true;
                    }
                    consumer.end();
                }
            }
        } finally {
            ended = true;
        }
    }

    private void emit() {
        assert !current.isEmpty();
        assert current.stream().filter(Objects::nonNull).noneMatch(String::isEmpty);
        if (!headersDone) {
            ArrayList<String> trimmed = new ArrayList<>(current.size());
            if (current.size() > 1 || current.get(0) != null) {
                for (String v : current) {
                    char first = v == null ? '\0' : v.charAt(0);
                    trimmed.add(v == null ? "" : (first == '?' || first == '$' ? v.substring(1) : v));
                }
            } // else: zero-vars header (ASK query)
            nColumns = trimmed.size();
            headersDone = true;
            consumer.vars(trimmed);
        } else if (nColumns == 0) {
            if (current.size() > 1 || current.get(0) != null) {
                error("Unexpected non-empty row "+current+" for zero-vars results");
            } else {
                consumer.row(new String[0]);
            }
        } else {
            if (current.size() == nColumns) {
                consumer.row(current.toArray(new String[0]));
            } else if (current.size() < nColumns) {
                error("Missing columns on row "+current+" expected "+nColumns+" columns. " +
                      "This is often caused by an unescaped line feed (\\n).");
            } else {
                error("Excess columns on row "+current+" expected "+nColumns+" columns. "+
                      "This is often caused by an unescaped tab (\\t).");
            }
        }
        current.clear();
        ++lineNumber;
    }

    private void error(String message) {
        if (hadError || ended)
            return;
        hadError = true;
        consumer.onError(message);
        consumer.end();
    }


}
