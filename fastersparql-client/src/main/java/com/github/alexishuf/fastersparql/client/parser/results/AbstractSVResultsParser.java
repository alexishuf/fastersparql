package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public abstract class AbstractSVResultsParser implements ResultsParser {
    private final ResultsParserConsumer consumer;
    private final String eol;
    private boolean headersDone;
    private final List<String> current = new ArrayList<>();
    private final StringBuilder carry = new StringBuilder();
    private int nColumns = -1, lineNumber = 1, columnNumber = 1;
    private boolean ended, hadError;

    protected AbstractSVResultsParser(ResultsParserConsumer consumer, String eol) {
        this.consumer = consumer;
        this.eol = eol;
    }

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
        for (int i = 0, len = input.length(); !hadError && i < len; ) {
            try {
                i = readTerm(input, i);
                columnNumber = columnNumber == -1 ? 1 : columnNumber+i;
            } catch (SyntaxException e) {
                error("Column "+(columnNumber+e.inputPos-i)+": "+e.getMessage());
            } catch (Throwable t) {
                error(t.toString());
            }
        }
    }

    protected boolean atHeaders() { return !headersDone; }

    protected int carry(CharSequence input, int from) {
        int length = input.length();
        carry.append(input, from, length);
        return length;
    }

    protected void addTerm(CharSequence cs, int begin, int end, boolean isEOL) {
        if (begin != end) {
            begin = CSUtils.skipSpaceAnd(cs, begin, end, '\0');
            end = CSUtils.reverseSkipSpaceAnd(cs, begin, end, '\0');
        }
        current.add(begin == end ? null : cs.subSequence(begin, end).toString());
        if (isEOL)
            emit();
    }

    /**
     * Try to parse a term from the input starting at {@code from}.
     *
     * This method MUST call either method:
     * <ul>
     *     <li>{@link AbstractSVResultsParser#addTerm(CharSequence, int, int, boolean)}</li>
     *     <li>{@link AbstractSVResultsParser#carry(CharSequence, int)} </li>
     * </ul>
     *
     * @return the index in {@code input} from where a subsequent {@code readTerm} shall start.
     */
    protected abstract int readTerm(CharSequence input, int from) throws SyntaxException;

    public static class SyntaxException extends Exception {
        final int inputPos;
        public SyntaxException(int inputPos, String message) {
            super(message);
            this.inputPos = inputPos;
        }
    }

    @Override public void end() {
        boolean valid = !ended && !hadError;
        try {
            if (valid) {
                if (carry.length() > 0) {
                    feed(eol);
                    if (carry.length() > 0)
                        error("Unterminated term (no closing \"?)");
                }
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
        columnNumber = -1;
    }

    private void error(String message) {
        if (hadError || ended)
            return;
        hadError = true;
        consumer.onError("Line "+lineNumber+": "+message);
        consumer.end();
    }
}