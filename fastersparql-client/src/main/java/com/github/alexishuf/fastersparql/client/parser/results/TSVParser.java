package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.UUID;

@Slf4j
public class TSVParser extends AbstractSVResultsParser {
    private static final char[] FORBIDDEN_CHARS = "\b\t\n\f\r\"\\".toCharArray();
    private static final char[] ESCAPE_CHARS = "btnfr\"\\".toCharArray();
    private static final char[] NT_FIRST = "\0\"<_".toCharArray();

    private final StringBuilder tmp = new StringBuilder();

    public TSVParser(ResultsParserConsumer consumer) {
        super(consumer, "\n");
    }

    @Override protected int readTerm(CharSequence input, int from) throws SyntaxException {
        int i = CSUtils.skipUntil(input, from, '\n', '\t');
        if (i == input.length()) {
            carry(input, from);
        } else {
            boolean isEOL = input.charAt(i) == '\n';
            char first = from == i ? '\0' : input.charAt(from);
            if (CSUtils.charInSorted(first, NT_FIRST) || atHeaders())
                addTerm(input, from, i, isEOL);
            else
                addAsNt(input, from, i, isEOL);
        }
        return i+1;
    }

    private void addAsNt(CharSequence input, int begin, int end,
                         boolean isEOL) throws SyntaxException {
        char first = input.charAt(begin);
        tmp.setLength(0);
        if (    (first == 't' && CSUtils.startsWith(input, begin, end, "true" )) ||
                (first == 'f' && CSUtils.startsWith(input, begin, end, "false")) ) {
            tmp.append('"').append(input, begin, end)
                    .append("\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
        } else if (first == '-' || first == '+' || first == '.' || (first > '0' && first < '9')) {
            boolean isDouble = false;
            boolean isDecimal = false;
            for (int i = begin+1; i < end; i++) {
                char c = input.charAt(i);
                boolean bad = false;
                if (c > '9') {
                    bad = c != 'e' && c != 'E';
                    isDouble = true;
                } else if (c < '0') {
                    if (c == '.') isDecimal = true;
                    else          bad = c != '+' && c != '-';
                }
                if (bad) {
                    throw new SyntaxException(i, input.subSequence(begin, end)+" looks like a " +
                            "TTL numeric literal, but '"+c+"' is not allowed.");
                }
            }
            tmp.append('"').append(input, begin, end)
                    .append("\"^^<http://www.w3.org/2001/XMLSchema#")
                    .append(isDouble ? "double>" : isDecimal ? "decimal>" : "integer>");
        } else if (first == '[' && input.charAt(end-1) == ']'){
            tmp.append("_:").append(UUID.randomUUID());
        } else {
            log.warn("Term {} in TSV is not valid, will wrap in a plain string",
                     input.subSequence(begin, end));
            tmp.append('"');
            for (int i = begin, last = begin; i < end; last = i+1) {
                i = CSUtils.skipUntilIn(input, last, end, FORBIDDEN_CHARS);
                tmp.append(input, last, i);
                char c = i == end ? '\0' : input.charAt(i);
                if (c == '"') {
                    tmp.append('\\').append('"');
                } else if (c == '\\') {
                    char next = i+1 >= end ? '\0' : input.charAt(i+1);
                    if (Arrays.binarySearch(ESCAPE_CHARS, next) >= 0) {
                        tmp.append('\\').append(next);
                        ++i; // valid escape, consume it
                    } else {
                        tmp.append('\\').append('\\');
                    }
                }
            }
            tmp.append('"');
        }
        addTerm(tmp, 0, tmp.length(), isEOL);
    }
}
