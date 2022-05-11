package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.client.util.CSUtils;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TSVParser extends AbstractSVResultsParser {
    private static final Logger log = LoggerFactory.getLogger(TSVParser.class);
    private static final char[] FORBIDDEN_CHARS = "\b\t\n\f\r\"\\".toCharArray();
    private static final char[] ESCAPE_CHARS = "btnfr\"\\".toCharArray();
    private static final char[] NT_FIRST = "\0\"<_".toCharArray();

    private final StringBuilder tmp = new StringBuilder();

    public static class Provider implements ResultsParserProvider {
        @Override public List<MediaType> mediaTypes() {
            return Arrays.asList(SparqlResultFormat.TSV.asMediaType(),
                                 new MediaType("text", "tsv"));
        }

        @Override public ResultsParser create(ResultsParserConsumer consumer) {
            return new TSVParser(consumer);
        }
    }

    public TSVParser(ResultsParserConsumer consumer) {
        super(consumer, "\n");
    }

    @Override protected int readTerm(CharSequence input, int begin, int end) throws SyntaxException {
        int i = CSUtils.skipUntilIn(input, begin, end, '\n', '\t');
        if (i == end) {
            carry(input, begin);
        } else {
            boolean isEOL = input.charAt(i) == '\n';
            char first = begin == i ? '\0' : input.charAt(begin);
            if (CSUtils.charInSorted(first, NT_FIRST) || atHeaders())
                addTerm(input, begin, i, isEOL);
            else
                addAsNt(input, begin, i, isEOL);
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
