package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.client.util.CSUtils;
import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.Collections;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.CSUtils.skipUntil;

public class CSVParser extends AbstractSVResultsParser {
    private final StringBuilder unquoteBuilder = new StringBuilder();
    private final StringBuilder ntBuilder = new StringBuilder();

    public static class Provider implements ResultsParserProvider {
        @Override public List<MediaType> mediaTypes() {
            return Collections.singletonList(SparqlResultFormat.CSV.asMediaType());
        }

        @Override public ResultsParser create(ResultsParserConsumer consumer) {
            return new CSVParser(consumer);
        }
    }

    public CSVParser(ResultsParserConsumer consumer) {
        super(consumer, "\r\n");
    }

    private static final char[] END_TERMS = {'\r', '"', ','};

    @Override protected int readTerm(CharSequence input, int from) throws SyntaxException {
        int len = input.length();
        while (true) {
            int first = skipUntil(input, from, END_TERMS);
            if (first == len) {
                return carry(input, from);
            } else if (input.charAt(first) == '"') {
                int closeQuote = findCloseQuote(input, first+1);
                if (closeQuote == len)
                    return carry(input, from);
                int end = findSep(input, closeQuote+1);
                if (end == len)
                    return carry(input, from);
                boolean isEOL = input.charAt(end) == '\r';
                CharSequence unquoted = unquote(input, first+1, closeQuote);
                CharSequence nt = toNt(unquoted, 0, unquoted.length());
                addTerm(nt, 0, nt.length(), isEOL);
                return end + (isEOL ? 2 : 1);
            } else if (input.charAt(first) == ',') {
                return trimAndAdd(input, from, first, false)+1;
            } else {
                assert input.charAt(first) == '\r';
                if (first+1 >= len)
                    return carry(input, from);
                else if (input.charAt(first+1) == '\n')
                    return trimAndAdd(input, from, first, true)+2;
                // else try a new first
            }
        }
    }

    private int trimAndAdd(CharSequence input, int begin, int end, boolean isEOL) {
        int givenEnd = end;
        if (begin == end) {
            addTerm(input, begin, end, isEOL);
        } else {
            begin = CSUtils.skipSpaceAnd(input, begin, end, '\0');
            end = CSUtils.reverseSkipSpaceAnd(input, begin, end, '\0');
            CharSequence nt = toNt(input, begin, end);
            addTerm(nt, 0, nt.length(), isEOL);
        }
        return givenEnd;
    }

    static int findCloseQuote(CharSequence input, int from) {
        int end = input.length(), i = from;
        while (i < end) {
            i = skipUntil(input, i, '"');
            if (i > from && input.charAt(i-1) == '\\')
                i += 1;
            else if (i+1 < end  && input.charAt(i+1) == '"')
                i += 2;
            else
                return i;
        }
        return end;
    }

    static int findSep(CharSequence cs, int from) throws SyntaxException {
        int end = cs.length();
        for (int i = from; i < end; i++) {
            char c = cs.charAt(i);
            if (c == ',') {
                return i;
            } else if (c == '\r') {
                if (i+1 >= end) {
                    return end;
                } else {
                    char c1 = cs.charAt(i + 1);
                    if (c1 == '\n') {
                        return i;
                    } else if (c1 < '\t' || (c1 > '\r' && c1 != ' ' && c1 != ',')) {
                        String msg = "Unexpected '" + c1 + "', expected whitespace or ','.";
                        throw new SyntaxException(i+1, msg);
                    }
                }
            } else if (c < '\t' || (c > '\r' && c != ' ')) {
                throw new SyntaxException(i, "Unexpected '" + c + "', expected whitespace or ','.");
            }
        }
        return end;
    }

    private static final char[] NEEDS_ESCAPE = {'\t', '\n', '\r', '"', '\\'};

    CharSequence unquote(CharSequence input, int begin, int end) throws SyntaxException {
        unquoteBuilder.setLength(0);
        for (int last = begin, i; last < end; last = i+1) {
            i = CSUtils.skipUntilIn(input, last, end, NEEDS_ESCAPE);
            unquoteBuilder.append(input, last, i);
            if (i == end)
                return unquoteBuilder;
            unquoteBuilder.append('\\');
            char c = input.charAt(i);
            if (c == '"') {
                if (i + 1 >= end || input.charAt(i + 1) != '"')
                    throw new SyntaxException(i, "\" is not followed by \" nor preceded by \\");
                ++i; // consume " at i+1
                unquoteBuilder.append('"');
            } else if (c == '\\') {
                char c1 = i+1 < end ? input.charAt(i+1) : '\0';
                char c2 = i+2 < end ? input.charAt(i+2) : '\0';
                if (c1 == '"' && c2 != '"') {
                    unquoteBuilder.append('"');
                    ++i; //consume " at i+1
                } else {
                    unquoteBuilder.append('\\');
                }
            } else {
                unquoteBuilder.append(c == '\t' ? 't' : (c == '\n' ? 'n' : 'r'));
            }
        }
        return unquoteBuilder;
    }

    CharSequence toNt(CharSequence cs, int begin, int end) {
        ntBuilder.setLength(0);
        int size = end - begin;
        ntBuilder.ensureCapacity(size +2);
        if (atHeaders())
            return cs.subSequence(begin, end);
        if (size >= 7) {
            boolean isHTTP = CSUtils.startsWith(cs, begin, end, "http://")
                    || CSUtils.startsWith(cs, begin, end, "https://");
            if (isHTTP)
                return ntBuilder.append('<').append(cs, begin, end).append('>').toString();
        }
        if (CSUtils.startsWith(cs, begin, end, "_:"))
            return cs.subSequence(begin, end);
        return ntBuilder.append('"').append(cs, begin, end).append('"');
    }
}
