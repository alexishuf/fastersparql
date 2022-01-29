package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.util.CSUtils;

import java.util.Map;

import static com.github.alexishuf.fastersparql.client.util.CSUtils.findNotEscaped;
import static com.github.alexishuf.fastersparql.client.util.CSUtils.skipUntilIn;

/**
 * No-dependencies binder for SPARQL
 *
 * This will not fully parse the SPARQL query, thus if an invalid query is given,
 * and even more invalid query can be output, creating confusion.
 */
public class SparqlBind {
    /**
     * Returns a copy {@code input} with variables replaced by the given values
     *
     * @param input the object (e.g., a SPARQL query) to be bound
     * @param varName2ntTerm a map from var names to RDF terms in N-Triples syntax.
     * @return a new copy of {@code input} with variables replaced or {@code input} itself if the
     *         bind operation is a no-op (no variable in {@code varName2ntTerm} appears in
     *         {@code input}.
     */
    public static CharSequence bind(CharSequence input, Map<String, String> varName2ntTerm) {
        BindHelpers.checkBind(varName2ntTerm);
        if (varName2ntTerm == null || varName2ntTerm.isEmpty()) return input.toString();
        boolean change = false;
        StringBuilder builder = new StringBuilder();
        for (int consumed = 0, len = input.length(); consumed < input.length(); ) {
            int varBegin = nextVar(input, consumed, len);
            builder.append(input, consumed, varBegin);
            if (varBegin < len) {
                int varEnd = varEnd(input, varBegin, len);
                if (varEnd < len) {
                    String varName = input.subSequence(varBegin + 1, varEnd).toString();
                    String value = varName2ntTerm.getOrDefault(varName, null);
                    if (value != null) {
                        change = true;
                        builder.append(value);
                    } else {
                        builder.append(input, varBegin, varEnd);
                    }
                }
                consumed = varEnd;
            }
        }
        return change ? builder.toString() : input;
    }

    private static final char[] FIRST = "\"#$'<?".toCharArray();

    static int nextVar(CharSequence cs, int begin, int end) {
        for (int i = begin; i < end; i++) {
            i = skipUntilIn(cs, i, end, FIRST);
            char c = i < end ? cs.charAt(i) : '\0';
            if (c == '"' || c == '\'') {
                i = stringEnd(cs, i, end);
            } else if (c == '<') {
                i = CSUtils.skipUntilIn(cs, i + 1, end, '>');
            } else if (c == '#') {
                i = skipUntilIn(cs, i + 1, end, '\n');
            } else {
                return i;
            }
        }
        return end;
    }

    private static boolean isLongQuote(CharSequence cs, int i, int end) {
        char c0 = cs.charAt(i);
        return i+2 < end && cs.charAt(i+1) == c0 && cs.charAt(i+2) == c0;
    }

    static int stringEnd(CharSequence cs, int i, int end) {
        if (i >= end)
            return end;
        char c0 = cs.charAt(i);
        if (isLongQuote(cs, i, end)) {
            int j = findNotEscaped(cs, i+3, c0);
            while (j < end && !isLongQuote(cs, j, end))
                j = findNotEscaped(cs, j+1, c0);
            return Math.min(end, j+3);
        } else {
            return Math.min(end, findNotEscaped(cs, i+1, c0)+1);
        }
    }

    static int varEnd(CharSequence cs, int begin, int end) {
        for (int i = begin; i < end; i++) {
            char c = cs.charAt(i);
            if (c < 128) {
                if (c < '0' || (c > '9' && c < 'A') || (c > 'Z' && c < '_') || c == '`' || c > 'z')
                    return i;
            }
        }
        return end;
    }
}
