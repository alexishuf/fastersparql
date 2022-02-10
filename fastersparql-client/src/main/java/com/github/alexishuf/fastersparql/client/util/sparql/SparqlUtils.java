package com.github.alexishuf.fastersparql.client.util.sparql;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;

import java.util.*;

import static com.github.alexishuf.fastersparql.client.util.CSUtils.*;
import static java.util.Collections.emptyList;

/**
 * No-dependencies binder for SPARQL
 *
 * This will not fully parse the SPARQL query, thus if an invalid query is given,
 * and even more invalid query can be output, creating confusion.
 */
public class SparqlUtils {
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
        VarUtils.checkBind(varName2ntTerm);
        if (varName2ntTerm == null || varName2ntTerm.isEmpty())
            return input.toString();
        boolean change = false;
        StringBuilder builder = new StringBuilder();
        @NonNegative int consumed = 0, openBody = 0;

        // check if this bind will transform a SELECT into an ASK
        // this is necessary as "SELECT WHERE" is invalid syntax and must thus become "ASK"
        ProjectionInfo projection = findProjection(input);
        if (projection != null && !projection.isAsk) {
            boolean becomesAsk = true;
            for (int i = 0, size = projection.vars.size(); becomesAsk && i < size; ++i)
                becomesAsk = varName2ntTerm.containsKey(projection.vars.get(i));
            if (becomesAsk) {
                change = true;
                builder.append(input, 0, projection.begin).append("ASK {");
                consumed = openBody = projection.end+1;
            }
        }

        // if did not transform a SELECT into an ASK, find the body start
        if (openBody == 0)
            openBody = findBodyOpen(input);

        // scan for vars and replace them.
        for (int e, len = input.length(); consumed < input.length(); consumed = e) {
            int b = nextVar(input, consumed, len);
            builder.append(input, consumed, b);
            e = varEnd(input, b+1, len);
            if (e < len) {
                String varName = input.subSequence(b+1, e).toString();
                String value = varName2ntTerm.getOrDefault(varName, null);
                if (value != null) {
                    change = true;
                    if (b >= openBody)
                        builder.append(value);
                } else {
                    builder.append(input, b, e);
                }
            }
        }
        return change ? builder.toString() : input;
    }

    /**
     * Get set of non-null, non-empty variable names exposed by the SPARQL query (fragment).
     *
     * If sparql is a whole query with a SELECT clause with an explicit projection, the
     * return will be the variables in the projection, in the order they are listed.
     *
     * If {@code sparql} is an ASK query, then the return will be an empty list.
     *
     * In all other cases ({@code SELECT * WHERE} and query fragments, such as expressions),
     * the returned list will contain all mentioned variables in the order they appear within
     * {@code sparql}.
     *
     * @param sparql The SPARQL query or fragment. syntax correctedness of the query is not
     *               checked.
     * @return a non-null list of distinct non-null and non-empty variable names (the name does
     *         not include {@code ?} or {@code $} markers).
     */
    public static List<@MinLen(1) String> publicVars(CharSequence sparql) {
        ProjectionInfo info = findProjection(sparql);
        if (info != null) return info.vars;
        return allVars(sparql);
    }

    /**
     * Get a list of all variables used within the given sparql query, including those excluded
     * by a projection clause (non-* SELECT or ASK).
     *
     * If the given sparql is a whole SPARQL query with a projection clause, the projected
     * variables will appear first in the results, followed by internal variables excluded
     * by the projection.
     *
     * @param sparql a SPARQL query, a sparql fragment or a SPARQL expression.
     * @return a non-null (bu possibly empty) list of non-null, distinct variable names
     *         (i.e., not followed by {@code ?} or {@code $}).
     */
    public static List<@MinLen(1) String> allVars(CharSequence sparql) {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (int consumed = 0, len = sparql.length(), end; consumed < len; consumed = end) {
            int begin = nextVar(sparql, consumed, len);
            end = varEnd(sparql, begin+1, len);
            if (end > begin+1)
                set.add(sparql.subSequence(begin+1, end).toString());
        }
        if (set.isEmpty()) return emptyList();
        assert set.stream().noneMatch(String::isEmpty) : "empty var names, fix me!";
        return new ArrayList<>(set);
    }

    /* --- --- --- private helper methods --- --- --- */

    private static final char[] BODY_START_FIRST = "\"#'()<{".toCharArray();

    static int findBodyOpen(CharSequence sparql) {
        for (int cons = 0, len = sparql.length(), i, depth = 0; cons < len; cons = i+1) {
            i = skipUntil(sparql, cons, BODY_START_FIRST);
            char c = i < len ? sparql.charAt(i) : '\0';
            if (c == '\"' || c == '\'') {
                i = stringEnd(sparql, i, len);
            } else if (c == '#') {
                i = skipUntil(sparql, i, '\n');
            } else if (c == '(') {
                ++depth;
            } else if (c == ')') {
                depth = Math.max(0, depth-1);
            } else if (c == '<') {
                i = skipUntil(sparql, i, '>');
            } else if (depth == 0 && c == '{') {
                return i;
            }
        }
        return -1;
    }

    private static final char[] PROLOGUE_FIRST = "\"#$'(<?AS[as{".toCharArray();


    static class ProjectionInfo {
        /** Whether this is an ASK ({@code true}) or SELECT query ({@code false})  */
        boolean isAsk;
        /** Index where {@code SELECT} or {@code ASK} starts */
        int begin;
        /** Index where of the '{' ending the projection */
        int end;
        /** List of all var names exposed by the projection */
        List<@MinLen(1) String> vars;

        public ProjectionInfo(boolean isAsk, int begin, int end, List<String> vars) {
            this.isAsk = isAsk;
            this.begin = begin;
            this.end = end;
            this.vars = vars;
        }
    }

    static @Nullable ProjectionInfo findProjection(CharSequence sparql) {
        for (int consumed = 0, len = sparql.length(), i; consumed < len; consumed = ++i) {
            i = skipUntil(sparql, consumed, PROLOGUE_FIRST);
            char c = i < len ? sparql.charAt(i) : '\0';
            if (c == '<') {
                i = skipUntil(sparql, i+1, '>');
            } else if (c == '#') {
                i = skipUntil(sparql, i+1, '\n');
            } else if (c == 's' || c == 'S') {
                //select
                if (i+5 >= len) return null;
                char c1 = sparql.charAt(i+1), c2 = sparql.charAt(i+2);
                if (c1 != 'e' && c1 != 'E' && c2 != 'l' && c2 != 'L') continue;
                c1 = sparql.charAt(i+3); c2 = sparql.charAt(i+4);
                if (c1 != 'e' && c1 != 'E' && c2 != 'c' && c2 != 'C') continue;
                c1 = sparql.charAt(i+5);
                if (c1 == 't' || c1 == 'T') {
                    return readProjection(sparql, i, false);
                }
            } else if (c == 'a' || c == 'A') {
                if (i+2 >= len) return null;
                char c1 = sparql.charAt(i+1), c2 = sparql.charAt(i+2);
                if ((c1 == 's' || c1 == 'S') && (c2 == 'k' || c2 == 'K')) {
                    int end = skipUntil(sparql, i + 2, '{');
                    return new ProjectionInfo(true, i, end, emptyList());
                }
            } else { // i == end or c is not expected in prologue
                return null;
            }
        }
        return null;
    }

    private static final char[] PROJECTION_FIRST = "#$()*?{".toCharArray();

    static @Nullable ProjectionInfo readProjection(CharSequence sparql, int begin, boolean isAsk) {

        Set<@MinLen(1) String> set = new LinkedHashSet<>();
        int consumed = begin + (isAsk ? 3 : 6), len = sparql.length(), lastVarName = -1, depth = 0;
        if (consumed > len)
            return null;
        for (int i; consumed < len; consumed = i+1) {
            i = skipUntilIn(sparql, consumed, len, PROJECTION_FIRST);
            char c = i == len ? '\0' : sparql.charAt(i);
            if (c == '(') {
                depth = Math.max(0, depth)+1;
                lastVarName = len; // makes varEnd() see no var on branch for ')'
            } else if (c == '#') {
                i = skipUntil(sparql, i + 1, '\n');
            } else if (c == ')') {
                if (--depth == 0) {
                    int e = varEnd(sparql, lastVarName, len);
                    if (e > lastVarName)
                        set.add(sparql.subSequence(lastVarName, e).toString());
                }
            } else if (c == '{') {
                if (depth == 0) {
                    consumed = i;
                    break; // end of projection
                }
            } else if (c == '*') {
                if (depth == 0)
                    return null;
            } else {
                lastVarName = i+1;
                if (depth == 0) {
                    int varEnd = varEnd(sparql, lastVarName, len);
                    if (varEnd > lastVarName)
                        set.add(sparql.subSequence(lastVarName, varEnd).toString());
                    i = varEnd-1;
                }
            }
        }

        return new ProjectionInfo(isAsk, begin, consumed, new ArrayList<>(set));
    }

    private static final char[] NEXT_VAR_FIRST = "\"#$'<?".toCharArray();

    static int nextVar(CharSequence cs, int begin, int end) {
        for (int i = begin; i < end; i++) {
            i = skipUntilIn(cs, i, end, NEXT_VAR_FIRST);
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
