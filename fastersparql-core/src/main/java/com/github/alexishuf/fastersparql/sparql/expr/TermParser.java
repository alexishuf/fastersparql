package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.model.rope.RopeWrapper;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.Rope.*;
import static com.github.alexishuf.fastersparql.model.rope.RopeDict.DT_integer;
import static com.github.alexishuf.fastersparql.model.rope.RopeDict.DT_string;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Range;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.invert;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.*;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public final class TermParser {
    private static final byte[][][] QUOTE_BYTES = {
            {null, {'\''}, null, {'\'', '\'', '\''}},
            {null, {'"' }, null, {'"',  '"',  '"'}},
    };
    private static final byte[] NT_QUOTE = QUOTE_BYTES[1][1];
    private static final int[] REL_IRI_ALLOWED = invert(alphabet("<>(){|", Rope.Range.WS));
    private static final Term[] POS_INTEGERS = new Term[1_000];
    private static final Term[] NEG_INTEGERS = new Term[1_000];

    private Rope in;
    private int begin, end;
    private int stopped, colon, qLen, lexEnd, dtBegin, langBegin, dtId;
    private Result result;
    private @Nullable Term term;

    /**
     * Map from prefixes (e.g., @code rdf:) to IRI prefixes (e.g., {@code http://...}) used to
     * parse prefixed IRIs (e.g., {@code rdf:type}) and datatypes (e.g., {@code "1"^^xsd:int}).
     */
    public PrefixMap prefixMap = new PrefixMap().resetToBuiltin();

    public enum Result {
        /** An RDF term in valid N-Triples syntax. */
        NT,
        /** A SPARQL ?- or $-prefixed variable */
        VAR,
        /** Unquoted boolean/numeric literal, [], (), '-quoted, '''-quoted or """-quoted strings */
        TTL,
        /** Not a valid term (e.g., unexpected suffixes, unclosed quotes before EOF) */
        MALFORMED,
        /** Met EOF before the term was complete (i.e., unclosed <, '\'' or '"') */
        EOF;

        public boolean isValid() { return this.ordinal() < 3; }
    }

    /**
     * Tries to parse the SPARQL RDF term or var that starts at index {@code begin} in
     * string {@code in} and ends at or before {@code end}.
     *
     * @param in string containing the SPARQL representation of an RDF term or var
     * @param begin index of the first char of the RDF term/var in {@code in}
     * @param end index of the first char after the RDF term/var in {@code in}
     * @return {@code true} iff there is such RDF term/var. If this method returns
     *         false, {@code in.substring(begins)} starts with an invalid term or something
     *         that is not a term (e.g., an SPARQL expression operator such as {@code <}.
     */
    public Result parse(Rope in, int begin, int end) {
        this.in = in;
        stopped = this.begin = begin;
        this.end = end;
        colon = qLen = lexEnd = dtBegin = langBegin = dtId = -1;
        term = null;
        if (end <= begin)
            return result = Result.EOF; //no term has less than 2 chars
        byte f = in.get(begin);
        return result = switch (f) {
            default       -> parseTTL();
            case '$', '?' -> {
                stopped = in.skip(begin+1, end, VARNAME);
                yield stopped-(begin) > 1 ? Result.VAR : Result.EOF;
            }
            case '<'      -> {
                if (begin+1 >= end) yield Result.EOF;
                stopped = in.requireThenSkipUntil(begin+1, end, LETTERS, '>');
                if      (stopped == end)         yield Result.EOF;
                else if (in.get(stopped) != '>') yield ambiguousIri(in, begin, end);
                ++stopped;
                yield Result.NT;
            }
            case '"', '\'' -> {
                if (begin+1 >= end) yield Result.EOF;
                int p = begin+1;
                qLen = p + 1 < end && in.get(p) == f && in.get(p+1) == f ? 3 : 1;
                byte[] quote = QUOTE_BYTES[f == '"' ? 1 : 0][qLen];
                p = begin+qLen;
                if ((lexEnd = in.skipUntilUnescaped(p, end, quote)) >= end) {
                    stopped = end;
                    yield Result.EOF;  //unclosed quote
                } else if (lexEnd+qLen == end) { // no datatype/language
                    stopped = end;
                } else { // may have @ or ^^
                    if (qLen > 1)
                        checkAmbiguousLexEnd();
                    switch (in.get(p = lexEnd+qLen)) {
                        default  -> stopped = p; // no @/^^
                        case '@' -> {
                            if ((stopped = in.skip(langBegin = ++p, end, LANGTAG)) == langBegin)
                                yield Result.MALFORMED; // empty lang tag
                        }
                        case '^' -> {
                            dtBegin = p+2;
                            if (dtBegin >= end || in.get(++p) != '^') {
                                stopped = p;
                                yield Result.MALFORMED;
                            }
                            if (in.get(++p) == '<') { // may have long <datatype>
                                stopped = in.requireThenSkipUntil(++p, end, LETTERS, '>');
                                if      (stopped == p && p < end) yield Result.MALFORMED;
                                else if (stopped < end)           ++stopped;
                                else                              yield Result.EOF;
                            } else {
                                Term dt = parsePrefixed(p); //validates and sets termEnd
                                if (dt != null) dtId = dt.asKnownDatatypeId();
                                else            yield stopped == end ? Result.EOF : Result.MALFORMED;
                            }
                        }
                    }
                }
                yield quote != NT_QUOTE || dtId != -1 ? Result.TTL : Result.NT;
            }
        };
    }

    private Result ambiguousIri(Rope in, int begin, int end) {
        int close = in.skipUntil(begin, end, '>');
        if (close == end)
            return Result.EOF;
        if (Rope.contains(BAD_IRI_START, in.get(begin+1)))
            return Result.MALFORMED;
        if (in.skip(begin+1, close, REL_IRI_ALLOWED) != close)
            return Result.MALFORMED;
        stopped = close+1;
        return Result.NT;
    }

    private void checkAmbiguousLexEnd() {
        byte quote = in.get(begin);
        int max = Math.min(end, lexEnd+qLen+1);
        for (int i = lexEnd+qLen; i < max && in.get(i) == quote; ++i)
            ++lexEnd;
    }

    /** Call {@link TermParser#parse(Rope, int, int)} and return {@link TermParser#asTerm()}. */
    public Term parseTerm(Rope in) {
        parse(in, 0, in.len());
        return asTerm();
    }

    /** Call {@link TermParser#parse(Rope, int, int)} and return {@link TermParser#asTerm()}. */
    public Term parseTerm(Rope in, int begin, int end) {
        parse(in, begin, end);
        return asTerm();
    }

    /**
     * Given a previous {@code true}-returning call to {@link TermParser#parse(Rope, int, int)},
     * get a {@link Term} for the parsed RDF term/var.
     *
     * @return a {@link Term} instance
     * @throws InvalidTermException if there is no such term/var or it uses a prefix
     *                              not defined in {@code prefixMap}
     */
    public Term asTerm() {
        if      (!result.isValid()) throw explain();
        else if (term != null)      return term;
        byte f = in.get(begin);
        return switch (f) {
            case '?', '$', '<' -> Term.valueOf(in, begin, stopped);
            case '\'' -> singleQuotedAsTerm();
            case '"' -> {
                int id = dtId;
                if (dtBegin >= 0) {
                    if (id ==  0) yield handleExoticPrefixedDatatype();
                    if (id == -1) id = RopeDict.internDatatype(in, lexEnd + qLen - 1, stopped);
                }
                if (qLen > 1 || id == DT_string)
                    yield coldDoubleQuotedAsTerm(id);
                yield id > 0 ? typed(in, begin, lexEnd, id) : copy(in, begin, stopped);
            }
            default -> Term.typed(RopeWrapper.asOpenLitU8(in, begin, stopped), dtId);
        };
    }

    private Term coldDoubleQuotedAsTerm(int id) {
        if (id == DT_string)
            return qLen > 1 ? wrap(escaped(lexEnd+1)) : copy(in, begin, lexEnd+1);
        else if (qLen > 1)
            return id > 0 ? typed(escaped(lexEnd), id) : wrap(escaped(stopped));
        throw new IllegalStateException(); // precondition (id == DT_string || qLen > 1) violated
    }

    private Term singleQuotedAsTerm() {
        int id = dtId;
        if (dtBegin >= 0) {
            if (id ==  0) return handleExoticPrefixedDatatype();
            if (id == -1) {
                int suffixBegin = lexEnd + qLen - 1;
                var suffix = new ByteRope(stopped - suffixBegin)
                        .append('"').append(in, suffixBegin+1, stopped);
                id = RopeDict.internDatatype(suffix, 0, suffix.len);
            }
        }
        int until = id == DT_string ? lexEnd+qLen : (id > 0 ? lexEnd : stopped);
        byte[] lex = escaped(until);
        return until == lexEnd ? typed(lex, id) : wrap(lex);
    }

    /**
     * If {@link TermParser#parse(Rope, int, int)} returned {@code true}, this is the
     * index of the first char in {@code in} that is not part of the RDF term/var
     * (can be {@code in.length()}).
     */
    public int termEnd() { return result != null && result.isValid() ? stopped : begin; }

    private Term handleExoticPrefixedDatatype() {
        byte[] lex = escaped(lexEnd);
        Term iri = prefixMap.expand(in, lexEnd + 3/*"^^*/, stopped);
        if (iri == null)
            throw new InvalidTermException(in, dtBegin, "Unresolved prefix");
        ByteRope full = new ByteRope(lex.length + 3 + iri.len());
        full.append(lex).append(ByteRope.DT_MID).append(iri);
        return Term.valueOf(full);
    }

    private static final int[] UNTIL_EXPLAIN_STOP = invert(alphabet(",\t\r\n", Range.WS));

    public InvalidTermException explain() {
        if (result.isValid())
            throw new IllegalStateException("Cannot explain() successful parse");
        Rope sub = in.sub(begin, in.skip(stopped, end, UNTIL_EXPLAIN_STOP));
        if (colon != -1)
            return new InvalidTermException(sub, colon-begin, "Unknown prefix "+in.sub(begin, colon));
        else if (end <= begin)
            return new InvalidTermException(sub, 0, "EOF: end <= begin");
        byte f = in.get(begin);
        return switch (f) {
            case '<' -> {
                if (result == Result.EOF)
                    yield new InvalidTermException(sub, 0, "No matching > for <");
                else if (begin+1 == end || !Rope.contains(LETTERS, in.get(begin+1)))
                    yield new InvalidTermException(sub, 1, "< followed by non-letter: likely a comparison operator");
                yield new InvalidTermException(sub, 0, "Malformed IRI");
            }
            case '"', '\'' -> {
                if (lexEnd+qLen > end) {
                    String quote = new String(QUOTE_BYTES[f == '"' ? 1 : 0][qLen]);
                    yield new InvalidTermException(sub, 0, "No closing "+quote);
                } else if (langBegin != -1) {
                    yield new InvalidTermException(sub, langBegin-begin, "Empty lang tag");
                } else if (dtBegin >= end || in.get(dtBegin-1) != '^') {
                    yield new InvalidTermException(sub, dtBegin-1-begin, "Incomplete ^^< suffix");
                } else if (in.get(dtBegin) == '<') {
                    if (result == Result.MALFORMED)
                        yield new InvalidTermException(sub, dtBegin-begin, "malformed datatype IRI "+in.sub(dtBegin, stopped));
                    yield new InvalidTermException(sub, dtBegin-begin, "No matching > for datatype IRI");
                } else {
                    yield new InvalidTermException(sub, dtBegin-begin, "Could not parse prefixed datatype IRI");
                }
            }
            default -> {
                int colon = in.skip(begin, end, PN_PREFIX);
                if (colon != end && in.get(colon) == ':')
                    yield new InvalidTermException(sub, 0, "Could not resolve prefix "+in.sub(begin, colon+1));
                if (Rope.contains(NUMBER_FIRST, f))
                    yield new InvalidTermException(sub, 0, "Could not parse numeric value");
                yield new InvalidTermException(sub, 0, "Unrecognized token (not a prefixed name)");
            }
        };
    }

    private @Nullable Term parsePrefixed(int p) {
        int colon = in.skip(p, end, PN_PREFIX);
        if (colon == end || in.get(colon) != ':')
            return null;
        boolean bn = colon == p + 1 && in.get(p) == '_';
        int e = in.skip(colon+1, end, bn ? BN_LABEL : PN_LOCAL);
        while (in.get(e-1) == '.' && in.get(e-2) != '\\') --e; // remove trailing .
        stopped = e;
        return bn ? Term.valueOf(in, p, e) :  prefixMap.expand(in, p, colon, e);
    }

    private static final byte[]  TRUE_utf8 = "true".getBytes(UTF_8);
    private static final byte[] FALSE_utf8 = "false".getBytes(UTF_8);

    private Result parseTTL() {
        assert begin != end;
        if ((term = parsePrefixed(begin)) != null)
            return Result.TTL;
        byte first = in.get(begin);
        if (first == 'a') {
            if (Rope.contains(A_FOLLOW, begin+1 == end ? (byte)'.' : in.get(begin+1))) {
                stopped = begin+1;
                term = RDF_TYPE;
                return Result.TTL;
            }
            return Result.MALFORMED;
        } else if (Rope.contains(NUMBER_FIRST, first)) {
            byte dot = 0, exp = 0, expSig = 0;
            int p = begin + switch (in.get(begin)) {
                case '+', '-' -> 1;
                default       -> 0;
            };
            for (; p < end; ++p) {
                byte c = in.get(p);
                boolean quit = switch (c) {
                    case '.' -> exp == 1 || dot++ == 1;
                    case 'e', 'E' -> exp++ == 1;
                    case '+', '-' -> exp == 0 || expSig++ == 1;
                    default -> c < '0' || c > '9';
                };
                if (quit)
                    break;
            }
            if (p <= end && p > begin && in.get(p-1) == '.') {
                --p;
                --dot;
            }
            stopped = p;
            dtId = 0;
            if (exp == 1) {
                dtId = RopeDict.DT_DOUBLE;
            } else if (dot >= 1) {
                dtId = RopeDict.DT_decimal;
            } else if (exp == 0 && expSig == 0 && dot == 0) {
                dtId = RopeDict.DT_integer;
                if (stopped-begin <= 4)
                    term = cachedInteger((int)in.parseLong(begin));
            }
            return dtId == 0 ? Result.MALFORMED : Result.TTL;
        } else if (first == 't' && in.has(begin, TRUE_utf8) && (begin+4 == end || contains(BOOL_FOLLOW, in.get(begin+4)))) {
            stopped = begin+4;
            term = TRUE;
            return Result.TTL;
        } else if (first == 'f' && in.has(begin, FALSE_utf8) && (begin+5 == end || contains(BOOL_FOLLOW, in.get(begin+5)))) {
            stopped = begin+5;
            term = FALSE;
            return Result.TTL;
        } else {
            return Result.MALFORMED;
        }
    }

    private @Nullable Term cachedInteger(int value) {
        Term[] cache = value < 0 ? NEG_INTEGERS : POS_INTEGERS;
        int pos = Math.abs(value);
        if (value < cache.length) {
            Term term = cache[pos];
            if (term == null)
                cache[pos] = term = Term.typed(in, begin, stopped, DT_integer);
            return term;
        }
        return null;
    }

    private byte[] escaped(int until) {
        int[] escapeNames = in.get(begin) == '"' ? LIT_ESCAPE_NAME : LIT_ESCAPE_NAME_SINGLE;
        var esc = new ByteRope(1 + lexEnd-(begin+qLen)
                                         + (until > lexEnd ? 1 + until-(lexEnd+qLen) : 0));
        esc.append('"');
        for (int consumed = begin+qLen, i; consumed < lexEnd; consumed = i+1) {
            i = in.skip(consumed, lexEnd, UNTIL_LIT_ESCAPED);
            esc.append(in, consumed, i);
            if (i < lexEnd) {
                byte c = in.get(i);
                esc.append('\\').append((byte)switch (c) {
                    case '\\' -> i+1 < lexEnd && contains(escapeNames, in.get(i+1))
                               ? in.get(++i) : '\\';
                    case '\r' -> 'r';
                    case '\n' -> 'n';
                    default -> c;
                });
            }
        }
        if (until > lexEnd) {
            esc.append('"');
            int begin = lexEnd+qLen;
            if (begin < until)
                esc.append(in, lexEnd + qLen, until);
        }
        return esc.utf8.length == esc.len ? esc.utf8 : Arrays.copyOf(esc.utf8, esc.len);
    }
}
