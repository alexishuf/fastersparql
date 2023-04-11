package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;


import static com.github.alexishuf.fastersparql.model.rope.Rope.*;
import static com.github.alexishuf.fastersparql.model.rope.RopeDict.*;
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

    private static final byte TTL_KIND_NUMBER   = 0;
    private static final byte TTL_KIND_TRUE     = 1;
    private static final byte TTL_KIND_FALSE    = 2;
    private static final byte TTL_KIND_TYPE     = 3;
    private static final byte TTL_KIND_BNODE    = 4;
    private static final byte TTL_KIND_PREFIXED = 5;

    private Rope in;
    private @Nullable ByteRope ntBuf;
    private int begin, end;
    private int stopped, lexEnd;
    private byte qLen, ttlKind;
    boolean typed, eager;
    private Result result;
    public int localBegin, localEnd, flaggedId;

    /**
     * Map from prefixes (e.g., @code rdf:) to IRI prefixes (e.g., {@code http://...}) used to
     * parse prefixed IRIs (e.g., {@code rdf:type}) and datatypes (e.g., {@code "1"^^xsd:int}).
     */
    public PrefixMap prefixMap = new PrefixMap().resetToBuiltin();

    /**
     * Makes this {@link TermParser} eager.
     *
     * <p>An eager parser will not return {@link Result#EOF} when meeting a TTL literal not
     * followed by a character that is not part of the literal. For example, an eager parser
     * will return {@link Result#TTL} for inputs {@code 23}, {@code "23"^^xsd:int} and
     * {@code ex:loc} even there could be more data past these inputs.</p>
     *
     * @return {@code this}
     */
    public @This TermParser eager() { eager = true; return this; }

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
        if (ntBuf != null) ntBuf.clear();
        stopped = this.begin = begin;
        this.end = end;
        flaggedId = 0x80000000;
        localBegin = localEnd = -1;
        qLen = -1;
        typed = false;
        if (end <= begin)
            return result = Result.EOF; //no term has less than 2 chars
        byte f = in.get(begin);
        return result = switch (f) {
            default       -> parseTTL();
            case '$', '?' -> {
                stopped = in.skip(begin+1, end, VARNAME);
                if (stopped - begin > 1) {
                    localBegin = begin;
                    localEnd = stopped;
                    flaggedId = 0;
                    yield Result.VAR;
                }
                yield Result.EOF;
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
                qLen = p + 1 < end && in.get(p) == f && in.get(p+1) == f ? (byte)3 : (byte)1;
                byte[] quote = QUOTE_BYTES[f == '"' ? 1 : 0][qLen];
                p = begin+qLen;
                lexEnd = in.skipUntilUnescaped(p, end, quote);
                if (lexEnd >= end) {
                    stopped = end;
                    yield Result.EOF;  //unclosed quote
                } else if (lexEnd +qLen == end) { // no datatype/language
                    localEnd = lexEnd;
                    stopped = end;
                } else { // may have @ or ^^
                    if (qLen > 1)
                        checkAmbiguousLexEnd();
                    switch (in.get(p = lexEnd +qLen)) {
                        default  -> stopped = p; // no @/^^
                        case '@' -> {
                            if ((stopped = in.skip(++p, end, LANGTAG)) == p)
                                yield Result.MALFORMED; // empty lang tag
                        }
                        case '^' -> {
                            typed = true;
                            if (p+2 >= end || in.get(++p) != '^') {
                                stopped = p;
                                yield Result.MALFORMED;
                            }
                            if (in.get(++p) == '<') { // may have long <datatype>
                                stopped = in.requireThenSkipUntil(++p, end, LETTERS, '>');
                                if      (stopped == p && p < end) yield Result.MALFORMED;
                                else if (stopped < end)           ++stopped;
                                else                              yield Result.EOF;
                            } else {
                                int iriBegin = p;
                                stopped = p = in.skip(p, end, PN_PREFIX);
                                if      (p         == end) yield Result.EOF;
                                else if (in.get(p) != ':') yield Result.MALFORMED;
                                else if (prefixMap.expand(in, iriBegin, p, p+1) == null)
                                    yield Result.MALFORMED; // unresolved prefix
                                stopped = p = in.skip(p+1, end, PN_LOCAL);
                                while (in.get(p-1) == '.' && !in.isEscaped(p-1)) --p;
                                if (!eager && p == end) yield Result.EOF;
                                stopped = p;
                                yield Result.TTL;
                            }
                        }
                    }
                }
                yield quote != NT_QUOTE ? Result.TTL : Result.NT;
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
     * Get the {@link Result} of the last {@link #parse(Rope, int, int)} call or
     * {@code null} if {@link #parse(Rope, int, int)} has not been called yet.
     */
    public Result result() { return result; }

    /**
     * Get the {@link Rope} to which {@link #localBegin()} and {@link #localEnd()} refer to.
     *
     * <p>This may be the input Rope given to {@link #parse(Rope, int, int)}. But if the parsed
     * term was a non-NT literal or the {@code a} keyword this return a buffer held by the
     * {@link TermParser}</p>
     */
    public Rope localBuf() {
        return ntBuf == null || ntBuf.len == 0 ? in : ntBuf;
    }

    /**
     * Equivalent to {@link Term#flaggedDictId} of {@link #asTerm()}.
     *
     * @return the {@link Term#flaggedDictId} of {@link #asTerm()}  or 0 if {@link #asTerm()}
     *         would raise an exception.
     */
    public int flaggedId() {
        if (flaggedId == 0x80000000) postParse();
        return flaggedId;
    }

    /** Sets {@code flaggedId}, {@code localBegin} and {@code localEnd} after
     *  {@link #parse(Rope, int, int)} */
    private void postParse() {
        if (result == null) throw new IllegalStateException("parse() not called");
        switch (in.get(begin)) {
            case '<' -> {
                long suffixAndId = RopeDict.internIri(in, begin, stopped);
                flaggedId  = (int)suffixAndId;
                localBegin = (int)(suffixAndId >>> 32);
                localEnd   = stopped;
            }
            case '"', '\'' -> {
                if (result == Result.NT) {
                    localBegin = begin;
                    int id = typed ? RopeDict.internDatatype(in, lexEnd, stopped) : 0;
                    if (id == 0 || id == DT_langString) {
                        localEnd = stopped;
                        flaggedId = 0;
                    } else if (id == DT_string) {
                        localEnd = lexEnd+1;
                        flaggedId = 0;
                    } else {
                        localEnd = lexEnd;
                        flaggedId = 0x80000000 | id;
                    }
                } else {
                    if (ntBuf == null) ntBuf = new ByteRope(stopped - begin);
                    toNT();
                    localBegin = 0;
                    if (typed) {
                        int id = flaggedId;
                        if (id != 0x80000000) {
                            localEnd = ntBuf.len-1;
                            id &= 0x7fffffff;
                        } else {
                            long suffixAndId = RopeDict.internLit(ntBuf, 0, ntBuf.len);
                            localEnd  = (int)(suffixAndId >>> 32);
                            id = (int)suffixAndId;
                        }
                        if (id == DT_string || id == DT_langString) {
                            localEnd++; // include closing "
                            id = 0;
                        }
                        flaggedId = id == 0 ? 0 : (id | 0x80000000);
                    } else {
                        localEnd = ntBuf.len;
                        flaggedId = 0;
                    }
                }
            }
            default -> {
                ByteRope buf = ntBuf == null ? ntBuf = new ByteRope(32) : ntBuf.clear();
                localBegin = 0;
                switch (ttlKind) {
                    case TTL_KIND_NUMBER  -> buf.append('"').append(in, begin, stopped);
                    case TTL_KIND_TRUE    -> buf.append( TRUE.local);
                    case TTL_KIND_FALSE   -> buf.append(FALSE.local);
                    case TTL_KIND_TYPE    -> buf.append(RDF_TYPE.local);
                    case TTL_KIND_BNODE   -> {
                        flaggedId = 0;
                        localBegin = begin;
                        localEnd = stopped;
                    }
                    case TTL_KIND_PREFIXED  -> {
                        int colon = in.skip(begin, stopped, PN_PREFIX);
                        //expanding with empty localName causes no allocation
                        Term term = prefixMap.expand(in, begin, colon, colon + 1);
                        if (term == null)
                            throw new InvalidTermException(in, begin, "Unresolved prefix");
                        if ((flaggedId = term.flaggedDictId) == 0)
                            buf.append(term.local, 0, term.local.length-1);
                        buf.append(in, colon+1, stopped).append('>');
                    }
                    default -> throw new IllegalStateException("corrupted");
                }
                if (localEnd == -1)
                    localEnd = buf.len;
            }
        }
    }

    /**
     * Offset into {@link #localBuf()} where the {@link Term#local} will begin if {@link #result()}
     * was {@link Result#NT} or {@link Result#VAR}. If {@link #result()} was {@link Result#TTL},
     * this will point to the first byte of the TTL string, which may refer to a subsequence of
     * {@link Term#local} or not even appear in {@link Term#local} (e.g., {@code a}).
     *
     * @return -1 if {@link #result()} is {@code null}, {@link Result#MALFORMED} or
     *         {@link Result#EOF}. Else an absolute index into {@link #localBuf()} that corresponds to
     *         the index where {@link Term#local} or an TTL term begins.
     */
    public int localBegin() {
        if (localBegin == -1) postParse();
        return localBegin;
    }

    /**
     * Where the local portion or TTL term pointed to by {@link #localBegin()} ends in {@link #localBuf()}
     * or -1 if {@link #localBegin()} is -1.
     */
    public int localEnd() {
        if (localEnd == -1) postParse(); // will set localEnd
        return localEnd;
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
        if (result == null || !result.isValid()) throw explain();
        return switch (in.get(begin)) {
            case '?', '$', '_' -> Term.valueOf(in, begin, stopped);
            case '<'           -> Term.prefixed(flaggedId(), in, localBegin, localEnd);
            case '"', '\''     -> {
                int flaggedId = flaggedId();
                if (flaggedId == 0)
                    yield Term.wrap(localBuf().toArray(localBegin(), localEnd));
                yield Term.typed(localBuf(), localBegin(), localEnd, flaggedId);
            }
            default -> switch (ttlKind) {
                case TTL_KIND_NUMBER   -> {
                    if (flaggedId == DT_integer && stopped-begin <= 4)
                        yield cachedInteger((int)in.parseLong(begin));
                    yield Term.typed(RopeWrapper.asOpenLitU8(in, begin, stopped), this.flaggedId);
                }
                case TTL_KIND_TRUE     -> TRUE;
                case TTL_KIND_FALSE    -> FALSE;
                case TTL_KIND_TYPE     -> RDF_TYPE;
                case TTL_KIND_BNODE    -> Term.valueOf(in, begin, end);
                case TTL_KIND_PREFIXED -> prefixMap.expand(in, begin, stopped);
                default -> throw new IllegalStateException("corrupted");
            };
        };
    }

    /**
     * If {@link TermParser#parse(Rope, int, int)} returned {@code true}, this is the
     * index of the first char in {@code in} that is not part of the RDF term/var
     * (can be {@code in.length()}).
     */
    public int termEnd() { return result != null && result.isValid() ? stopped : begin; }

    private static final int[] UNTIL_EXPLAIN_STOP = invert(alphabet(",\t\r\n", Range.WS));

    public InvalidTermException explain() {
        if (result == null)
            throw new IllegalStateException("parse() not called");
        if (result.isValid())
            throw new IllegalStateException("Cannot explain() successful parse");
        Rope sub = in.sub(begin, in.skip(stopped, end, UNTIL_EXPLAIN_STOP));
        if (end <= begin)
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
                if (lexEnd+qLen >= end) {
                    String quote = new String(QUOTE_BYTES[f == '"' ? 1 : 0][qLen]);
                    yield new InvalidTermException(sub, 0, "No closing " + quote);
                } else if (in.get(lexEnd+qLen) == '@') {
                    yield new InvalidTermException(sub, lexEnd+qLen-begin, "Empty lang tag");
                } else if (in.get(lexEnd+qLen) != '^') {
                    yield new InvalidTermException(sub, lexEnd + qLen - begin, "Expected @ or ^^< after closing quotes");
                } else if (result == Result.MALFORMED) {
                    yield new InvalidTermException(sub, lexEnd+qLen-begin, "malformed datatype IRI "+in.sub(lexEnd+qLen, stopped));
                } else {
                    yield new InvalidTermException(sub, lexEnd+qLen-begin, "Unterminated datatype IRI");
                }
            }
            default -> {
                if (result == Result.EOF)
                    yield new InvalidTermException(sub, 0, "Ambiguous end of TTL term. Need more input or eager() option");
                if (ttlKind == TTL_KIND_PREFIXED || ttlKind == TTL_KIND_TYPE) {
                    if (in.get(in.reverseSkipUntil(begin, stopped, ':')) == ':')
                        yield new InvalidTermException(sub, 0, "Unresolved prefix");
                    yield new InvalidTermException(sub, 0, "Invalid prefixed IRI syntax");
                }
                if (Rope.contains(NUMBER_FIRST, f))
                    yield new InvalidTermException(sub, 0, "Could not parse numeric value");
                yield new InvalidTermException(sub, 0, "Unrecognized token (not a prefixed name)");
            }
        };
    }

    private static final byte[]  TRUE_utf8 = "true".getBytes(UTF_8);
    private static final byte[] FALSE_utf8 = "false".getBytes(UTF_8);

    private Result parseTTL() {
        assert begin != end;
        int colon = in.skip(begin, end, PN_PREFIX);
        if (colon != end && in.get(colon) == ':') {
            boolean bn = colon == begin + 1 && in.get(begin) == '_';
            int e = in.skip(colon + 1, end, bn ? BN_LABEL : PN_LOCAL);
            while (in.get(e - 1) == '.' && !in.isEscaped(e - 1)) --e; // remove trailing .
            if (!eager && e == end)
                return Result.EOF;
            stopped = e;
            if (bn) {
                ttlKind = TTL_KIND_BNODE;
                return Result.TTL;
            } else if (prefixMap.expand(in, begin, colon, colon + 1) == null) {
                return Result.MALFORMED;
            } else {
                ttlKind = TTL_KIND_PREFIXED;
                return Result.TTL;
            }
        }
        byte first = in.get(begin);
        if (first == 'a') {
            byte follow = begin+1 < end ? in.get(begin+1) : eager ? (byte)'.' : 0;
            if (Rope.contains(A_FOLLOW, follow)) {
                stopped = begin+1;
                ttlKind = TTL_KIND_TYPE;
                flaggedId = P_RDF;
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
            if (p < end && in.get(p) == '.') --dot;
            if (p <= end && p > begin && in.get(p-1) == '.') { --p; --dot; }
            stopped = p;
            if      (exp == 1                           ) flaggedId = 0x80000000|DT_DOUBLE;
            else if (dot >= 1                           ) flaggedId = 0x80000000|DT_decimal;
            else if (exp == 0 && expSig == 0 && dot == 0) flaggedId = 0x80000000|DT_integer;
            else                                          return Result.MALFORMED;
            ttlKind = TTL_KIND_NUMBER;
            return !eager && p == end ? Result.EOF : Result.TTL;
        } else if (tryParseBool(first, (byte)'t', TRUE_utf8)
                || tryParseBool(first, (byte)'f', FALSE_utf8)) {
            return Result.TTL;
        } else {
            return Result.MALFORMED;
        }
    }

    private boolean tryParseBool(byte first, byte exFirst, byte[] utf8) {
        if (first != exFirst || !in.has(begin, utf8)) return false;
        int i = begin+utf8.length;
        if ((i < end && contains(BOOL_FOLLOW, in.get(i))) || (i == end && eager)) {
            stopped = i;
            ttlKind = exFirst == 't' ? TTL_KIND_TRUE : TTL_KIND_FALSE;
            flaggedId = DT_BOOLEAN | 0x80000000;
            return true;
        }
        return false;
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

    private void toNT() {
        int[] escapeNames = in.get(begin) == '"' ? LIT_ESCAPE_NAME : LIT_ESCAPE_NAME_SINGLE;
        int stopped = this.stopped;
        int size = 2/*""*/ + lexEnd - (begin + qLen) + stopped-(lexEnd+qLen);
        ByteRope esc = ntBuf;
        if (ntBuf == null) ntBuf = esc = new ByteRope(size);
        else               ntBuf.clear().ensureFreeCapacity(size);
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
        esc.append('"');
        int p = lexEnd + qLen;
        if ((p < stopped && in.get(p) == '@') || (p+2 < stopped && in.get(p+2) == '<')) {
            esc.append(in, p, stopped);
        } else if (p+3 /*^p:*/ < stopped) { // prefix datatype
            Term iri = prefixMap.expand(in, p += 2 /*^*/, stopped);
            if (iri == null)
                throw new InvalidTermException(in.sub(p, stopped), 0, "Unresolved prefix");
            // if iri is not a xsd: or rdf: iri, flaggedId will remain "unset"
            if ((flaggedId = iri.asKnownDatatypeId() | 0x80000000) == 0x80000000)
                esc.append('^').append('^').append(iri);
        } // else: plain string
    }
}
