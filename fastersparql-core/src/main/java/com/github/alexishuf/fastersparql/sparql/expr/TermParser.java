package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.*;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public abstract sealed class TermParser extends AbstractOwned<TermParser> {
    public static final int BYTES = 16 + 12*4 + MutableRope.BYTES + 128;
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

    private SegmentRope in;
    private @Nullable MutableRope ntBuf;
    private int begin, end;
    private int stopped, lexEnd;
    private byte qLen, ttlKind;
    boolean typed, eager, sharedSuffixed;
    private Result result;
    public int localBegin, localEnd;
    private final PrivateRopeFactory ropeFactory = new PrivateRopeFactory();
    private FinalSegmentRope shared;
    private final PrefixMap prefixMap = PrefixMap.create().takeOwnership(this).resetToBuiltin();

    private static final Alloc<TermParser> ALLOC;
    private static final Supplier<TermParser> ALLOC_FAC = new Supplier<>() {
        @Override public TermParser get() {
            return new TermParser.Concrete().takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "TermParser.ALLOC_FAC";}
    };
    static {
        ALLOC = new Alloc<>(TermParser.class,
                "TermParser.ALLOC", Alloc.THREADS*64,
                ALLOC_FAC, BYTES);
        Primer.INSTANCE.sched(() -> ALLOC.prime(() -> {
            TermParser p = new Concrete().takeOwnership(RECYCLED);
            p.ntBuf = new MutableRope(Bytes.createPooled(new byte[256]), 0);
            return p;
        }, 2, 0));
    }

    public static Orphan<TermParser> create() {
        TermParser p = ALLOC.create();
        p.eager = false;
        p.prefixMap.resetToBuiltin();
        return p.releaseOwnership(RECYCLED);
    }

    private TermParser() {}

    @Override public @Nullable TermParser recycle(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        if (ALLOC.offer(this) != null)
            internalMarkGarbage(RECYCLED);
        return null;
    }

    @Override protected @Nullable TermParser internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        prefixMap.recycle(this);
        if (ntBuf != null)
            ntBuf.close();
        return null;
    }

    private static final class Concrete extends TermParser implements Orphan<TermParser> {
        @Override public TermParser takeOwnership(Object o) {return takeOwnership0(o);}
    }

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

    /**
     * Map from prefixes (e.g., @code rdf:) to IRI prefixes (e.g., {@code http://...}) used to
     * parse prefixed IRIs (e.g., {@code rdf:type}) and datatypes (e.g., {@code "1"^^xsd:int}).
     */
    public PrefixMap prefixMap() { return prefixMap; }

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
    public Result parse(SegmentRope in, int begin, int end) {
        this.in = in;
        if (ntBuf != null) ntBuf.clear();
        stopped = this.begin = begin;
        this.end = end;
        shared = FinalSegmentRope.EMPTY;
        sharedSuffixed = false;
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
                    shared = FinalSegmentRope.EMPTY;
                    yield Result.VAR;
                }
                yield Result.EOF;
            }
            case '<'      -> {
                if (begin+1 >= end) yield Result.EOF;
                stopped = in.requireThenSkipUntil(begin+1, end, LETTERS, (byte)'>');
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
                                stopped = in.requireThenSkipUntil(++p, end, LETTERS, (byte)'>');
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
        int close = in.skipUntil(begin, end, (byte)'>');
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

    /** Call {@link TermParser#parse(SegmentRope, int, int)} and return {@link TermParser#asTerm()}. */
    public Term parseTerm(SegmentRope in) {
        parse(in, 0, in.len());
        return asTerm();
    }

    /** Call {@link TermParser#parse(SegmentRope, int, int)} and return {@link TermParser#asTerm()}. */
    public Term parseTerm(SegmentRope in, int begin, int end) {
        parse(in, begin, end);
        return asTerm();
    }

    /**
     * Get the {@link Result} of the last {@link #parse(SegmentRope, int, int)} call or
     * {@code null} if {@link #parse(SegmentRope, int, int)} has not been called yet.
     */
    public Result result() { return result; }

    /**
     * Get the {@link Rope} to which {@link #localBegin()} and {@link #localEnd()} refer to.
     *
     * <p>This may be the input Rope given to {@link #parse(SegmentRope, int, int)}. But if the parsed
     * term was a non-NT literal or the {@code a} keyword this return a buffer held by the
     * {@link TermParser}</p>
     */
    public SegmentRope localBuf() {
        if (localBegin == -1) postParse();
        return ntBuf == null || ntBuf.len == 0 ? in : ntBuf;
    }

    /**
     * Equivalent to {@link Term#shared()} of {@link #asTerm()}.
     *
     * @return the {@link Term#shared()} of {@link #asTerm()}  or 0 if {@link #asTerm()}
     *         would raise an exception.
     */
    public FinalSegmentRope shared() {
        if (localBegin == -1) postParse();
        return shared;
    }

   /** Equivalent to {@link Term#sharedSuffixed()} of {@link #asTerm()}. */
    public boolean sharedSuffixed() {
        if (localBegin == -1) postParse();
        return sharedSuffixed;
    }

    /** Sets {@code flaggedId}, {@code localBegin} and {@code localEnd} after
     *  {@link #parse(SegmentRope, int, int)} */
    private void postParse() {
        if (result == null) throw new IllegalStateException("parse() not called");
        switch (in.get(begin)) {
            case '<' -> {
                var s      = SHARED_ROPES.internPrefixOf(in, begin, stopped);
                shared     = s;
                sharedSuffixed = false;
                localBegin = begin + s.len;
                localEnd   = stopped;
            }
            case '"', '\'' -> {
                sharedSuffixed = true;
                if (result == Result.NT) {
                    localBegin = begin;
                    var sh = typed ? SHARED_ROPES.internDatatype(in, lexEnd, stopped) : null;
                    if (sh == null || sh == SharedRopes.DT_langString) {
                        localEnd = stopped;
                        shared = FinalSegmentRope.EMPTY;
                    } else if (sh == SharedRopes.DT_string) {
                        localEnd = lexEnd+1;
                        shared = FinalSegmentRope.EMPTY;
                    } else {
                        localEnd = lexEnd;
                        shared = sh;
                    }
                } else {
                    MutableRope ntBuf = this.ntBuf;
                    if (ntBuf == null)
                        this.ntBuf = ntBuf = new MutableRope(stopped-begin);
                    toNT();
                    localBegin = 0;
                    if (typed) {
                        var sh = shared;
                        if (sh.len > 0) {
                            localEnd = ntBuf.len-1;
                        } else {
                            sh = SHARED_ROPES.internDatatypeOf(ntBuf, 0, ntBuf.len);
                            localEnd = ntBuf.len - (sh == null ? 0 : sh.len);
                        }
                        if (sh == SharedRopes.DT_string || sh == SharedRopes.DT_langString) {
                            localEnd++; // include closing "
                            sh = FinalSegmentRope.EMPTY;
                        }
                        shared = sh;
                    } else {
                        localEnd = ntBuf.len;
                        shared = FinalSegmentRope.EMPTY;
                    }
                }
            }
            case '?', '$' -> {
                localBegin = begin;
                localEnd = stopped;
            }
            default -> {
                MutableRope buf = ntBuf == null ? ntBuf = new MutableRope(32) : ntBuf.clear();
                localBegin = 0;
                switch (ttlKind) {
                    case TTL_KIND_NUMBER  -> buf.append('"').append(in, begin, stopped);
                    case TTL_KIND_TRUE    -> buf.append( TRUE.local());
                    case TTL_KIND_FALSE   -> buf.append(FALSE.local());
                    case TTL_KIND_TYPE    -> buf.append(RDF_TYPE.local());
                    case TTL_KIND_BNODE   -> {
                        shared = FinalSegmentRope.EMPTY;
                        localBegin = begin;
                        localEnd = stopped;
                    }
                    case TTL_KIND_PREFIXED  -> {
                        int colon = in.skip(begin, stopped, PN_PREFIX);
                        //expanding with empty localName causes no allocation
                        Term term = prefixMap.expand(in, begin, colon, colon + 1);
                        if (term == null)
                            throw new InvalidTermException(in, begin, "Unresolved prefix");
                        shared = term.finalShared();
                        SegmentRope local = term.local();
                        buf.append(local, 0, local.len-1);
                        unescapePrefixedLocal(buf, colon+1);
                    }
                    default -> throw new IllegalStateException("corrupted");
                }
                if (localEnd == -1)
                    localEnd = buf.len;
            }
        }
    }

    /**
     * Offset into {@link #localBuf()} where the {@link Term#local()} will begin if {@link #result()}
     * was {@link Result#NT} or {@link Result#VAR}. If {@link #result()} was {@link Result#TTL},
     * this will point to the first byte of the TTL string, which may refer to a subsequence of
     * {@link Term#local()} or not even appear in {@link Term#local()} (e.g., {@code a}).
     *
     * @return -1 if {@link #result()} is {@code null}, {@link Result#MALFORMED} or
     *         {@link Result#EOF}. Else an absolute index into {@link #localBuf()} that corresponds to
     *         the index where {@link Term#local()} or an TTL term begins.
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
     * Given a previous {@code true}-returning call to {@link TermParser#parse(SegmentRope, int, int)},
     * get a {@link Term} for the parsed RDF term/var.
     *
     * @return a {@link Term} instance
     * @throws InvalidTermException if there is no such term/var, or it uses a prefix
     *                              not defined in {@code prefixMap}
     */
    public Term asTerm() {
        if (result == null || !result.isValid()) throw explain();
        SegmentRope lBuf = localBuf();
        final SegmentRope sh = shared();
        return switch (in.get(begin)) {
            case '?', '$'  -> Term.wrap(FinalSegmentRope.EMPTY, in.sub(begin, stopped));
            case '<'       -> Term.wrap(sh, in.sub(localBegin, localEnd));
            case '"', '\'' -> {
                var local = lBuf == in ? in.sub(localBegin, localEnd)
                                       : ropeFactory.asFinal(lBuf, localBegin, localEnd);
                yield Term.wrap(local, sh);
            }
            default -> switch (ttlKind) {
                case TTL_KIND_NUMBER   -> {
                    if (sh == DT_integer && localEnd-localBegin <= 4) {
                        Term term = cachedInteger((int) in.parseLong(begin));
                        if (term != null)
                            yield term;
                    }
                    var local = RopeFactory.make(stopped-begin+1)
                                           .add('"').add(in, begin, stopped).take();
                    yield Term.wrap(local, sh);
                }
                case TTL_KIND_TRUE     -> TRUE;
                case TTL_KIND_FALSE    -> FALSE;
                case TTL_KIND_TYPE     -> RDF_TYPE;
                case TTL_KIND_PREFIXED -> Term.wrap(sh, ropeFactory.asFinal(lBuf));
                case TTL_KIND_BNODE    -> {
                    var r = asFinal(in, begin, stopped);
                    yield Term.splitAndWrap(r);
                }
                default -> throw new IllegalStateException("corrupted");
            };
        };
    }

    /**
     * If {@link TermParser#parse(SegmentRope, int, int)} returned {@code true}, this is the
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
            int[] alphabet = colon == begin + 1 && in.get(begin) == '_' ? BN_LABEL : PN_LOCAL;
            int e = in.skip(colon + 1, end, alphabet);
            while (e <= end && in.get(e-1) == '\\')
                e = in.skip(++e, end, alphabet);
            while (in.get(e - 1) == '.' && !in.isEscaped(e - 1)) --e; // remove trailing .
            if (!eager && e == end)
                return Result.EOF;
            stopped = e;
            if (alphabet == BN_LABEL) {
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
                shared = SharedRopes.P_RDF;
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
            sharedSuffixed = true;
            if      (exp == 1                           ) shared = SharedRopes.DT_DOUBLE;
            else if (dot >= 1                           ) shared = SharedRopes.DT_decimal;
            else if (exp == 0 && expSig == 0 && dot == 0) shared = DT_integer;
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
            sharedSuffixed = true;
            shared = SharedRopes.DT_BOOLEAN;
            return true;
        }
        return false;
    }

    private @Nullable Term cachedInteger(int value) {
        Term[] cache = value < 0 ? NEG_INTEGERS : POS_INTEGERS;
        int pos = Math.abs(value);
        if (value < cache.length) {
            Term term = cache[pos];
            if (term == null) {
                term = Term.wrap(asFinal(localBuf(), localBegin(), localEnd()), DT_integer);
                cache[pos] = term;
            }
            return term;
        }
        return null;
    }

    private void toNT() {
        int[] escapeNames = in.get(begin) == '"' ? LIT_ESCAPE_NAME : LIT_ESCAPE_NAME_SINGLE;
        int stopped = this.stopped;
        int size = 2/*""*/ + lexEnd - (begin + qLen) + stopped-(lexEnd+qLen);
        MutableRope esc = ntBuf;
        if (ntBuf == null) ntBuf = esc = new MutableRope(size);
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
            sharedSuffixed = true;
            if ((shared = iri.asKnownDatatypeSuff()) == null)
                esc.append('^').append('^').append(iri);
        } // else: plain string
    }

    private void unescapePrefixedLocal(MutableRope out, int begin) {
        var in = this.in;
        for (int i, end = stopped; begin < end; begin = i) {
            out.append(in, begin, i = in.skipUntil(begin, end, (byte)'\\'));
            if (++i >= end) break;
            out.append(in.get(i++));
        }
        out.append('>');
    }
}
