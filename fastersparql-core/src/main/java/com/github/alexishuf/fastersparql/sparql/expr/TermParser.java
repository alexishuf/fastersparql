package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.client.util.Skip.alphabet;
import static com.github.alexishuf.fastersparql.client.util.Skip.skip;
import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.FALSE;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.TRUE;

@SuppressWarnings({"StringEquality", "BooleanMethodIsAlwaysInverted"})
public final class TermParser {
    private String in;
    private int begin, end;
    private int termEnd, colon, qLen, lexEnd, dtBegin, langBegin;
    private @Nullable Term term;
    private @Nullable String dt;

    /**
     * Map from prefixes (e.g., @code rdf:) to IRI prefixes (e.g., {@code http://...}) used to
     * parse prefixed IRIs (e.g., {@code rdf:type}) and datatypes (e.g., {@code "1"^^xsd:int}).
     */
    public PrefixMap prefixMap = PrefixMap.builtin();

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
    public boolean parse(String in, int begin, int end) {
        this.in = in;
        termEnd = this.begin = begin;
        this.end = end;
        colon = qLen = lexEnd = dtBegin = langBegin = -1;
        dt = null;
        term = null;
        if (begin >= end)
            return false;
        char f = in.charAt(begin);
        switch (f) {
            case '<' -> {
                int e = skip(in, begin+1, end, IRIREF);
                termEnd = e == end || in.charAt(e) != '>' ? begin : e+1;
            }
            case '$', '?' -> termEnd = skip(in, begin+1, end, VARNAME);
            case '"', '\'' -> {
                int p = begin+1;
                qLen = p + 1 < end && in.charAt(p) == f && in.charAt(p+1) == f ? 3 : 1;
                String quote = qLen == 3 ? (f == '"' ? "\"\"\"" : "'''") : (f == '"' ? "\"" : "'");
                p = begin+qLen;
                lexEnd = in.indexOf(quote, p);
                while (lexEnd != -1) {
                    while (lexEnd+qLen < end && in.charAt(lexEnd+qLen) == f) ++lexEnd;
                    int escapes = 0;
                    for (int i = lexEnd - 1, startLex = begin + qLen; i >= startLex; i--) {
                        if (in.charAt(i) == '\\') ++escapes;
                        else break;
                    }
                    if (escapes % 2 == 0) break;
                    lexEnd = in.indexOf(quote, lexEnd + qLen);
                }
                if (lexEnd == -1) {
                    termEnd = begin; //unclosed quote
                } else if (lexEnd+qLen == end) { // no datatype/language
                    dt = string;
                    termEnd = end;
                } else { // may have @ or ^^
                    switch (in.charAt(p = lexEnd+qLen)) {
                        default  -> { // no @/^^
                            dt = string;
                            termEnd = p;
                        }
                        case '@' -> {
                            dt = langString;
                            termEnd = skip(in, langBegin = ++p, end, LANGTAG);
                            if (termEnd == langBegin)
                                termEnd = begin; // empty lang tag
                        }
                        case '^' -> {
                            dtBegin = p+2;
                            if (dtBegin >= end || in.charAt(++p) != '^') {
                                termEnd = begin; // syntax error
                            } else if (in.charAt(++p) == '<') { // may have long <datatype>
                                int e = skip(in, p+1, end, IRIREF);
                                if (e < end && in.charAt(e) == '>') { // has <datatype>
                                    // http://www.w3.org/2001/XMLSchema#string
                                    if (e-p-1 == 39 && in.regionMatches(p+31, "ma#string", 0, 9))
                                        dt = string;
                                    termEnd = e+1;
                                } else {
                                    termEnd = begin;
                                }
                            } else if (!parsePrefixed(p)) {
                                termEnd = begin; // unknown prefix or syntax error
                            }
                        }
                    }
                }
            }
            default -> {
                if (!parsePrefixed(begin) && !parseRdfType()) {
                    if ((NUMBER_FIRST[f>>6] & (1L << f)) != 0) {
                        parseNumber();
                    } else if (!parseBool(TRUE) && !parseBool(FALSE)) {
                        termEnd = begin; // no term
                    }
                } // else: colon and termEnd set
            }
        }
        return termEnd > begin;
    }

    /**
     * Given a previous {@code true}-returning call to {@link TermParser#parse(String, int, int)},
     * get a {@link Term} for the parsed RDF term/var.
     *
     * @return a {@link Term} instance
     * @throws InvalidTermException if there is no such term/var or it uses a prefix
     *                              not defined in {@code prefixMap}
     */
    public Term asTerm() {
        if (termEnd == begin)
            throw explain();
        char f = in.charAt(begin);
        return switch (in.charAt(begin)) {
            case '?', '$' -> new Term.Var(in.substring(begin, termEnd));
            case '<' -> new Term.IRI(in.substring(begin, termEnd));
            case '"', '\'' -> {
                String lex, dt = this.dt, lang = null;
                if (f == '"' && qLen == 1) {
                    lex = in.substring(begin+1, lexEnd);
                } else {
                    var sb = new StringBuilder(lexEnd - begin + 10);
                    escapeForSingleDQuote(sb);
                    lex = sb.toString();
                }
                if (langBegin != -1) {
                    lang = in.substring(langBegin, termEnd);
                    dt = langString;
                } else if (dt == null) {
                    if (colon == -1) {
                        // <http://www.w3.org/2001/XMLSchema#>
                        if (dtBegin+34 < termEnd && in.charAt(dtBegin+32) == 'a'
                                                 && in.charAt(dtBegin+33) == '#') {
                            dt = RDFTypes.fromXsdLocal(in, dtBegin+34, termEnd-1);
                        } else {
                            dt = in.substring(dtBegin+1, termEnd-1);
                        }
                    } else {
                        dt = expandPrefixed(dtBegin, prefixMap);
                    }
                }
                yield new Term.Lit(lex, dt, lang);
            }
            default -> {
                if  (term != null)
                    yield term;
                else if (dt != null)
                    yield new Term.Lit(in.substring(begin, termEnd), dt, null);
                else if (f == '_' && colon == begin+1)
                    yield new Term.BNode(in.substring(begin, termEnd));
                else if (colon >= 0)
                    yield new Term.IRI(expandPrefixed(begin, prefixMap));
                throw new InvalidTermException(in, begin, "Not an RDF term/var");
            }
        };
    }

    /**
     * If {@link TermParser#parse(String, int, int)} returned {@code true}, this is the
     * index of the first char in {@code in} that is not part of the RDF term/var
     * (can be {@code in.length()}).
     */
    public int termEnd() { return termEnd; }

    private static final long[] NT_FIRST = alphabet("?$<").get();
    /**
     * Whether the term parsed by the last {@link TermParser#parse(String, int, int)} call is
     * in N-Triples syntax or is a var.
     */
    public boolean isNTOrVar() {
        char f = begin < end ? in.charAt(begin) : '~';
        return (NT_FIRST[f>>6] & (1L << f)) != 0
                || (f == '_' && colon == begin+1)
                || (f == '"' && qLen == 1 && colon == -1);
    }

    /**
     * Get the term parsed by the previous {@link TermParser#parse(String, int, int)} call
     * in N-Triples syntax. This will expand prefixes and convert long-quoted or
     * {@code '}-quoted literals into literals quoted with a single {@code "}.
     *
     * @return the term in N-Triples syntax or the var in SPARQL syntax
     * @throws InvalidTermException if the previous {@link TermParser#parse(String, int, int)}
     *                              call returned false or if the term uses a prefix not
     *                              defined in {@code prefixMap}
     */
    public String asNT() {
        if (termEnd == begin) {
            throw explain();
        } else if (isNTOrVar()) {
            if (begin == 0 && termEnd == in.length())
                return in; // if the term spans the whole input, do not call substring
            // substring() will not be a no-op. Do not waster space/time on ^^<...#string>
            return in.substring(begin, dt == string ? lexEnd+1 : termEnd);
        }
        return buildNT();
    }

    private String buildNT() {
        char f = in.charAt(begin);
        return switch (f) {
            case '"', '\'' -> {
                var sb = new StringBuilder(termEnd - begin + (colon == -1 ? 10 : 55));
                sb.append('"');
                if (f == '"' && qLen == 1)
                    sb.append(in, begin + 1, lexEnd);
                else
                    escapeForSingleDQuote(sb);
                sb.append('"');
                if (langBegin != -1) {
                    sb.append('@').append(in, langBegin, termEnd);
                } else if (dtBegin != -1 && dt != string) {
                    if (colon == -1)
                        sb.append("^^").append(in, dtBegin, termEnd);
                    else
                        sb.append("^^<").append(expandPrefixed(dtBegin, prefixMap)).append('>');
                }
                yield sb.toString();
            }
            default -> {
                if (term != null)
                    yield term.nt();
                if (dt != null)
                    yield '"' + in.substring(begin, termEnd) + "\"^^<" + dt + '>';
                if (colon >= 0)
                    yield '<' + expandPrefixed(begin, prefixMap) + '>';
                throw new InvalidTermException(in, begin, "Not an RDF term/var");
            }
        };
    }

    private InvalidTermException explain() {
        if (colon != -1)
            return new InvalidTermException(in, colon, "Unknown prefix");
        return new InvalidTermException(in, begin, "No RDF term/var");
    }

    private String expandPrefixed(int pos, PrefixMap prefixMap) {
        String iri = prefixMap.expandPrefixed(in, pos, colon, termEnd);
        if (iri == null)
            throw new InvalidTermException(in, pos, "Unknown prefix");
        return iri;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean parsePrefixed(int p) {
        int i = skip(in, p, end, PN_PREFIX);
        if (i < end && in.charAt(i) == ':') {
            colon = i;
            boolean bNode = i == p+1 && in.charAt(p) == '_';
            if (!bNode && prefixMap.uri(in, p, i) == null) {
                termEnd = begin; // unmapped prefix
                return true;
            }
            while (true) {
                i = skip(in, i, end, PN_LOCAL);
                if (i >= end || in.charAt(i) != '\\')
                    break;
                i += 2;
            }
            //un-parse unescaped trailing '.'
            for (int b = colon+2; i > b && in.charAt(i-1) == '.' && in.charAt(i-2) != '\\';)
                --i;
            // remember that if we parsed a xsd:string datatype
            if (p>begin && colon==p+3 && in.regionMatches(p, "xsd:string", 0, 10))
                dt = string;
            termEnd = i;
            return true;
        }
        return false;
    }

    private boolean parseBool(Term.Lit b) {
        String lex = b.lexical();
        int len = lex.length();
        if (in.regionMatches(begin, lex, 0, len)) {
            int i = begin+len;
            char f = i < end ? in.charAt(i) : ' ';
            if ((BOOL_FOLLOW[f>>6] & (1L << f)) != 0) {
                termEnd = i;
                term = b;
                return true;
            }
        }
        return false;
    }
    private static final long[] A_FOLLOW = alphabet(",.;/(){}[]!=<>").whitespace().control().get();
    private boolean parseRdfType() {
        if (begin < end && in.charAt(begin) == 'a') {
            char n = begin+1 >= end ? '.' : in.charAt(begin+1);
            if ((A_FOLLOW[n>>6] & (1L << n)) != 0) {
                term = Term.IRI.type;
                termEnd = begin+1;
                return true;
            }
        }
        return false;
    }

    private void parseNumber() {
        byte dot = 0, exp = 0, expSig = 0;
        int p = begin + switch (in.charAt(begin)) {
            case '+', '-' -> 1;
            default       -> 0;
        };
        for (; p < end; ++p) {
            char c = in.charAt(p);
            boolean quit = switch (c) {
                case '.' -> exp == 1 || dot++ == 1;
                case 'e', 'E' -> exp++ == 1;
                case '+', '-' -> exp == 0 || expSig++ == 1;
                default -> c < '0' || c > '9';
            };
            if (quit)
                break;
        }
        if (p <= end && p > begin && in.charAt(p-1) == '.') {
            --p;
            --dot;
        }
        termEnd = p;
        if (exp == 1)
            dt = DOUBLE;
        else if (dot >= 1)
            dt = decimal;
        else if (exp == 0 && expSig == 0 && dot == 0)
            dt = integer;
        else
            termEnd = begin;
    }

    private void escapeForSingleDQuote(StringBuilder sb) {
        for (int i = begin+qLen; i < lexEnd; i++) {
            char c = in.charAt(i);
            switch (c) {
                case '\\' -> sb.append('\\').append(in.charAt(++i));
                case  '"' -> sb.append('\\').append('"');
                case '\r' -> sb.append('\\').append('r');
                case '\n' -> sb.append('\\').append('n');
                default   -> sb.append(c);
            }
        }
    }
}
