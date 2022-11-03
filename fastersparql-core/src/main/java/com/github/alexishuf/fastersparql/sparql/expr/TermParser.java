package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.UUID;

import static com.github.alexishuf.fastersparql.client.util.Skip.*;
import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.FALSE;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.TRUE;

public final class TermParser {
    private static final long[] PREFIXED = alphabet(":").digits().letters().get();

    /** {@link TermParser#parse(String, int, int[])} with {@code start=0} and {@code outPos=null}*/
    public static Term parse(String in) {
        return parse(in, 0, null);
    }

    /**
     * Parse a {@link Term} starting at index {@code start} of {@code in}
     *
     * @param in the string containing the term in N-Triples or Turtle syntax
     * @param start index into string of the first char of the term
     * @param outPos if non-null, {@code outPos} receives the index of the first char after
     *               the term (i.e., its end) and where further parsing of an expression/SPARQL
     *               query may continue.
     *
     * @throws InvalidTermException if there is a syntax error or {@code in} is {@code null}/empty.
     * @return A non-null {@link Term} parsed from the SPARQL string
     */
    public static Term parse(String in, int start, int @Nullable [] outPos) {
        int len = in == null ? 0 : in.length(), p = start;
        if (len == 0)
            throw new InvalidTermException(null, 0, "Cannot parse a term out of null/\"\"");
        try {
            char fst = in.charAt(p++);
            return switch (fst) {
                case '_' -> {
                    if (in.charAt(p++) != ':')
                        throw new InvalidTermException(in, start +1, ':');
                    yield new Term.BNode(in.substring(start, p = nameEnd(in, p, len, BN_LABEL)));
                }
                case '[' -> {
                    while (in.charAt(p) <= ' ') ++p;
                    if (in.charAt(p) != ']')
                        throw new InvalidTermException(in, p, ']');
                    yield new Term.BNode("_:"+ UUID.randomUUID());
                }
                case '<' -> {
                    p = skip(in, ++p, len, SparqlSkip.IRIREF);
                    if (p >= len || in.charAt(p) != '>')
                        throw new InvalidTermException(in, start, "No matching >");
                    yield new Term.IRI(in.substring(start, ++p));
                }
                case '"', '\'' -> {
                    int qLen = p+1 < len && in.charAt(p)==fst && in.charAt(p+1)==fst ? 3 : 1;
                    String quote = qLen==3 ? (fst=='"' ? "\"\"\"" : "'''") : (fst=='"' ? "\"" : "'");
                    p = start +qLen;
                    int endLex = in.indexOf(quote, p);
                    while (endLex != -1) {
                        while (endLex+qLen < len && in.charAt(endLex+qLen) == fst) ++endLex;
                        int escapes = 0;
                        for (int i = endLex-1, startLex = start+qLen; i >= startLex; i--) {
                            if (in.charAt(i) == '\\') ++escapes;
                            else break;
                        }
                        if (escapes % 2 == 0) break;
                        endLex = in.indexOf(quote, endLex+1);
                    }
                    if (endLex == -1)
                        throw new InvalidTermException(in, p, "Unclosed "+quote);
                    p = endLex+qLen;
                    String dt = string, lang = null;
                    if (p < len) {
                        if (in.charAt(p) == '@') {
                            ++p;
                            dt = langString;
                            int langStart = p;
                            p = nameEnd(in, p, len, LANGTAG);
                            lang = in.substring(langStart, p).intern();
                        } else if (in.charAt(p) == '^') {
                            ++p; // consume first ^
                            if (in.charAt(p) != '^')
                                throw new InvalidTermException(in, p, "Expected ^^");
                            ++p; // consume second ^
                            if (in.charAt(p) == '<') {
                                int e = in.indexOf('>', p);
                                if (e == -1)
                                    throw new InvalidTermException(in, p, "Missing >");
                                dt = in.substring(p+1, e).intern();
                                p = e+1; //consume ...>
                            } else {
                                int dtLen = skip(in, p, len, PREFIXED) - p;
                                dt = expandPrefixed(in, p, p+dtLen);
                                if (dt == null)
                                    throw new InvalidTermException(in, p, "Not a datatype IRI");
                                p += dtLen;
                            }
                        }
                    }
                    if (RDFTypes.isXsd(dt, RDFTypes.BOOLEAN))
                        yield in.charAt(start +qLen) == 't' ? TRUE : FALSE;
                    yield new Term.Lit(in.substring(start +qLen, endLex), dt, lang);
                }
                case 't' -> {
                    if (!in.regionMatches(start+1, "rue", 0, 3))
                        throw new InvalidTermException(in, start, "Expected true");
                    p += 3;
                    yield TRUE;
                }
                case 'f' -> {
                    if (!in.regionMatches(start+1, "alse", 0, 4))
                        throw new InvalidTermException(in, start, "Expected false");
                    p += 4;
                    yield FALSE;
                }
                case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', '-', '+' -> {
                    byte dot = 0, exp = 0, expSig = 0;
                    for (; p < len; ++p) {
                        char c = in.charAt(p);
                        boolean quit = switch (c) {
                            case '.'      -> exp == 1 || dot++ == 1;
                            case 'e', 'E' -> exp++ == 1;
                            case '+', '-' -> exp == 0 || expSig++ == 1;
                            default       -> c < '0' || c > '9';
                        };
                        if (quit) { break; }
                    }
                    if (p <= len && in.charAt(p-1) == '.')
                        --p;
                    String lexical = in.substring(start, p);
                    if (exp != 0)
                        yield new Term.Lit(lexical, RDFTypes.DOUBLE, null);
                    else if (dot != 0)
                        yield new Term.Lit(lexical, RDFTypes.decimal, null);
                    else
                        yield new Term.Lit(lexical, RDFTypes.integer, null);
                }
                case '?', '$'
                        -> new Term.Var(in.substring(start, p = nameEnd(in, p, len, VARNAME)));
                default -> throw new InvalidTermException(in, p, "Expected a var or RDF value");
            };
        } catch (StringIndexOutOfBoundsException t) {
            throw new InvalidTermException(in, p, "Reached end before completing term");
        } finally {
            if (outPos != null)
                outPos[0] = p;
        }
    }

    private static int nameEnd(String in, int p, int len, long[] alphabet) {
        int first = p;
        p = skip(in, p, len, alphabet);
        while (p > first && in.charAt(p-1) == '.') --p;
        if (p == first)
            throw new InvalidTermException(in, p, "Empty name");
        return p;
    }
}
