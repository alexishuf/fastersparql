package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.DT_MID;
import static com.github.alexishuf.fastersparql.model.rope.RopeDict.*;
import static com.github.alexishuf.fastersparql.model.rope.RopeWrapper.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.PN_LOCAL_LAST;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.stream;

@SuppressWarnings("SpellCheckingInspection")
public final class Term extends Rope implements Expr {
    public static final int  SUFFIX_MASK   = 0x80000000;
    public static final long SUFFIX_MASK_L = 0x80000000L;

    public static final Term FALSE = new Term(DT_BOOLEAN|SUFFIX_MASK, "\"false".getBytes(UTF_8));
    public static final Term TRUE = new Term(DT_BOOLEAN|SUFFIX_MASK, "\"true".getBytes(UTF_8));
    public static final Term EMPTY_STRING = new Term(0, "\"\"".getBytes(UTF_8));

    public static final byte[] CLOSE_IRI = {'>'};
    public static final Term XSD = new Term(P_XSD, CLOSE_IRI);
    public static final Term RDF = new Term(P_RDF, CLOSE_IRI);

    public static final Term XSD_DURATION = new Term(P_XSD, "duration>".getBytes(UTF_8));
    public static final Term XSD_DATETIME = new Term(P_XSD, "dateTime>".getBytes(UTF_8));
    public static final Term XSD_TIME = new Term(P_XSD, "time>".getBytes(UTF_8));
    public static final Term XSD_DATE = new Term(P_XSD, "date>".getBytes(UTF_8));
    public static final Term XSD_GYEARMONTH = new Term(P_XSD, "gYearMonth>".getBytes(UTF_8));
    public static final Term XSD_GYEAR = new Term(P_XSD, "gYear>".getBytes(UTF_8));
    public static final Term XSD_GMONTHDAY = new Term(P_XSD, "gMonthDay>".getBytes(UTF_8));
    public static final Term XSD_GDAY = new Term(P_XSD, "gDay>".getBytes(UTF_8));
    public static final Term XSD_GMONTH = new Term(P_XSD, "gMonth>".getBytes(UTF_8));
    public static final Term XSD_BOOLEAN = new Term(P_XSD, "boolean>".getBytes(UTF_8));
    public static final Term XSD_BASE64BINARY = new Term(P_XSD, "base64Binary>".getBytes(UTF_8));
    public static final Term XSD_HEXBINARY = new Term(P_XSD, "hexBinary>".getBytes(UTF_8));
    public static final Term XSD_FLOAT = new Term(P_XSD, "float>".getBytes(UTF_8));
    public static final Term XSD_DECIMAL = new Term(P_XSD, "decimal>".getBytes(UTF_8));
    public static final Term XSD_DOUBLE = new Term(P_XSD, "double>".getBytes(UTF_8));
    public static final Term XSD_ANYURI = new Term(P_XSD, "anyURI>".getBytes(UTF_8));
    public static final Term XSD_STRING = new Term(P_XSD, "string>".getBytes(UTF_8));
    public static final Term XSD_INTEGER = new Term(P_XSD, "integer>".getBytes(UTF_8));
    public static final Term XSD_NONPOSITIVEINTEGER = new Term(P_XSD, "nonPositiveInteger>".getBytes(UTF_8));
    public static final Term XSD_LONG = new Term(P_XSD, "long>".getBytes(UTF_8));
    public static final Term XSD_NONNEGATIVEINTEGER = new Term(P_XSD, "nonNegativeInteger>".getBytes(UTF_8));
    public static final Term XSD_NEGATIVEINTEGER = new Term(P_XSD, "negativeInteger>".getBytes(UTF_8));
    public static final Term XSD_INT = new Term(P_XSD, "int>".getBytes(UTF_8));
    public static final Term XSD_UNSIGNEDLONG = new Term(P_XSD, "unsignedLong>".getBytes(UTF_8));
    public static final Term XSD_POSITIVEINTEGER = new Term(P_XSD, "positiveInteger>".getBytes(UTF_8));
    public static final Term XSD_SHORT = new Term(P_XSD, "short>".getBytes(UTF_8));
    public static final Term XSD_UNSIGNEDINT = new Term(P_XSD, "unsignedInt>".getBytes(UTF_8));
    public static final Term XSD_BYTE = new Term(P_XSD, "byte>".getBytes(UTF_8));
    public static final Term XSD_UNSIGNEDSHORT = new Term(P_XSD, "unsignedShort>".getBytes(UTF_8));
    public static final Term XSD_UNSIGNEDBYTE = new Term(P_XSD, "unsignedByte>".getBytes(UTF_8));
    public static final Term XSD_NORMALIZEDSTRING = new Term(P_XSD, "normalizedString>".getBytes(UTF_8));
    public static final Term XSD_TOKEN = new Term(P_XSD, "token>".getBytes(UTF_8));
    public static final Term XSD_LANGUAGE = new Term(P_XSD, "language>".getBytes(UTF_8));

    public static final Term RDF_LANGSTRING = new Term(P_RDF, "langString>".getBytes(UTF_8));
    public static final Term RDF_HTML = new Term(P_RDF, "HTML>".getBytes(UTF_8));
    public static final Term RDF_XMLLITERAL = new Term(P_RDF, "XMLLiteral>".getBytes(UTF_8));
    public static final Term RDF_JSON = new Term(P_RDF, "JSON>".getBytes(UTF_8));

    public static final Term RDF_TYPE = new Term(P_RDF, new ByteRope("type>").utf8);
    public static final Term RDF_FIRST = new Term(P_RDF, new ByteRope("first>").utf8);
    public static final Term RDF_REST = new Term(P_RDF, new ByteRope("rest>").utf8);
    public static final Term RDF_NIL = new Term(P_RDF, new ByteRope("nil>").utf8);
    public static final Term RDF_VALUE = new Term(P_RDF, new ByteRope("value>").utf8);
    public static final Term RDF_PROPERTY = new Term(P_RDF, new ByteRope("Property>").utf8);
    public static final Term RDF_LIST = new Term(P_RDF, new ByteRope("List>").utf8);
    public static final Term RDF_BAG = new Term(P_RDF, new ByteRope("Bag>").utf8);
    public static final Term RDF_SEQ = new Term(P_RDF, new ByteRope("Seq>").utf8);
    public static final Term RDF_ALT = new Term(P_RDF, new ByteRope("Alt>").utf8);
    public static final Term RDF_STATEMENT = new Term(P_RDF, new ByteRope("Statement>").utf8);
    public static final Term RDF_SUBJECT = new Term(P_RDF, new ByteRope("subject>").utf8);
    public static final Term RDF_PREDICATE = new Term(P_RDF, new ByteRope("predicate>").utf8);
    public static final Term RDF_OBJECT = new Term(P_RDF, new ByteRope("object>").utf8);
    public static final Term RDF_DIRECTION = new Term(P_RDF, new ByteRope("direction>").utf8);

    private static final Term[] SRT_RDF = new Term[] {
            RDF_ALT,
            RDF_BAG,
            RDF_HTML,
            RDF_JSON,
            RDF_LIST,
            RDF_PROPERTY,
            RDF_SEQ,
            RDF_STATEMENT,
            RDF_XMLLITERAL,
            RDF_DIRECTION,
            RDF_FIRST,
            RDF_LANGSTRING,
            RDF_NIL,
            RDF_OBJECT,
            RDF_PREDICATE,
            RDF_REST,
            RDF_SUBJECT,
            RDF_TYPE,
            RDF_VALUE,
    };

    private static final Term[] SRT_XSD = new Term[] {
            XSD_ANYURI,
            XSD_BASE64BINARY,
            XSD_BOOLEAN,
            XSD_BYTE,
            XSD_DATE,
            XSD_DATETIME,
            XSD_DECIMAL,
            XSD_DOUBLE,
            XSD_DURATION,
            XSD_FLOAT,
            XSD_GDAY,
            XSD_GMONTH,
            XSD_GMONTHDAY,
            XSD_GYEAR,
            XSD_GYEARMONTH,
            XSD_HEXBINARY,
            XSD_INT,
            XSD_INTEGER,
            XSD_LANGUAGE,
            XSD_LONG,
            XSD_NEGATIVEINTEGER,
            XSD_NONNEGATIVEINTEGER,
            XSD_NONPOSITIVEINTEGER,
            XSD_NORMALIZEDSTRING,
            XSD_POSITIVEINTEGER,
            XSD_SHORT,
            XSD_STRING,
            XSD_TIME,
            XSD_TOKEN,
            XSD_UNSIGNEDBYTE,
            XSD_UNSIGNEDINT,
            XSD_UNSIGNEDLONG,
            XSD_UNSIGNEDSHORT
    };

    public static final Term[] FREQ_XSD_DT = new Term[] {
            XSD_INTEGER,
            XSD_DECIMAL,
            XSD_BOOLEAN,
            XSD_STRING,
            XSD_INT,
            XSD_LONG,
            XSD_DOUBLE,
            XSD_FLOAT,

            XSD_DATE,
            XSD_DATETIME,
            XSD_DURATION,
            XSD_TIME,

            XSD_ANYURI,
            XSD_BASE64BINARY,
            XSD_HEXBINARY,

            XSD_BYTE,
            XSD_SHORT,
            XSD_NEGATIVEINTEGER,
            XSD_NONNEGATIVEINTEGER,
            XSD_NONPOSITIVEINTEGER,
            XSD_NORMALIZEDSTRING,
            XSD_POSITIVEINTEGER,
            XSD_UNSIGNEDBYTE,
            XSD_UNSIGNEDINT,
            XSD_UNSIGNEDLONG,
            XSD_UNSIGNEDSHORT,

            XSD_GDAY,
            XSD_GMONTH,
            XSD_GMONTHDAY,
            XSD_GYEAR,
            XSD_GYEARMONTH,
            XSD_LANGUAGE,
            XSD_TOKEN
    };

    public static final int[] FREQ_XSD_DT_ID = {
            DT_integer,
            DT_decimal,
            DT_BOOLEAN,
            DT_string,
            DT_INT,
            DT_LONG,
            DT_DOUBLE,
            DT_FLOAT,

            DT_date,
            DT_dateTime,
            DT_duration,
            DT_time,

            DT_anyURI,
            DT_base64Binary,
            DT_hexBinary,

            DT_BYTE,
            DT_SHORT,
            DT_negativeInteger,
            DT_nonNegativeInteger,
            DT_nonPositiveInteger,
            DT_normalizedString,
            DT_positiveInteger,
            DT_unsignedByte,
            DT_unsignedInt,
            DT_unsignedLong,
            DT_unsignedShort,

            DT_gDay,
            DT_gMonth,
            DT_gMonthDay,
            DT_gYear,
            DT_gYearMonth,
            DT_language,
            DT_token
    };

    static {
        assert stream(SRT_RDF).map(Rope::toString).sorted().toList()
                .equals(stream(SRT_RDF).map(Rope::toString).toList())
                : "SORTED_RDF_TERMS is not sorted";
        assert stream(SRT_XSD).map(Rope::toString).sorted().toList()
                .equals(stream(SRT_XSD).map(Rope::toString).toList())
                : "SORTED_XSD_TERMS is not sorted";

        Map<Byte, Integer> freq = new HashMap<>();
        stream(SRT_RDF).forEach(t -> freq.put(t.local[0], freq.getOrDefault(t.local[0], 0)+1));
        assert freq.values().stream().noneMatch(e -> e > 8)
                : "There are local[0] values with more than 8 occurrences in SRT_RDF ";

        freq.clear();
        stream(SRT_XSD).forEach(t -> freq.put(t.local[0], freq.getOrDefault(t.local[0], 0)+1));
        assert freq.values().stream().noneMatch(e -> e > 8)
                : "There are local[0] values with more than 8 occurrences in SRT_XSD ";
    }

    private static final byte[] INTERN_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes(UTF_8);
    private static final int INTERN_W = 62;
    private static final Term[][] PLAIN = {
            new Term[INTERN_W],
            new Term[INTERN_W*INTERN_W],
    };
    private static final byte[][][] IRI_LOCALS = {
            new byte[INTERN_W][],
            new byte[INTERN_W*INTERN_W][],
     };

    static {
        assert INTERN_W == INTERN_ALPHABET.length;
        for (int i0 = 0; i0 < INTERN_W; i0++) {
            char c0 = (char) INTERN_ALPHABET[i0];
            Term t = new Term(0, ("\"" + c0 + "\"").getBytes(UTF_8));
            byte[] l = (c0+">").getBytes(UTF_8);
            PLAIN[0][internIdx(t.local, 1, 1)] = t;
            IRI_LOCALS[0][internIdx(l, 0, 1)] = l;
            for (int i1 = 0; i1 < INTERN_W; i1++) {
                char c1 = (char) INTERN_ALPHABET[i1];
                t = new Term(0, ("\""+c0+c1+"\"").getBytes(UTF_8));
                l = (String.valueOf(c0)+c1+">").getBytes(UTF_8);
                PLAIN[1][internIdx(t.local, 1, 2)] = t;
                IRI_LOCALS[1][internIdx(l, 0, 2)] = l;
            }
        }
    }

    private static int internCharIdx(Object src, int i) {
        byte c = get(src, i);
        if      (c >= 'a') return c > 'z' ? -1 : 36-'a'+c;
        else if (c >= 'A') return c > 'Z' ? -1 : 10-'A'+c;
        else if (c >= '0') return c > '9' ? -1 : c-'0';
        return -1;
    }

    private static int internIdx(Object src, int begin, int n) {
        return switch (n) {
            case -2, -1, 0 -> -2;
            case 1 -> internCharIdx(src, begin);
            case 2 -> {
                int i0 = internCharIdx(src, begin), i1 = internCharIdx(src, begin+1);
                yield i0 < 0 || i1 < 0 ? -1 : i0*INTERN_W + i1;
            }
            default -> -1;
        };
    }

    private static byte[] internIriLocal(Object src, int begin, int end) {
        int n = end-begin;
        if (n <= 3) {
            if (get(src, end-1) == '>') --n;
            int i = internIdx(src, begin, n);
            if      (i == -2) return CLOSE_IRI;
            else if (i >= 0) return IRI_LOCALS[n-1][i];
        }
        return asCloseIriU8(src, begin, end);
    }

    private static Term internPlain(Object src, int begin, int end) {
        int n = end-begin-2;
        if (n <= 2) {
            int i = internIdx(src, begin+1, n);
            if      (i == -2) return EMPTY_STRING;
            else if (i >=  0) return PLAIN[n-1][i];
        }
        return new Term(0, asLitU8(src, begin, end));
    }

    /**
     * Gets the term in {@code sorted} whose local name matches
     * {@code [localBegin, localBegin+localLen)} in {@code utf8} or {@code null} if no such
     * term exists
     */
    private static Term intern(int id, Rope rope, int localBegin, int localLen) {
        Term[] sorted = id == P_XSD ? SRT_XSD : id == P_RDF ? SRT_RDF : null;
        if (sorted != null) {
            if (localLen == 0 || (localLen == 1 && rope.get(localBegin) == '>'))
                return id == P_XSD ? XSD : RDF;
            int b = 0, e = sorted.length;
            byte first = rope.get(localBegin);
            Term t;
            // binary search for a range < 8
            for (int m = e >> 1; e - b > 8; m = (b + e) >> 1) {
                byte actual = sorted[m].local[0];
                if (actual < first) {
                    b = m;
                } else if (actual > first) {
                    e = m;
                } else {
                    b = e = m;
                    while (b > 0 && sorted[b-1].local[0] == first) --b;
                    while (e < sorted.length && sorted[e].local[0] == first) ++e;
                    break;
                }
            }
            //linear search
            if (rope.get(localBegin + localLen - 1) != '>')
                return internUnclosed(id, sorted, rope, localBegin, localLen, first, b, e);
            for (byte c = -1; c <= first && b < e; ++b) {
                c = (t = sorted[b]).local[0];
                if (c == first && t.local.length == localLen && rope.has(localBegin, t.local))
                    return t;
            }
        }
        return new Term(id, rope.toArray(localBegin, localBegin+localLen));
    }

    private static Term internUnclosed(int id, Term[] sorted, Rope rope, int localBegin,
                                       int localLen, byte first, int b, int e) {
        int base = RopeDict.get(id).len, localEnd = localBegin+localLen;
        Term t;
        for (byte c = -1; c <= first && b < e; ++b) {
            c = (t = sorted[b]).local[0];
            if (c == first && t.local.length == localLen + 1 && t.has(base, rope, localBegin, localEnd))
                return t;
        }
        return new Term(id, asCloseIriU8(rope, localBegin, localBegin+localLen));
    }

    /** If negative, {@code RopeDict.get(0x7fffffff&flaggedDictId)} is prefixed to the
     *  local bytes. If positive, {@code RopeDict.get(flaggedDictId)} is suffixed. If zero, the
     *  local bytes are the whole Term representation. */
    public final int flaggedDictId;
    /** Array containing UTF-8 bytes to be prefixed or suffixed to {@code flaggedDictId} */
    public final byte[] local;

    private @Nullable Number number;
    private int hash;

    private Term(int flaggedDictId, byte[] local) {
        super(local.length + RopeDict.getTolerant(flaggedDictId).len);
        this.flaggedDictId = flaggedDictId;
        this.local = local;
        assert isValidTermConstruction(flaggedDictId, local) : "invalid term";
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean isValidTermConstruction(int flaggedId, byte[] local) {
        if (local == null)
            return false; // local/localOff/localLen issues
        if (local.length == 0)
            return false; // local cannot be empty, local[0] is used for type detection
        if (flaggedId == SUFFIX_MASK)
            return false; // "suffix id zero" is not a valid flaggedId
        if (flaggedId == (SUFFIX_MASK|DT_langString))
            return false; // lang-tagged literals do not take "^^rdf:langString" suffix
        if ((flaggedId == P_XSD || flaggedId == P_RDF)
                && new ByteRope(local).skip(0, local.length, ALPHANUMERIC) != local.length-1)
            return false; // no weird chars in xsd: or rdf: iris
        if (len() < 2)
            return false; // too short for an RDF term
        if ("<\"?$_".indexOf((char) get(0)) < 0)
            return false; // bad first char
        ByteRope suffix = RopeDict.getTolerant(flaggedId);
        if (suffix.offset != 0)
            return false; // RopeDict ByteRopes MUST have offset==0
        if (flaggedId > 0 && suffix.get(0) != '<')
            return false; // IRI prefix does start with '<'
        if (flaggedId < 0) {
            if (!suffix.has(0, DT_MID))
                return false; // literal suffix does not start with \"^^
            if (suffix.get(suffix.len-1) != '>')
                return false; // literal suffix does not end with '>'
        }
        if (flaggedId == (DT_string|SUFFIX_MASK))
            return false; // should store "lexical", not "lexical"^^<...#string>

        if (get(0) == '<') { // IRI
            if (get(len()-1) != '>')
                return false; // no closing '>'
            if (len() > 2 && get(len()-2) == '>')
                return false; // something went wrong upstream: IRI ending in >>
            if (len() > 2 && get(1) == '<')
                return false; // something went wrong upstream: IRI starting with <<
            if (skipUntil(1, len()-1, '<', '>') != len()-1)
                return false; // <> within IRI
            if (skip(0, len(), UNTIL_SPECIAL_WS) != len())
                return false; // suspicious non-' ' whitespace. ' ' is invalid but tolerable
        } else if (get(0) == '"') { // literal
            int endLex = reverseSkip(0, len(), UNTIL_DQ);
            if (endLex == 0) {
                return false; // no closing '"'
            } else if (endLex+1 < len()) {
                if (get(endLex+1) == '@') {
                    if (len() == endLex + 2)
                        return false; // empty lang tag
                    if (skip(endLex + 2, len(), SparqlSkip.LANGTAG) < len())
                        return false; // invalid lang tag
                } else if (endLex+3 >= len() || get(endLex+1) != '^' || get(endLex+2) != '^' || get(endLex+3) != '<') {
                    return false; // expected ^^< after closing '"'
                } else if (get(len()-1) != '>') {
                    return false; // no closing '>' in typed literal
                }
            }
            if (!validEscapes(endLex))
                return false;
        } else if (get(0) == '?' || get(0) == '?') {
            if (skip(1, len(), SparqlSkip.VARNAME) < len())
                return false; // bad char in var name
        } else if (get(0) == '_') {
            if (get(1) != ':')
                return false; // expected ':'
            if (skip(2, len(), SparqlSkip.BN_LABEL) < len())
                return false; // invalid character in bnode label
        }
        return true;
    }

    private boolean validEscapes(int lexEnd) {
        for (int i = 1; i < lexEnd; i = skip(i+1, lexEnd, UNTIL_DQ_OR_BACKSLASH)) {
            byte c = get(i);
            if      (c ==  '"') return false;
            else if (c == '\\') ++i;
        }
        return true;
    }

    /**
     * Create a Term from the SPARQL var or N-Triples UTF-8 string in the
     * {@code [offset, offset+len)} range of {@code utf8}.
     *
     * <p>{@code utf8} will be kept by reference, thus changes to the bytes will be visible
     * from the returned {@link Term} instance.</p>
     *
     * In the general case, prefer using {@link Term#valueOf(Rope)} and its overloads, as
     * {@code valueOf()} will try to use {@link RopeDict} compression and may also intern some
     * frequently used known {@link Term} instances.
     *
     * @param utf8 Array of UTF-8 bytes
     * @return a new {@link Term instance}
     */
    public static Term wrap(byte[] utf8) {
        return new Term(0, utf8);
    }

    /**
     * Create a Term for an N-Triples plain or lang-tagged string in {@code r.sub(begin, end)}.
     */
    public static Term copy(Rope r, int begin, int end) {
        return r.get(end-1) == '"' && end-begin <= 4 ? internPlain(r, begin, end)
                                                     : new Term(0, r.toArray(begin, end));
    }

    /**
     * Equivalent to {@code wrap(r.utf8)}, cropping {@code r.utf8} if necessary.
     *
     * <p><strong>Important:</strong> {@code r.utf8} is taken by reference, thus changes to
     * {@code r} will be visible in ther {@link Term} instance.</p>
     */
    public static Term wrap(ByteRope r) {
        byte[] u8 = r.utf8;
        if (r.len != u8.length)
            u8 = Arrays.copyOf(u8, r.len);
        return new Term(0, u8);
    }

    /**
     * Create a {@link Term} instance using a suffix/prefix id and copying the local part
     * from {@code source}.
     *
     * <p>In the general case, this will simply call {@link Term#Term(int, byte[])}.
     * However, short local parts (up to 2 bytes when using a suffix/prefix or 3 bytes
     * when not using one), instead of creating a new {@code byte[]} instance, a
     * {@code byte[]} instance will be used from a static-initialized set.</p>
     *
     * @param flaggedId the suffix/prefix id. {@code flaggedDictId&0x7fffffff} is the id
     *                      in {@link RopeDict#get(int)}, while the most significant bit indicates
     *                      whether this is a prefix (0) or a suffix (1). If
     *                      {@code flaggedDictId == 0}, no prefix/suffix will be used and
     *                      {@code source} contains a full N-Triples
     * @param src from where the local part of the {@link Term} shall be copied
     * @param off index of the first byte that is part of the term
     * @param len number of bytes which are part of this {@link Term#local}
     * @return a new {@link Term} instance, which may be interned.
     */
    public static Term make(int flaggedId, byte[] src, int off, int len) {
        int end = off+len;
        if (flaggedId == 0 || flaggedId == (DT_string|SUFFIX_MASK)) {
            if (len == 0) return null;
            return src[end-1] == '"' && len <= 4 ? internPlain(src, off, end)
                                                 : new Term(0, copyOfRange(src, off, end));
        } else if (flaggedId == (DT_BOOLEAN|SUFFIX_MASK)) {
            return switch (src[off+1]) {
                case 't' -> TRUE;
                case 'f' -> FALSE;
                default -> throw new InvalidTermException(new String(src, off, len, UTF_8), 1, "expected true or false for ^^xsd:boolean");
            };
        } else if (flaggedId == P_XSD || flaggedId == P_RDF) {
            return intern(flaggedId, new ByteRope(src), off, len);
        }
        byte[] local = flaggedId > 0 ? internIriLocal(src, off, end) : copyOfRange(src, off, end);
        return new Term(flaggedId, local);
    }

    /**
     * Create a {@link Term} for a typed literal.
     *
     * <p><strong>Important: </strong>{@code lex} is kept by reference, thus changes to
     * it will be visible in the {@link Term} instance.</p>
     *
     * @param lex UTF-8 array containing the escaped (as enclosed in {@code ""}) lexical
     *            form, maybe surrounded by '"'. If suffixId is zero or {@code DT_string},
     *            lex SHOULD be enclosed by '"'. Else, {@code lex} SHOULD start with a single '"',
     *            not including the closing '"', which is part of the suffixId. If {@code lex} is
     *            not quoted as it should, quotes will be inserted.
     * @param offset Index of the first byte in lex to consider
     * @param len Size of the lexical form (or {@code "\""+lexicalForm}) in bytes.
     * @param suffixId Value such that suffixing the given range inside
     *                 {@code lex} with {@link RopeDict#get(int)} yields a
     *                 valid N-Triples serialization of the term.
     * @return A {@link Term} for the term. {@code lex} is held by reference,
     *         thus it MUST not be mutated, else changes might feed users of the {@link Term}
     *         with garbage.
     */
    public static Term typed(byte[] lex, int offset, int len, int suffixId) {
        if ((suffixId&0x7fffffff) == DT_string || suffixId == 0)
            return typedCold(lex, offset, len);
        var w = forOpenLit(lex, offset, len);
        if (w != NONE)
            lex = w.toArray(lex, offset, offset+len);
        else if (offset > 0 || len != lex.length)
            lex = copyOfRange(lex, offset, len);
        return new Term(suffixId|SUFFIX_MASK, lex);
    }

    /** Equivalent to {@code typed(lex.toARray(begin, end), suffixId)}. */
    public static Term typed(Rope lex, int begin, int end, int suffixId) {
        if ((suffixId&0x7fffffff) == DT_string || suffixId == 0)
            return typedCold(lex.toArray(begin, end), 0, end-begin);
        return new Term(suffixId|SUFFIX_MASK, asOpenLitU8(lex, begin, end));
    }

    private static Term typedCold(byte[] lex, int offset, int len) {
        int e = offset + len;
        return new Term(0, forLit(lex, offset, e).toArray(lex, offset, e));
    }

    /** Equivalent to {@code typed(lex, 0, lex.length, suffixId)} */
    public static Term typed(byte[] lex, int suffixId) {
        return typed(lex, 0, lex.length, suffixId);
    }

    /** Equivalent to {@code typed(lex.toString().getBytes(UTF_8), suffixId)}. */
    public static Term typed(Object lex, int suffixId) {
        if (!(lex instanceof CharSequence) && !(lex instanceof byte[]))
            lex = lex.toString();
        boolean string = suffixId == 0 || (suffixId & 0x7fffffff) == DT_string;
        byte[] u8 = (string ? forLit(lex) : forOpenLit(lex)).toArray(lex);
        return typed(u8, 0, u8.length, suffixId);
    }

    /**
     * Create an N-Triples term for {@code new String(RopeDict.get(prefixId), UTF_8)
     * + new String(suffix, suffixOffset, suffixLen, UTF_8)}.
     *
     * @param prefixId An integer {@code > 0} such that {@link RopeDict#get(int)} yields
     *                 the prefix of the N-Triples term
     * @param suffix {@code byte[]} where there is a UTF-8 representation of the term suffix
     * @return A {@link Term} instance for the term in N-Triples syntax. {@code suffix}
     *         is held by reference, thus changes to it will be reflected on the {@link Term}
     *         and might cause clients of the {@link Term} object to consume garbage.
     */
    public static Term prefixed(int prefixId, byte[] suffix) {
        if (prefixId == P_XSD || prefixId == P_RDF)
            return intern(prefixId, new ByteRope(suffix), 0, suffix.length);
        byte[] l = suffix.length > 4 ? suffix : internIriLocal(suffix, 0, suffix.length);
        return new Term(prefixId, l);
    }

    /**
     * Create a {@link Term} holding the IRI obtained by concatenating
     * {@code RopeDict.get(prefixId)} with {@code suffix.sub(begin, end)} and '>'
     * (if the suffix slice does not already end in {@code >}).
     *
     * <p>Unlike {@link Term#prefixed(int, byte[])}, suffix is not held by reference, thus it
     * can be safely mutated after this call returns. Also note that this method is preferred
     * over the aforementioned one since it will try to use static initialized arrays before
     * copying a new {@code byte[]} out of {@code suffix}.</p>
     *
     *
     * @param prefixId An id such that {@link RopeDict#get(int)} yields the prefix of the IRI.
     *                 Such prefix must start with {@code <} and must not end with {@code >}
     * @param suffix Rope holding the local portion of the IRI
     * @param begin where the local portion of the IRI starts (inclusive) in {@code suffix}
     * @param end where the local portion of the IRI ends (non-inclusive) in {@code suffix}
     * @return a new {@link Term} instance.
     */
    public static Term prefixed(int prefixId, Rope suffix, int begin, int end) {
        if (prefixId == P_XSD || prefixId == P_RDF)
            return intern(prefixId, suffix, begin, end-begin);
        return new Term(prefixId, internIriLocal(suffix, begin, end));
    }

    /** Create an IRI Term for an optionally or partially {@code <>}-wrapped iri. */
    public static Term iri(CharSequence iriCS) {
        return valueOf(forIri(iriCS).toRope(iriCS));
    }

    /** Equivalent to {@code valueOf("\""+escLex+"\"@"+lang)}.*/
    public static Term lang(CharSequence escLex, CharSequence lang) {
        var w = forLit(escLex);
        var r = new ByteRope(escLex.length() + w.extraBytes() + 1 + lang.length());
        return new Term(0, w.append(r, escLex).append('@').append(lang).fitBytes());
    }

    public static Term valueOf(Rope r, int begin, int end) {
        if (r == null || end <= begin)  return null;
        return switch (r.get(begin)) {
            case '"' -> {
                if (end-begin < 2) throw new InvalidTermException("\"", 0, "Unclosed literal");
                long localAndId = internLit(r, begin, end);
                if ((int)localAndId == DT_string)
                    localAndId = ((localAndId>>>32) + 1) << 32;
                else if ((int)localAndId != 0)
                    localAndId |= SUFFIX_MASK_L;
                yield new Term((int)localAndId, r.toArray(begin, (int)(localAndId>>>32)));
            }
            case '<' -> {
                long localAndId = internIri(r, begin, end);
                int id = (int)localAndId;
                if (id == P_RDF || id == P_XSD) {
                    localAndId >>>= 32;
                    yield intern(id, r, (int)localAndId, end-(int)localAndId);
                }
                yield new Term((int)localAndId, internIriLocal(r, (int)(localAndId>>>32), end));
            }
            case '?', '$', '_' -> new Term(0, r.toArray(begin, end));
            default -> throw new InvalidTermException(r.sub(begin, end).toString(), 0, "Does not start with <, \", ?, $ or _");
        };
    }

    /** Equivalent to {@link Term#valueOf(Rope, int, int)} from {@code 0} to {@code r.len()}. */
    public static Term valueOf(Rope r) {
        return r == null ? null : r instanceof Term t ? t : valueOf(r, 0, r.len());
    }

    /** Equivalent to {@link Term#valueOf(Rope)} over the UTF-8 encoding of {@code string}. */
    public static Term valueOf(@Nullable CharSequence string) {
        if (string == null || string.isEmpty())
            return null;
        if (string instanceof Rope)
            return valueOf((Rope) string);
        var br = new ByteRope(string);
        return switch (br.utf8[0]) {
            case '?', '$', '_' -> new Term(0, br.utf8);
            default -> Term.valueOf(br, 0, br.len);
        };
    }


    /** Get an array of terms where the i-th element is the result of {@code valueOf(terms[i])} */
    public static @Nullable Term[] array(Object... terms) {
        if (terms.length == 1 && terms[0] instanceof Collection<?> coll)
            terms = coll.stream().map(i -> i == null ? null : i.toString()).toArray(String[]::new);
        if (terms.length == 1 && terms[0] instanceof Object[] arr)
            terms = arr;
        Term[] a = new Term[terms.length];
        TermParser termParser = new TermParser().eager();
        termParser.prefixMap.add(Rope.of("owl"), Term.iri("http://www.w3.org/2002/07/owl#"));
        termParser.prefixMap.add(Rope.of("foaf"), Term.iri("http://xmlns.com/foaf/0.1/"));
        termParser.prefixMap.add(Rope.of(""), Term.iri("http://example.org/"));
        termParser.prefixMap.add(Rope.of("ex"), Term.iri("http://example.org/"));
        termParser.prefixMap.add(Rope.of("exns"), Term.iri("http://www.example.org/ns#"));
        for (int i = 0; i < terms.length; i++)
            a[i] = terms[i] == null ? null : termParser.parseTerm(Rope.of(terms[i]));
        return a;
    }

    /** Equivalent to {@link Term#array(Object...)} but yields a {@link List} instead of an array. */
    public static List<@Nullable Term> termList(Object... terms) { return Arrays.asList(array(terms)); }
    public static List<@Nullable Term> termList(CharSequence... terms) { return Arrays.asList(array((Object[]) terms)); }

    /**
     * Create a plain string whose lexical for is {@code escapedLex}, which MAY be already
     * surrounded by {@code '"'}s.
     */
    public static Term plainString(Object escapedLex) {
        byte[] u8 = forLit(escapedLex).toArray(escapedLex);
        return internPlain(u8, 0, u8.length);
    }

    /* --- --- --- Rope implementation --- --- --- */

    @Override public @Nullable MemorySegment segment() {
        return flaggedDictId == 0 ? MemorySegment.ofArray(local) : null;
    }

    /* --- --- --- Expr implementation --- --- --- */

    @Override public int argCount() { return 0; }
    @Override public Expr arg(int i) { throw new IndexOutOfBoundsException(i); }
    @Override public Term eval(Binding binding) { return binding.getIf(this); }
    @Override public Expr bound(Binding binding) { return binding.getIf(this); }

    @Override public Vars vars() {
        Rope name = name();
        if (name == null) return Vars.EMPTY;
        var singleton = new Vars.Mutable(1);
        singleton.add(name);
        return singleton;
    }

    /**
     * Get the SPARQL preferred representation of this {@link Term}. {@link Term#RDF_TYPE}
     * becomes "a" and literals typed as XSD integer, decimal double and boolean are replaced by
     * their lexical forms (without quotes and datatype suffix).
     */
    @Override public void toSparql(ByteSink<?> dest, PrefixAssigner assigner) {
        toSparql(dest, assigner, flaggedDictId, local, 0, local.length);
    }

    public static void toSparql(ByteSink<?> dest, PrefixAssigner assigner,
                                int flaggedDictId, byte[] local, int localOff, int localLen) {
        ByteRope shared = RopeDict.getTolerant(flaggedDictId);
        if (flaggedDictId < 0) {
            int id = flaggedDictId & 0x7fffffff;
            if (id == DT_integer || id == DT_decimal || id == DT_DOUBLE || id == DT_BOOLEAN) {
                --localLen;
                ++localOff;
            } else {
                dest.append(local, localOff, localLen); // write "\"LEXICAL_FORM"
                // will finalize writing shared
                localLen = (local = shared.utf8).length;
                localOff = 0;
                // try to find a prefix name for PREFIX in shared="\"^^<PREFIX...>
                long localAndId = internIri(shared, 3, shared.len);
                Rope name = (int) localAndId == 0 ? null
                        : assigner.nameFor(RopeDict.get((int) localAndId));
                if (name != null) { // have a prefix name for the datatype IRI
                    dest.append(DT_MID).append(name).append(':');
                    // do not write trailing '>' and start from where PREFIX ended
                    localLen = localLen - 1 - (localOff = (int) (localAndId >>> 32));
                }
            }
        } else if (flaggedDictId > 0) {
            if (flaggedDictId == P_RDF && localLen == 5
                    && Arrays.equals(local, localOff, localOff+localLen,
                                     Term.RDF_TYPE.local, 0, Term.RDF_TYPE.local.length)) {
                localLen = (local = ByteRope.A.utf8).length;
                localOff = 0;
            } else {
                Rope name = assigner.nameFor(shared);
                if (name == null) {
                    dest.append(shared);
                } else {
                    dest.append(name).append(':');
                    --localLen; // do not write trailing >
                    int last = localOff + localLen - 1;
                    if (localLen > 0 && !Rope.contains(PN_LOCAL_LAST, local[last])) {
                        if (!Rope.isEscaped(local, localOff, last)) {
                            dest.append(local, localOff, localLen - 1)
                                    .append('\\').append(local[last]);
                            localLen = 0;
                        }
                    }
                }
            }
        }
        dest.append(local, localOff, localLen);
    }

    /* --- --- --- term methods --- --- --- */

    public enum Type {
        LIT,
        IRI,
        BLANK,
        VAR
    }

    public boolean isIri() { return flaggedDictId > 0 || local[0] == '<'; }

    public boolean isVar() {
        byte f = flaggedDictId == 0 ? local[0] : 0;
        return f == '?' || f == '$';
    }

    public Type type() {
        if (flaggedDictId > 0) {
            return Type.IRI;
        } else if (flaggedDictId < 0) {
            return Type.LIT;
        }
        return switch (local[0]) {
            case '"'      -> Type.LIT;
            case '_'      -> Type.BLANK;
            case '<'      -> Type.IRI;
            case '?', '$' -> Type.VAR;
            default       -> throw new InvalidTermException(toString(), 0, "bad start");
        };
    }


    /** If this is a var, gets its name (without leading '?'/'$'). Else, return {@code null}. */
    public @Nullable Rope name() {
        byte c = local.length == 0 ? 0 : local[0];
        return c == '?' || c == '$' ? new ByteRope(local, 1, local.length-1) : null;
    }

    /** {@code lang} if this is a literal tagged with {@code @lang}, else {@code null}. */
    public @Nullable Rope lang() {
        if (local[0] != '"' || flaggedDictId != 0) return null;
        int i = RopeSupport.reverseSkip(local, 1, local.length, UNTIL_DQ);
        if (local[i] == '"' && i+1 < local.length && local[i+1] == '@')
            return new ByteRope(local, i+2, local.length-(i+2));
        return null;
    }

    /**
     * If {@link Term#type()} is {@link Type#LIT}, get the {@link RopeDict} {@code DT_} id for the
     * explicit (or implicit {@code DT_string}/{@code DT_langString}) datatype.
     * @return The {@code DT_} id from {@link RopeDict} or zero if this is not a literal.
     */
    public @NonNegative int datatypeId() {
        if (local.length == 0 || local[0] != '"' || flaggedDictId > 0)
            return 0;
        if (flaggedDictId == 0) {
            int i = RopeSupport.reverseSkip(local, 1, local.length, UNTIL_DQ);
            if (i+2 < local.length && local[i+1] == '@') // has @ and >= 1 char after it
                return DT_langString;
            else if (local.length < 2 || local[i] != '"' || i+1 < local.length)
                throw new InvalidTermException(toString(), i, "No closing \" or garbage after it");
            else
                return DT_string;
        }
        return flaggedDictId & 0x7fffffff;
    }

    /**
     * If this is an IRI of an XML schema or RDF datatype, get a {@code DT_} id from
     * {@link RopeDict} such that {@code new String(RopeDict.get(id), UTF_8).equals("\"^^"+this)}.
     *
     * @return the aforementioned {@code id} or zero if {@link RopeDict} is full or if
     *         {@code this} is not an XSD/RDF IRI.
     */
    public @NonNegative int asKnownDatatypeId() {
        if (flaggedDictId == P_XSD) {
            for (int i = 0; i < FREQ_XSD_DT.length; ++i) {
                if (FREQ_XSD_DT[i] == this) return FREQ_XSD_DT_ID[i];
            }
        } else if (flaggedDictId == P_RDF) {
            if      (this == RDF_LANGSTRING) return DT_langString;
            else if (this == RDF_HTML)       return DT_HTML;
            else if (this == RDF_JSON)       return DT_JSON;
            else if (this == RDF_XMLLITERAL) return DT_XMLLiteral;
        }
        return 0;
    }

    public @NonNegative int asDatatypeId() {
        int id = asKnownDatatypeId();
        return id > 0 ? id : asDatatypeIdCold();
    }

    private @NonNegative int asDatatypeIdCold() {
        if (type() != Type.IRI) return 0;
        var r = new ByteRope(3 + len()).append("\"^^").append(this);
        return RopeDict.internDatatype(r, 0, r.len);
    }

    /**
     * Get the (explicit or implicit) datatype IRI or {@code null} if this is not a literal.
     */
    public @Nullable Term datatypeTerm() {
        if (flaggedDictId > 0 || local.length == 0 || local[0] != '"') {
            return null;
        } else if (flaggedDictId < 0) {
            var suffix = RopeDict.get(flaggedDictId & 0x7fffffff);
            return Term.valueOf(suffix, 3/*"^^*/, suffix.len);
        } else {
            int i = RopeSupport.reverseSkip(local, 0, local.length, UNTIL_DQ);
            if      (i+1 == local.length) return XSD_STRING;
            else if (local[i+1] == '@') return RDF_LANGSTRING;
            throw new InvalidTermException(this, i, "Unexpected suffix");
        }
    }

    /**
     * Get the lexical form of the literal with escapes as required by {@code "}-quoted
     * N-Triples literals.
     */
    public @Nullable Rope escapedLexical() {
        if (local.length == 0 || local[0] != '"') return null;
        if (flaggedDictId < 0)
            return new ByteRope(local, 1, local.length-1);
        int endLex = RopeSupport.reverseSkip(local, 1, local.length, UNTIL_DQ);
        if (local.length < 2 || local[endLex] != '"')
            throw new InvalidTermException(toString(), 0, "Unclosed \"");
        return new ByteRope(local, 1, endLex-1);
    }

    /**
     * Create new literal with same datatype as {@code this}, but using the given lexical form
     * {@code lex}.
     *
     * <p>{@code lex}:</p>
     * <ul>
     *     <li><strong>MUST</strong> be {@code \}-escaped for {@code "}-quoted N-Triple literals</li>
     *     <li><strong>MAY</strong> start with a opening {@code "} quote</li>
     *     <li><strong>MAY</strong> end with a closing {@code "} quote</li>
     * </ul>
     *
     * If the opening and closing quotes in {@code lex} do not match what is required by
     * {@code this}, quotes will be inserted/removed.
     *
     * @param lex the new lexical form: MUST be escaped and MAY include opening/closing quotes.
     * @return a Term with given lexical form and this {@link Term} lang or datatype.
     */
    public Term withLexical(Rope lex) {
        byte[] l;
        if (flaggedDictId == 0) {
            var w = forLit(lex);
            if (local[local.length-1] == '"') {
                l = w.toArray(lex);
            } else {
                int tail = RopeSupport.reverseSkip(local, 1, local.length, UNTIL_DQ)+1;
                int tailLen = local.length-tail;
                var tmp = new ByteRope(w.extraBytes() + lex.len() + tailLen);
                l = w.append(tmp, lex).append(local, tail, tailLen).utf8;
            }
        } else if (flaggedDictId < 0) {
            l = forOpenLit(lex).toArray(lex);
        } else {
            throw new InvalidExprTypeException(this, this, "literal");
        }
        return new Term(flaggedDictId, l);
    }

    public static boolean isNumericDatatype(int maybeFlaggedId) {
        int id = maybeFlaggedId & 0x7fffffff;
        return id == DT_INT || id == DT_unsignedShort || id == DT_DOUBLE || id == DT_FLOAT ||
               id == DT_integer || id == DT_positiveInteger || id == DT_nonPositiveInteger ||
               id == DT_nonNegativeInteger || id == DT_unsignedLong || id == DT_decimal ||
               id == DT_LONG || id == DT_unsignedInt || id == DT_SHORT || id == DT_unsignedByte ||
               id == DT_BYTE;
    }


    /** Get the {@link Number} for this term, or {@code null} if it is not a number. */
    public Number asNumber() {
        if (number != null || flaggedDictId >= 0)
            return number;
        int id = flaggedDictId & 0x7fffffff;
        String lexical = new String(local, 1, local.length-1);
        try {
            if (id == DT_INT || id == DT_unsignedShort) {
                number = Integer.valueOf(lexical);
            } else if (id == DT_DOUBLE) {
                number = Double.valueOf(lexical);
            } else if (id == DT_FLOAT) {
                number = Float.valueOf(lexical);
            } else if (id == DT_integer || id == DT_positiveInteger || id ==DT_nonPositiveInteger || id == DT_unsignedLong) {
                number = new BigInteger(lexical);
            } else if (id == DT_decimal) {
                number = new BigDecimal(lexical);
            } else if (id == DT_LONG || id == DT_unsignedInt) {
                number = Long.valueOf(lexical);
            } else if (id == DT_SHORT || id == DT_unsignedByte) {
                number = Short.valueOf(lexical);
            } else if (id == DT_BYTE) {
                number = (byte)Integer.parseInt(lexical);
            }
        } catch (NumberFormatException e) {
            Rope dt = RopeDict.get(id);
            dt = dt.sub(4, dt.len()-5);
            throw new ExprEvalException("Lexical form " + lexical + " is not valid for " + dt + ": " + e.getMessage());
        }
        return number;
    }

    /** Get the {@code int} value of this literal or throw if it is not a numeric literal. */
    public int asInt() {
        Number n = asNumber();
        if (n == null) throw new ExprEvalException(this+" is not a numeric literal");
        long l = n.longValue();
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)
            throw new ExprEvalException(this+" overflows as int");
        return (int)l;
    }

    private Number requireNumeric(String caller) {
        Number n = asNumber();
        if (n == null)
            throw new ExprEvalException(caller + " not defined for " + this);
        return n;
    }
    private static BigDecimal asBigDecimal(Number n, Number other) {
        int scale = Math.max(n     instanceof BigDecimal d ? d.scale() : 0,
                other instanceof BigDecimal d ? d.scale() : 0);
        if (scale == 0)
            scale = 8;
        if (n instanceof BigDecimal d)
            return d.setScale(scale, RoundingMode.HALF_DOWN);
        BigDecimal d = new BigDecimal(n.toString());
        if (d.scale() < scale)
            d = d.setScale(scale, RoundingMode.HALF_DOWN);
        return d;
    }
    private static BigInteger asBigInteger(Number n) {
        return switch (n) {
            case BigInteger b -> b;
            case BigDecimal d -> d.toBigInteger();
            default -> BigInteger.valueOf(n.longValue());
        };
    }

    /** Implements {@link Comparable#compareTo(Object)} for numeric literals */
    public int compareTo(Term rhs) {
        Number l = asNumber(), r = rhs.asNumber();
        if ((l == null) != (r == null)) {
            throw new ExprEvalException("compareTo not defined for "+this+" and "+rhs);
        } else if (l != null) {
            if (l instanceof BigDecimal || r instanceof BigDecimal)
                return asBigDecimal(l, r).compareTo(asBigDecimal(r, l));
            else if (l instanceof BigInteger || r instanceof BigInteger)
                return asBigDecimal(l, r).compareTo(asBigDecimal(r, l));
            else if (l instanceof Double || r instanceof Double || l instanceof Float || r instanceof Float)
                return Double.compare(l.doubleValue(), r.doubleValue());
            else
                return Long.compare(l.longValue(), r.longValue());
        } else {
            return super.compareTo(rhs);
        }
    }

    public Term add(Term rhs) {
        Number l = requireNumeric("add"), r = rhs.requireNumeric("add");
        Number result;
        int datatype;
        if (l instanceof BigDecimal || r instanceof BigDecimal) {
            result = asBigDecimal(l, r).add(asBigDecimal(r, l));
            datatype = DT_decimal;
        } else if (l instanceof BigInteger || r instanceof BigInteger) {
            result = asBigInteger(l).add(asBigInteger(r));
            datatype = DT_integer;
        } else if (l instanceof Double || r instanceof Double) {
            result = l.doubleValue() + r.doubleValue();
            datatype = DT_DOUBLE;
        } else if (l instanceof Float || r instanceof Float) {
            result = l.floatValue() + r.floatValue();
            datatype = DT_FLOAT;
        } else if (l instanceof Long || r instanceof Long) {
            result = l.longValue() + r.longValue();
            datatype = DT_LONG;
        } else if (l instanceof Integer || r instanceof Integer) {
            result = l.intValue() + r.intValue();
            datatype = DT_INT;
        } else if (l instanceof Short || r instanceof Short) {
            result = l.shortValue() + r.shortValue();
            datatype = DT_SHORT;
        } else if (l instanceof Byte || r instanceof Byte) {
            result = l.byteValue() + r.byteValue();
            datatype = DT_BYTE;
        } else {
            result = l.doubleValue() + r.doubleValue();
            datatype = DT_DOUBLE;
        }
        if (result.equals(l)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return typed(u8, 0, u8.length, datatype);
    }

    public Term subtract(Term rhs) {
        Number l = requireNumeric("subtract"), r = rhs.requireNumeric("subtract");
        Number result;
        int datatype;
        if (l instanceof BigDecimal || r instanceof BigDecimal) {
            result = asBigDecimal(l, r).subtract(asBigDecimal(r, l));
            datatype = DT_decimal;
        } else if (l instanceof BigInteger || r instanceof BigInteger) {
            result = asBigInteger(l).subtract(asBigInteger(r));
            datatype = DT_integer;
        } else if (l instanceof Double || r instanceof Double) {
            result = l.doubleValue() - r.doubleValue();
            datatype = DT_DOUBLE;
        } else if (l instanceof Float || r instanceof Float) {
            result = l.floatValue() - r.floatValue();
            datatype = DT_FLOAT;
        } else if (l instanceof Long || r instanceof Long) {
            result = l.longValue() - r.longValue();
            datatype = DT_LONG;
        } else if (l instanceof Integer || r instanceof Integer) {
            result = l.intValue() - r.intValue();
            datatype = DT_INT;
        } else if (l instanceof Short || r instanceof Short) {
            result = l.shortValue() - r.shortValue();
            datatype = DT_SHORT;
        } else if (l instanceof Byte || r instanceof Byte) {
            result = l.byteValue() - r.byteValue();
            datatype = DT_BYTE;
        } else {
            result = l.doubleValue() - r.doubleValue();
            datatype = DT_DOUBLE;
        }
        if (result.equals(l)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return typed(u8, 0, u8.length, datatype);
    }

    public Term negate() {
        Number n = asNumber();
        if (n != null) {
            byte[] u8;
            if (local[1] == '-') {
                u8 = new byte[local.length-1];
                u8[0] = '"';
                arraycopy(local, 2, u8, 1, local.length-2);
            } else {
                u8 = new byte[local.length+1];
                u8[0] = '"';
                u8[1] = '-';
                arraycopy(local, 1, u8, 2, local.length-1);
            }
            return new Term(flaggedDictId, u8);
        }
        return asBool() ? FALSE : TRUE;
    }

    public Term multiply(Term rhs) {
        Number l = requireNumeric("multiply"), r = rhs.requireNumeric("multiply");
        Number result;
        int datatype;
        if (l instanceof BigDecimal || r instanceof BigDecimal) {
            result = asBigDecimal(l, r).multiply(asBigDecimal(r, l));
            datatype = DT_decimal;
        } else if (l instanceof BigInteger || r instanceof BigInteger) {
            result = asBigInteger(l).multiply(asBigInteger(r));
            datatype = DT_integer;
        } else if (l instanceof Double || r instanceof Double) {
            result = l.doubleValue() * r.doubleValue();
            datatype = DT_DOUBLE;
        } else if (l instanceof Float || r instanceof Float) {
            result = l.floatValue() * r.floatValue();
            datatype = DT_FLOAT;
        } else if (l instanceof Long || r instanceof Long) {
            result = l.longValue() * r.longValue();
            datatype = DT_LONG;
        } else if (l instanceof Integer || r instanceof Integer) {
            result = l.intValue() * r.intValue();
            datatype = DT_INT;
        } else if (l instanceof Short || r instanceof Short) {
            result = l.shortValue() * r.shortValue();
            datatype = DT_SHORT;
        } else if (l instanceof Byte || r instanceof Byte) {
            result = l.byteValue() * r.byteValue();
            datatype = DT_BYTE;
        } else {
            result = l.doubleValue() * r.doubleValue();
            datatype = DT_DOUBLE;
        }
        if (result.equals(l)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return typed(u8, 0, u8.length, datatype);
    }

    public Term divide(Term rhs) {
        Number l = requireNumeric("divide"), r = rhs.requireNumeric("divide");
        Number result;
        int datatype;
        if (l instanceof BigDecimal || r instanceof BigDecimal) {
            result = asBigDecimal(l, r).divide(asBigDecimal(r, l), RoundingMode.HALF_DOWN);
            datatype = DT_decimal;
        } else if (l instanceof BigInteger || r instanceof BigInteger) {
            result = asBigInteger(l).divide(asBigInteger(r));
            datatype = DT_integer;
        } else if (l instanceof Double || r instanceof Double) {
            result = l.doubleValue() / r.doubleValue();
            datatype = DT_DOUBLE;
        } else if (l instanceof Float || r instanceof Float) {
            result = l.floatValue() / r.floatValue();
            datatype = DT_FLOAT;
        } else if (l instanceof Long || r instanceof Long) {
            result = l.longValue() / r.longValue();
            datatype = DT_LONG;
        } else if (l instanceof Integer || r instanceof Integer) {
            result = l.intValue() / r.intValue();
            datatype = DT_INT;
        } else if (l instanceof Short || r instanceof Short) {
            result = l.shortValue() / r.shortValue();
            datatype = DT_SHORT;
        } else if (l instanceof Byte || r instanceof Byte) {
            result = l.byteValue() / r.byteValue();
            datatype = DT_BYTE;
        } else {
            result = l.doubleValue() / r.doubleValue();
            datatype = DT_DOUBLE;
        }
        if (result.equals(l)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return typed(u8, 0, u8.length, datatype);
    }

    public Term abs() {
        requireNumeric("abs");
        return local[1] == '-' ? negate() : this;
    }

    public Term ceil() {
        Number n = requireNumeric("ceil");
        Number result = (switch (n) {
            case BigDecimal b -> b.setScale(0, RoundingMode.UP);
            case Double d     -> Math.ceil(d);
            case Float d      -> Math.ceil(d);
            default           -> n;
        });
        if (result.equals(n)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return new Term(flaggedDictId, u8);
    }

    public Term floor() {
        Number n = requireNumeric("floor");
        Number result = (switch (n) {
            case BigDecimal b -> b.setScale(0, RoundingMode.DOWN);
            case Double d     -> Math.floor(d);
            case Float d      -> Math.floor(d);
            default           -> n;
        });
        if (result.equals(n)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return new Term(flaggedDictId, u8);
    }

    public Term round() {
        Number n = requireNumeric("floor");
        Number result = (switch (n) {
            case BigDecimal b -> b.setScale(0, RoundingMode.HALF_DOWN);
            case Double d     -> Math.floor(d);
            case Float d      -> Math.floor(d);
            default           -> n;
        });
        if (result.equals(n)) return this;
        byte[] u8 = ("\"" + result).getBytes(UTF_8);
        return new Term(flaggedDictId, u8);
    }

    /** Evaluate this term as a Boolean, per the SPARQL boolean value rules. */
    public boolean asBool() {
        return switch (type()) {
            case LIT -> {
                int id = flaggedDictId & 0x7fffffff;
                if (id == DT_BOOLEAN) {
                    yield local[1] == 't';
                } else if (id == DT_string) {
                    yield local.length > 1;
                } else if (id == 0) {
                    yield reverseSkip(0, local.length, UNTIL_DQ) > 1;
                } else {
                    Number n = asNumber();
                    yield switch (n) {
                        case null -> throw new InvalidExprTypeException("No boolean value for "+this);
                        case BigInteger i -> !i.equals(BigInteger.ZERO);
                        case BigDecimal i -> !i.equals(BigDecimal.ZERO);
                        case Double d     -> !d.isNaN() && d != 0;
                        case Float d      -> !d.isNaN() && d != 0;
                        default           -> n.longValue() != 0;
                    };
                }
            }
            case IRI, BLANK -> throw new InvalidExprTypeException(this+" has no boolean value");
            case VAR -> throw new UnboundVarException(this);
        };
    }


    @Override public boolean equals(Object o) {
        if (o == this) return true;
        if (o instanceof Term t) {
            Number n = asNumber(), tn = t.asNumber();
            if      ( n != null) return tn != null && compareTo(t) == 0;
            else if (tn != null) return false;
            return t.flaggedDictId == flaggedDictId && RopeSupport.arraysEqual(local, t.local);
        }
        return super.equals(o);
    }

    @Override public int hashCode() {
        if (hash == 0)  {
            Number n = asNumber();
            if (n == null)
                hash = 31*flaggedDictId + RopeSupport.hash(local, 0, local.length);
            else
                hash = n.hashCode();
        }
        return hash;
    }
}
