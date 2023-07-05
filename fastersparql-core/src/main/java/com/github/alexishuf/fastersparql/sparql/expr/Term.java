package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityShallowPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.RopeWrapper.*;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compare2_2;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.PN_LOCAL_LAST;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.HAS_UNSAFE;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("SpellCheckingInspection")
public final class Term extends Rope implements Expr, ExprEvaluator {
    private static final int POOL_COL = AffinityShallowPool.reserveColumn();
    private static final byte IS_READONLY = 0x0000010;
    private static final byte   IS_SUFFIX = 0x0000001;
    private static final byte   TYPE_MASK = 0x0000006;
    private static final int     TYPE_BIT = numberOfTrailingZeros(TYPE_MASK);

    public static final Term FALSE = new Term(DT_BOOLEAN, "\"false", true);
    public static final Term TRUE = new Term(DT_BOOLEAN, "\"true", true);
    public static final Term EMPTY_STRING = new Term(EMPTY, "\"\"", true);

    public static final SegmentRope CLOSE_IRI = new SegmentRope(">".getBytes(UTF_8), 0, 1);
    public static final Term EMPTY_IRI = new Term(new SegmentRope(MemorySegment.ofArray("<".getBytes(UTF_8)), 0, 1), CLOSE_IRI, false);
    public static final Term XSD = new Term(P_XSD, CLOSE_IRI, false);
    public static final Term RDF = new Term(P_RDF, CLOSE_IRI, false);
    static {
        XSD.flags |= IS_READONLY;
        RDF.flags |= IS_READONLY;
    }

    public static final Term XSD_DURATION = new Term(P_XSD, "duration>", false);
    public static final Term XSD_DATETIME = new Term(P_XSD, "dateTime>", false);
    public static final Term XSD_TIME = new Term(P_XSD, "time>", false);
    public static final Term XSD_DATE = new Term(P_XSD, "date>", false);
    public static final Term XSD_GYEARMONTH = new Term(P_XSD, "gYearMonth>", false);
    public static final Term XSD_GYEAR = new Term(P_XSD, "gYear>", false);
    public static final Term XSD_GMONTHDAY = new Term(P_XSD, "gMonthDay>", false);
    public static final Term XSD_GDAY = new Term(P_XSD, "gDay>", false);
    public static final Term XSD_GMONTH = new Term(P_XSD, "gMonth>", false);
    public static final Term XSD_BOOLEAN = new Term(P_XSD, "boolean>", false);
    public static final Term XSD_BASE64BINARY = new Term(P_XSD, "base64Binary>", false);
    public static final Term XSD_HEXBINARY = new Term(P_XSD, "hexBinary>", false);
    public static final Term XSD_FLOAT = new Term(P_XSD, "float>", false);
    public static final Term XSD_DECIMAL = new Term(P_XSD, "decimal>", false);
    public static final Term XSD_DOUBLE = new Term(P_XSD, "double>", false);
    public static final Term XSD_ANYURI = new Term(P_XSD, "anyURI>", false);
    public static final Term XSD_STRING = new Term(P_XSD, "string>", false);
    public static final Term XSD_INTEGER = new Term(P_XSD, "integer>", false);
    public static final Term XSD_NONPOSITIVEINTEGER = new Term(P_XSD, "nonPositiveInteger>", false);
    public static final Term XSD_LONG = new Term(P_XSD, "long>", false);
    public static final Term XSD_NONNEGATIVEINTEGER = new Term(P_XSD, "nonNegativeInteger>", false);
    public static final Term XSD_NEGATIVEINTEGER = new Term(P_XSD, "negativeInteger>", false);
    public static final Term XSD_INT = new Term(P_XSD, "int>", false);
    public static final Term XSD_UNSIGNEDLONG = new Term(P_XSD, "unsignedLong>", false);
    public static final Term XSD_POSITIVEINTEGER = new Term(P_XSD, "positiveInteger>", false);
    public static final Term XSD_SHORT = new Term(P_XSD, "short>", false);
    public static final Term XSD_UNSIGNEDINT = new Term(P_XSD, "unsignedInt>", false);
    public static final Term XSD_BYTE = new Term(P_XSD, "byte>", false);
    public static final Term XSD_UNSIGNEDSHORT = new Term(P_XSD, "unsignedShort>", false);
    public static final Term XSD_UNSIGNEDBYTE = new Term(P_XSD, "unsignedByte>", false);
    public static final Term XSD_NORMALIZEDSTRING = new Term(P_XSD, "normalizedString>", false);
    public static final Term XSD_TOKEN = new Term(P_XSD, "token>", false);
    public static final Term XSD_LANGUAGE = new Term(P_XSD, "language>", false);

    public static final Term RDF_LANGSTRING = new Term(P_RDF, "langString>", false);
    public static final Term RDF_HTML = new Term(P_RDF, "HTML>", false);
    public static final Term RDF_XMLLITERAL = new Term(P_RDF, "XMLLiteral>", false);
    public static final Term RDF_JSON = new Term(P_RDF, "JSON>", false);

    public static final Term RDF_TYPE = new Term(P_RDF, "type>", false);
    public static final Term RDF_FIRST = new Term(P_RDF, "first>", false);
    public static final Term RDF_REST = new Term(P_RDF, "rest>", false);
    public static final Term RDF_NIL = new Term(P_RDF, "nil>", false);
    public static final Term RDF_VALUE = new Term(P_RDF, "value>", false);
    public static final Term RDF_PROPERTY = new Term(P_RDF, "Property>", false);
    public static final Term RDF_LIST = new Term(P_RDF, "List>", false);
    public static final Term RDF_BAG = new Term(P_RDF, "Bag>", false);
    public static final Term RDF_SEQ = new Term(P_RDF, "Seq>", false);
    public static final Term RDF_ALT = new Term(P_RDF, "Alt>", false);
    public static final Term RDF_STATEMENT = new Term(P_RDF, "Statement>", false);
    public static final Term RDF_SUBJECT = new Term(P_RDF, "subject>", false);
    public static final Term RDF_PREDICATE = new Term(P_RDF, "predicate>", false);
    public static final Term RDF_OBJECT = new Term(P_RDF, "object>", false);
    public static final Term RDF_DIRECTION = new Term(P_RDF, "direction>", false);

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

    public static final SegmentRope[] FREQ_XSD_DT_SUFF = {
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

    private static final byte[] INTERN_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes(UTF_8);
    private static final int INTERN_W = 62;
    private static final Term[][] PLAIN = {
            new Term[INTERN_W],
            new Term[INTERN_W*INTERN_W],
    };
    private static final SegmentRope[][] IRI_LOCALS = {
            new SegmentRope[INTERN_W],
            new SegmentRope[INTERN_W*INTERN_W],
     };

    static {
        assert INTERN_W == INTERN_ALPHABET.length;
        for (int i0 = 0; i0 < INTERN_W; i0++) {
            char c0 = (char) INTERN_ALPHABET[i0];
            Term t = new Term(EMPTY, Rope.of('"', c0, '"'), true);
            SegmentRope l = Rope.of(c0, '>');
            PLAIN[0][internIdx(t.local(), 1, 1)] = t;
            IRI_LOCALS[0][internIdx(l, 0, 1)] = l;
            for (int i1 = 0; i1 < INTERN_W; i1++) {
                char c1 = (char) INTERN_ALPHABET[i1];
                t = new Term(EMPTY, Rope.of('"',c0,c1,'"'), true);
                l = Rope.of(c0, c1, '>');
                PLAIN[1][internIdx(t.local(), 1, 2)] = t;
                IRI_LOCALS[1][internIdx(l, 0, 2)] = l;
            }
        }
    }

    private static int internCharIdx(SegmentRope src, int i) {
        byte c = get(src, i);
        if      (c >= 'a') return c > 'z' ? -1 : 36-'a'+c;
        else if (c >= 'A') return c > 'Z' ? -1 : 10-'A'+c;
        else if (c >= '0') return c > '9' ? -1 : c-'0';
        return -1;
    }

    private static int internIdx(SegmentRope src, int begin, int n) {
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

    private static SegmentRope internIriLocal(SegmentRope src, int begin, int len, boolean copy) {
        if (len <= 3) {
            if (get(src, begin+len-1) == '>') --len;
            int i = internIdx(src, begin, len);
            if      (i == -2) return CLOSE_IRI;
            else if (i >=  0) return IRI_LOCALS[len-1][i];
        }
        if (copy)
            return new ByteRope(src.toArray(begin, begin+len));
        return src;
    }

    private static Term internPlain(SegmentRope src, int begin, int len, boolean copy) {
        int n = len-2;
        if (n <= 2) {
            int i = internIdx(src, begin+1, n);
            if      (i == -2) return EMPTY_STRING;
            else if (i >=  0) return PLAIN[n-1][i];
        }
        var w = forLit(src, begin, begin+len);
        if (!copy && (w != NONE || begin != 0 || len != src.len)) copy = true;
        var local = copy ? new ByteRope(w.toArray(src, begin, begin + len)) : src;
        return new Term(EMPTY, local, true);
    }

    private static Term internRdf(SegmentRope local, int begin, int end) {
        Term candidate = switch (local.get(begin)) {
            case '>' -> RDF;
            case 'A' -> RDF_ALT;
            case 'B' -> RDF_BAG;
            case 'H' -> RDF_HTML;
            case 'J' -> RDF_JSON;
            case 'L' -> RDF_LIST;
            case 'P' -> RDF_PROPERTY;
            case 'S' -> switch (begin+1 < end ? local.get(begin+1) : 0) {
                case 'e' -> RDF_SEQ;
                case 't' -> RDF_STATEMENT;
                default -> null;
            };
            case 'X' -> RDF_XMLLITERAL;
            case 'd' -> RDF_DIRECTION;
            case 'f' -> RDF_FIRST;
            case 'l' -> RDF_LANGSTRING;
            case 'n' -> RDF_NIL;
            case 'o' -> RDF_OBJECT;
            case 'p' -> RDF_PREDICATE;
            case 'r' -> RDF_REST;
            case 's' -> RDF_SUBJECT;
            case 't' -> RDF_TYPE;
            case 'v' -> RDF_VALUE;
            default -> null;
        };
        if (candidate != null && candidate.second.compareTo(local, begin, end) == 0)
            return candidate;
        return new Term(P_RDF, new ByteRope(local.toArray(begin, end)), false);
    }

    private static Term internXsd(SegmentRope local, int begin, int end) {
        byte c1 = begin+1 < end ? local.get(begin+1) : (byte)'\0';
        Term candidate = switch (local.get(begin)) {
            case '>' -> XSD;
            case 'a' -> XSD_ANYURI;
            case 'b' -> switch (c1) {
                case 'a' -> XSD_BASE64BINARY;
                case 'o' -> XSD_BOOLEAN;
                case 'y' -> XSD_BYTE;
                default -> null;
            };
            case 'd' -> switch (c1) {
                case 'a' -> local.len > 5 /*date>*/ ? XSD_DATETIME : XSD_DATE;
                case 'e' -> XSD_DECIMAL;
                case 'o' -> XSD_DOUBLE;
                case 'u' -> XSD_DURATION;
                default -> null;
            };
            case 'f' -> XSD_FLOAT;
            case 'g' -> switch (c1) {
                case 'd' -> XSD_GDAY;
                case 'm' -> local.len > 7 /*gMonth>*/ ? XSD_GMONTHDAY : XSD_GMONTH;
                case 'y' -> local.len > 6 /*gYear>*/ ? XSD_GYEARMONTH : XSD_GYEAR;
                default -> null;
            };
            case 'h' -> XSD_HEXBINARY;
            case 'i' -> local.len > 4 /*int>*/ ? XSD_INTEGER : XSD_INT;
            case 'l' -> switch (c1) {
                case 'a' -> XSD_LANGUAGE;
                case 'o' -> XSD_LONG;
                default -> null;
            };
            case 'n' -> switch (c1) {
                case 'e' -> XSD_NEGATIVEINTEGER;
                case 'o' -> switch (local.get(3)) {
                    case 'N' -> XSD_NONNEGATIVEINTEGER;
                    case 'P' -> XSD_NONPOSITIVEINTEGER;
                    default -> null;
                };
                default -> null;
            };
            case 'p' -> XSD_POSITIVEINTEGER;
            case 's' -> switch (c1) {
                case 'h' -> XSD_SHORT;
                case 't' -> XSD_STRING;
                default -> null;
            };
            case 't' -> switch (c1) {
                case 'i' -> XSD_TIME;
                case 'o' -> XSD_TOKEN;
                default -> null;
            };
            case 'u' -> switch (local.len > 8 ? local.get(8) : 0) {
                case 'B' -> XSD_UNSIGNEDBYTE;
                case 'I' -> XSD_UNSIGNEDINT;
                case 'L' -> XSD_UNSIGNEDLONG;
                case 'S' -> XSD_UNSIGNEDSHORT;
                default -> null;
            };
            default -> null;
        };
        if (candidate != null && candidate.second.compareTo(local, begin, end) == 0)
            return candidate;
        return new Term(P_XSD, new ByteRope(local.sub(begin, end)), false);
    }


    private @NonNull SegmentRope first, second;
    private byte flags, cachedEndLex;
    private @Nullable Number number;
    private int hash;


    /** Construct an interned {@link Term} */
    private Term(@NonNull SegmentRope shared, @NonNull String local, boolean suffixShared) {
        this(shared, new ByteRope(local), suffixShared);
    }
    /**
     * Build a term for {@code shared+local} or {@code local+shared},
     * if {@code suffixShared == true}.
     *
     * <p> Both {@link SegmentRope}s are held by reference, thus changes to them will be
     * reflected on the {@link Term}.</p>
     *
     * <p>The new {@link Term} instance will be read-only, disallowing
     * {@link #set(SegmentRope, SegmentRope, boolean)} calls. To create a mutable {@link Term},
     * see {@link Term#Term()}.</p>
     *
     * @param shared a shared segment, ideally this should  be interned and shared among multiple
     *               Term instances. This may be null
     * @param local a segment that comes after or before (if {@code suffixShared}) {@code shared}
     *              This must not be {@code null} nor empty.
     * @param suffixShared whether {@code shared} is a suffix to {@code local}
     */
    public Term(@Nullable SegmentRope shared, @NonNull SegmentRope local, boolean suffixShared) {
        super((shared == null ? 0 : shared.len) + local.len);
        if (shared == null) shared = EMPTY;
        if (suffixShared) {
            first  = local;
            second = shared;
            flags  = IS_READONLY|IS_SUFFIX;
        } else {
            first  = shared;
            second = local;
            flags  = IS_READONLY;
        }
        assert validate();
    }

    /**
     * Create a term to be later {@link #set(SegmentRope, SegmentRope, boolean)}.
     *
     * <p>Since it is impossible to construct an invalid {@link Term}, This will produce a
     * {@link Term} instance equals to {@link #EMPTY_STRING} (but with {@code set()} enabled).</p>
     */
    private Term() {
        super(2);
        SegmentRope f = EMPTY_STRING.first;
        first  = SegmentRope.pooledWrap(f.segment, f.utf8, f.offset, f.len);
        second = EMPTY;
        flags  = IS_SUFFIX;
    }

    public static Term mutable() { return new Term(); }

    public static Term pooledMutable() {
        Term t = AffinityShallowPool.get(POOL_COL);
        return t == null ? new Term() : t;
    }

    public void recycle() {
        if ((flags & IS_READONLY) != 0)
            throw new UnsupportedOperationException("This term instance is read-only.");
        var myLocal = local();
        myLocal.wrap(EMPTY_STRING.first);
        set0(EMPTY, myLocal, true);
        AffinityShallowPool.offer(POOL_COL, this);
    }

    public void set(@NonNull SegmentRope shared, @NonNull MemorySegment localSeg,
                    byte @Nullable[] localU8, long localOff, int localLen, boolean suffixShared) {
        if ((flags & IS_READONLY) != 0)
            throw new UnsupportedOperationException("This term instance is read-only.");
        var myLocal = local();
        myLocal.wrapSegment(localSeg, localU8, localOff, localLen);
        set0(shared, myLocal, suffixShared);
    }

    private void set0(SegmentRope shared, SegmentRope myLocal, boolean suffixShared) {
        this.len = shared.len + myLocal.len;
        if (suffixShared) {
            this.first = myLocal;
            this.second = shared;
        } else {
            this.first = shared;
            this.second = myLocal;
        }
        this.hash = 0;
        this.number = null;
        this.flags = suffixShared ? IS_SUFFIX : 0;
        this.cachedEndLex = 0;
        assert validate();
    }

    public void set(@NonNull SegmentRope shared, @NonNull SegmentRope local, boolean suffixShared) {
        if ((flags & IS_READONLY) != 0)
            throw new UnsupportedOperationException("This term instance is read-only.");
        this.len = shared.len + local.len;
        var myLocal = local();
        myLocal.wrap(local);
        set0(shared, myLocal, suffixShared);
    }

    @SuppressWarnings("ConstantValue") private boolean validate() {
        if (first == null) throw new AssertionError("first is null");
        if (second == null) throw new AssertionError("second is null");
        if (first.len + second.len < 2)
            throw new AssertionError("term len < 2");
        if ((flags & IS_SUFFIX) != 0) {
            if (second() != shared() || second != shared())
                throw new AssertionError("sharedSuffixed, but second != shared");
            if (first.len == 0)
                throw new AssertionError("first/local() is empty");
            if (second.len > 0) {
                if (second.get(0) != '"')
                    throw new AssertionError("suffixed shared does not start with \"");
                if (second.len > 1) {
                    if (second.get(1) == '^') {
                        if (second.len < 5)
                            throw new AssertionError("shared suffix has ^ but is too short");
                        if (second.get(2) != '^' || second.get(3) != '<' || second.get(second.len-1) != '>')
                            throw new AssertionError("shared suffix is not a valid datatype");
                    } else if (second.get(1) == '@') {
                        if (second.len < 3)
                            throw new AssertionError("empty lang tag");
                    } else {
                        throw new AssertionError("shared suffix is not closing quote followed by lang trag or datatype");
                    }
                }
            }
        } else {
            if (first() != shared() || first != shared())
                throw new AssertionError("first != shared in prefixed term");
            if (second.len == 0)
                throw new AssertionError("Empty local segment");
            if (first.len > 0 && first.get(0) != '<')
                throw new AssertionError("shared prefix does not start with <");
        }
        if (local().len == 0)
            throw new AssertionError("empty local segment");
        if (shared() == DT_langString)
            throw new AssertionError("explicit rdf:langString datatype");
        if (shared() == DT_string)
            throw new AssertionError("explicit xsd:string datatype");
        if ((shared() == P_XSD || shared() == P_RDF)
                && second.skip(0, second.len, ALPHANUMERIC) != second.len-1)
            throw new AssertionError("Unexpected chars in local segment of rdf:/xsd:Term");
        if ("<\"?$_".indexOf((char) get(0)) < 0)
            throw new AssertionError("Unexpected start char ");

        if (get(0) == '<') { // IRI
            if (get(len()-1) != '>')
                throw new AssertionError("no closing '>'");
            if (len() > 2 && get(len()-2) == '>')
                throw new AssertionError("IRI ending in >>");
            if (len() > 2 && get(1) == '<')
                throw new AssertionError("IRI starting with <<");
            if (skipUntil(1, len()-1, '<', '>') != len()-1)
                throw new AssertionError("<> within IRI");
            if (skip(0, len(), UNTIL_SPECIAL_WS) != len())
                throw new AssertionError("suspicious control char in term");
        } else if (get(0) == '"') { // literal
            int endLex = reverseSkipUntil(0, len(), '"');
            if (endLex == 0)
                throw new AssertionError("no closign \"");
            validateEscapes(endLex);
        } else if (get(0) == '?' || get(0) == '?') {
            if (skip(1, len(), SparqlSkip.VARNAME) < len())
                throw new AssertionError("bad char in var name");
        } else if (get(0) == '_') {
            if (get(1) != ':')
                throw new AssertionError("expected :");
            if (skip(2, len(), SparqlSkip.BN_LABEL) < len())
                throw new AssertionError("invalid character in bnode label");
        }
        return true;
    }

    private void validateEscapes(int lexEnd) {
        for (int i = 1; i < lexEnd; i = skip(i+1, lexEnd, UNTIL_DQ_OR_BACKSLASH)) {
            byte c = get(i);
            if      (c ==  '"') throw new AssertionError("unescaped \"");
            else if (c == '\\') ++i;
        }
    }

    /**
     * Wraps the given prefix and suffix into a Term. Both may be changed by this call and may be
     * kept by reference in the resulting {@link Term}.
     *
     * @param prefix prefix of the N-Triples RDF term or SPARQL variable or null. If {@code suffix}
     *               is non-null and the RDF term is an IRI, this will be kept as the
     *               {@link #shared} segment.
     * @param suffix suffix of the N-Triples RDF term or SPARQL variable or null.
     *               If the RDF term is a literal and prefix was also given, this will be kept as
     *               the {@link #shared} segment.
     * @return A possibly interned {@link Term} instance representing the N-Triples term or
     *         SPARQL var denoted by {@code prefix+suffix}.
     */
    public static Term wrap(@Nullable SegmentRope prefix, @Nullable SegmentRope suffix) {
        if (prefix == null) prefix = EMPTY;
        if (suffix == null) suffix = EMPTY;
        return switch (prefix.len > 0 ? prefix.get(0) : suffix.len > 0 ? suffix.get(0) : 0) {
            case 0 -> throw new InvalidTermException(ByteRope.EMPTY, 0, "empty input");
            case '"' -> {
                if (prefix.len == 0) {
                    prefix = suffix;
                    suffix = EMPTY;
                }
                if (suffix.len > 0) {
                    if (suffix.get(0) != '"')
                        throw new IllegalArgumentException("suffix must start with \"");
                    // ensure suffix is interned as some methods rely on reference equality.
                    if (suffix.len >= MIN_INTERNED_LEN)
                        suffix = SHARED_ROPES.internDatatype(suffix, 0, suffix.len);
                } else if (prefix.len <= 4) {
                    yield internPlain(prefix, 0, prefix.len, false);
                } else if (prefix.reverseSkipUntil(0, prefix.len, '"') == 0) {
                    throw new InvalidTermException(prefix, prefix.len, "No closing \"");
                }
                if (suffix == DT_string) {
                    suffix = null;
                    prefix = new ByteRope(prefix.len+1).append(prefix).append('"');
                } else if (suffix == DT_langString) {
                    throw new IllegalArgumentException("got ^^rdf:langString suffix instead of lang tag");
                } else if (suffix == DT_BOOLEAN) {
                    yield switch (prefix.get(1)) {
                        case 't' -> TRUE;
                        case 'f' -> FALSE;
                        default -> throw new InvalidTermException(prefix, 1, "boolean must be true or false");
                    };
                }
                yield new Term(suffix, prefix, true);
            }
            case '<' -> {
                if (suffix.len == 0) {
                    suffix = prefix;
                    prefix = EMPTY;
                } else if (prefix == P_RDF) {
                    yield internRdf(suffix, 0, suffix.len);
                } else if (prefix == P_XSD) {
                    yield internXsd(suffix, 0, suffix.len);
                }
                if (suffix.len <= 3) {
                    suffix = internIriLocal(suffix, 0, suffix.len, false);
                } else if (suffix.get(suffix.len-1) != '>') {
                    throw new InvalidTermException(suffix, suffix.len - 1, "No closing >");
                }
                yield new Term(prefix, suffix, false);
            }
            case '?', '$', '_' -> {
                if (suffix.len == 0)
                    suffix = prefix;
                else if (prefix.len > 0)
                    suffix = new ByteRope(prefix.len + suffix.len).append(prefix).append(suffix);
                yield new Term(EMPTY, suffix, false);
            }
            default -> throw new InvalidTermException(Rope.of(prefix, suffix), 0, "Not a NT start");
        };
    }

    /**
     * Creates a {@link Term} that refers to a copy of the bytes in {@code r.sub(begin, end)},
     * which must contain a valid N-Triples RDF term or a SPARQL variable.
     *
     * @param r The rope containing an RDF term or variable.
     * @param begin the index where the RDF term or var starts in {@code r}
     * @param end {@code r.len} or the index of the first byte in {@code r} after the term or var.
     * @return a possibly interned Term instance.
     * @throws InvalidTermException if there is a syntax error in the N-Triples term or SPARQL var.
     *                              only cheap checks are executed, since this method is not a
     *                              parser and should be called with already valid data.
     */
    public static Term valueOf(SegmentRope r, int begin, int end) {
        if (r == null || end <= begin)  return null;
        int len = end - begin;
        if (len < 2) throw new InvalidTermException(r, 1, "input is too short");
        return switch (r.get(begin)) {
            case '"' -> {
                SegmentRope suffix = SHARED_ROPES.internDatatypeOf(r, begin, end);
                if (suffix == DT_BOOLEAN) {
                    yield switch (r.get(begin+1)) {
                        case 't' -> TRUE;
                        case 'f' -> FALSE;
                        default -> throw new InvalidTermException(r.sub(begin, end), 1, "boolean must be true or false");
                    };
                } else if (suffix.len == 0 && end-begin <=4) {
                    yield internPlain(r, begin, end - suffix.len - begin, true);
                }
                yield wrap(new ByteRope(r.toArray(begin, end-suffix.len)), suffix);
            }
            case '<' -> {
                SegmentRope prefix = SHARED_ROPES.internPrefixOf(r, begin, end);
                if (prefix == P_RDF)
                    yield internRdf(r, begin+P_RDF.len, end);
                if (prefix == P_XSD)
                    yield internXsd(r, begin+P_XSD.len, end);
                int suffixLen = end-begin-prefix.len;
                SegmentRope suffix;
                if (prefix.len > 0 && suffixLen <=3)
                    suffix = internIriLocal(r, begin+prefix.len, suffixLen, true);
                else
                    suffix = new ByteRope(r.toArray(begin+prefix.len, end));
                yield new Term(prefix, suffix, false);
            }
            case '?', '$', '_' -> wrap(null, new ByteRope(r.toArray(begin, end)));
            default -> throw new InvalidTermException(r.sub(begin, end).toString(), 0,
                                                      "Does not start with <, \", ?, $ or _");
        };
    }

    /**
     * Similar to {@link #valueOf(SegmentRope, int, int)}, but will take ownership of
     * {@code rope}, possibly mutating it and keeping a reference to it in the built {@link Term}.
     *
     * @param rope a valid N-Triples RDF term or SPARQL var
     * @return A {@link Term} for the given RDF term or SPARQL var
     * @throws InvalidTermException if there is a syntax error in the N-Triples term or SPARQL var.
     *                              only cheap checks are executed, since this method is not a
     *                              parser and should be called with already valid data.
     */
    public static Term splitAndWrap(SegmentRope rope) {
        if (rope.len == 0) return null;
        else if (rope.len < 2) throw new InvalidTermException(rope, 0, "input too short");
        return switch (rope.get(0)) {
            case '"' -> {
                SegmentRope suffix = SHARED_ROPES.internDatatypeOf(rope, 0, rope.len);
                if (suffix != null) rope.len -= suffix.len;
                yield wrap(rope, suffix);
            }
            case '<' -> {
                SegmentRope prefix = SHARED_ROPES.internPrefixOf(rope, 0, rope.len);
                if (prefix != null) {
                    rope.offset += prefix.len;
                    rope.len -= prefix.len;
                }
                yield wrap(prefix, rope);
            }
            default -> wrap(null, rope);
        };
    }

    /** Equivalent to {@link #valueOf(SegmentRope, int, int)} from {@code 0} to {@code r.len()}. */
    public static Term valueOf(CharSequence cs) {
        if (cs == null) return null;
        if (cs instanceof SegmentRope s) return valueOf(s, 0, s.len);
        else return splitAndWrap(new ByteRope(cs));
    }


    public static Term prefixed(SegmentRope prefix, String local) {
        return Term.wrap(prefix, new ByteRope(local));
    }
    public static Term typed(Object lex, SegmentRope datatype) {
        if (lex instanceof byte[] b)
            lex = new String(b, UTF_8);
        String lexS = lex.toString();
        return Term.wrap(RopeWrapper.forOpenLit(lexS).toRope(lexS), datatype);
    }
    public static Term iri(Object iri) {
        return Term.splitAndWrap(RopeWrapper.forIri(iri).toRope(iri));
    }
    public static Term plainString(String lex) {
        return Term.wrap(RopeWrapper.forLit(lex).toRope(lex), null);
    }
    public static Term lang(String lex, String lang) {
        ByteRope tmp = new ByteRope();
        RopeWrapper.forLit(lex).append(tmp, lex).append('@').append(lang);
        return Term.wrap(tmp, null);
    }

    /** Get an array of terms where the i-th element is the result of {@code valueOf(terms[i])} */
    public static @Nullable Term[] array(Object... terms) {
        if (terms.length == 1 && terms[0] instanceof Collection<?> coll)
            terms = coll.stream().map(i -> i == null ? null : i.toString()).toArray(String[]::new);
        if (terms.length == 1 && terms[0] instanceof Object[] arr)
            terms = arr;
        Term[] a = new Term[terms.length];
        try (TermParser termParser = new TermParser().eager()) {
            termParser.prefixMap.add(Rope.of("owl"), Term.valueOf("<http://www.w3.org/2002/07/owl#>"));
            termParser.prefixMap.add(Rope.of("foaf"), Term.valueOf("<http://xmlns.com/foaf/0.1/>"));
            termParser.prefixMap.add(Rope.of(""), Term.valueOf("<http://example.org/>"));
            termParser.prefixMap.add(Rope.of("ex"), Term.valueOf("<http://example.org/>"));
            termParser.prefixMap.add(Rope.of("exns"), Term.valueOf("<http://www.example.org/ns#>"));
            for (int i = 0; i < terms.length; i++)
                a[i] = terms[i] == null ? null : termParser.parseTerm(SegmentRope.of(terms[i]));
        }
        return a;
    }

    /** Equivalent to {@link #array(Object...)} but yields a {@link List} instead of an array. */
    public static List<@Nullable Term> termList(Object... terms) { return Arrays.asList(array(terms)); }
    public static List<@Nullable Term> termList(CharSequence... terms) { return Arrays.asList(array((Object[]) terms)); }


    /* --- --- --- Rope implementation --- --- --- */

    private void checkRange(int begin, int end) {
        int len = this.len;
        String msg;
        if      (end   < begin) msg = "Range with end < begin";
        else if (begin <     0) msg = "Negative begin";
        else if (end   >   len) msg = "Range overflows Rope end";
        else return;
        throw new IndexOutOfBoundsException(msg);
    }

    @Override public byte get(int i) {
        if (i < 0 || i >= len) throw new IndexOutOfBoundsException();
        SegmentRope fst = this.first, snd = this.second;
        int fstLen = fst.len;
        return i < fstLen ? fst.get(i) : snd.get(i-fstLen);
    }

    @Override public byte[] copy(int begin, int end, byte[] dest, int offset) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;
        if (begin < fstLen) {
            int e = Math.min(end, fstLen);
            fst.copy(begin, e, dest, offset);
            offset += e-begin;
            begin = e;
        }
        if (end > fstLen)
            snd.copy(begin-fstLen, end-fstLen, dest, offset);
        return dest;
    }

    @Override public int write(OutputStream out) throws IOException {
        first.write(out);
        second.write(out);
        return len;
    }

    @Override public PlainRope sub(int begin, int end) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;
        if      (end   <= fstLen) return fst.sub(begin, end);
        else if (begin >= fstLen) return snd.sub(begin-fstLen, end-fstLen);
        var tsr = new TwoSegmentRope();
        tsr.wrapFirst(fst.segment, fst.utf8, fst.offset+begin, fstLen-begin);
        tsr.wrapSecond(snd.segment, snd.utf8, snd.offset+Math.max(0, begin-fstLen), end-fstLen);
        return tsr;
    }

    @Override public int skipUntil(int begin, int end, char c0) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;

        int e = Math.min(end, fstLen), i;
        if (begin < fstLen && (i = fst.skipUntil(begin, e, c0)) < e) return i;
        if ((e = end-fstLen) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + snd.skipUntil(i, e, c0);
        }
        return end;
    }

    @Override public int skipUntil(int begin, int end, char c0, char c1) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen && (i = fst.skipUntil(begin, e, c0, c1)) < e) return i;
        if ((e = end-fstLen) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + snd.skipUntil(i, e, c0, c1);
        }
        return end;
    }


    @Override public int skipUntilLast(int begin, int end, char c0) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;
        int e = end-fstLen, i = Math.max(0, begin-fstLen);
        if (e > 0 && (i = snd.skipUntilLast(i, e, c0)) < e) return fstLen+i;
        if (begin < fstLen) {
            e = Math.min(fstLen, end);
            if ((i = fst.skipUntilLast(begin, e, c0)) < e) return i;
        }
        return end;
    }

    @Override public int skipUntilLast(int begin, int end, char c0, char c1) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;
        int e = end-fstLen, i = Math.max(0, begin-fstLen);
        if (e > 0 && (i = snd.skipUntilLast(i, e, c0, c1)) < e) return fstLen+i;
        if (begin < fstLen) {
            e = Math.min(fstLen, end);
            if ((i = fst.skipUntilLast(begin, e, c0, c1)) < e) return i;
        }
        return end;
    }

    @Override public int skip(int begin, int end, int[] alphabet) {
        checkRange(begin, end);
        SegmentRope fst = first, snd = second;
        int fstLen = fst.len;
        int e = Math.min(end, fstLen), i;
        if (begin < fstLen && (i = fst.skip(begin, e, alphabet)) < e) return i;
        if ((e = Math.max(0, end-fstLen)) > 0) {
            i = Math.max(0, begin-fstLen);
            return fstLen + snd.skip(i, e, alphabet);
        }
        return end;
    }

    @Override public boolean has(int pos, Rope rope, int begin, int end) {
        if (pos < 0 || begin < 0 || pos > len || end > rope.len)
            throw new IndexOutOfBoundsException();
        int rLen = end-begin;
        if (pos+rLen > len) return false;
        return cmp(pos, pos+rLen, rope, begin, end) == 0;
    }

    @Override public int compareTo(@NonNull Rope o) {
        return cmp(0, len, o, 0, o.len);
    }

    public int compareTo(MemorySegment segment, long off, int len) {
        // collect segments and ranges for this
        SegmentRope fst = first, snd = second;
        return -SegmentRope.compare1_2(segment, off, len,
                                       fst.segment, fst.offset, fst.len,
                                       snd.segment, snd.offset, snd.len);
    }

    @Override public int compareTo(Rope o, int begin, int end) {
        return cmp(0, len, o, begin, end);
    }

    public int cmp(int begin, int end, Rope rope, int rBegin, int rEnd) {
        if (!HAS_UNSAFE)
            return cmpNoUnsafe(begin, end, rope, rBegin, rEnd);
        if (begin < 0 || end > len || rBegin < 0 || rEnd > rope.len)
            throw new IndexOutOfBoundsException();

        // collect segments and ranges for this
        SegmentRope fst = first, snd = second;
        int      fstLen = Math.min(fst.len, end)-begin;

        // collect segments and ranges for rope
        byte[] ofst, osnd;
        long ofstOff, osndOff;
        int ofstLen, osndLen;
        if (rope instanceof Term t) {
            SegmentRope f = t.first, s = t.second;
            ofst = f.utf8; ofstOff = f.segment.address()+f.offset; ofstLen = f.len;
            osnd = s.utf8; osndOff = s.segment.address()+s.offset; osndLen = s.len;
        } else if (rope instanceof SegmentRope s) {
            ofst = s.utf8; ofstOff = s.segment.address()+s.offset; ofstLen = s.len;
            osnd = null;      osndOff = 0;        osndLen = 0;
        } else {
            TwoSegmentRope t = (TwoSegmentRope) rope;
            ofst = t.fstU8; ofstOff = t.fst.address()+t.fstOff; ofstLen = t.fstLen;
            osnd = t.sndU8; osndOff = t.snd.address()+t.sndOff; osndLen = t.sndLen;
        }

        // crop segments to [rBegin, rEnd)
        if (rBegin < ofstLen)  ofstOff += rBegin;
        if (rEnd > ofstLen)  { osndOff += Math.max(0, rBegin-ofstLen); osndLen = rEnd - ofstLen; }
        else                   ofstLen = rEnd;

        return compare2_2(fst.utf8, fst.segment.address()+fst.offset+begin, fstLen,
                          snd.utf8,
                          snd.segment.address()+snd.offset+Math.max(0, begin-fst.len),
                          len-fstLen,
                          ofst, ofstOff, ofstLen, osnd, osndOff, osndLen);
    }

    private int cmpNoUnsafe(int begin, int end, Rope rope, int rBegin, int rEnd) {
        if (begin < 0 || end > len || rBegin < 0 || rEnd > rope.len)
            throw new IndexOutOfBoundsException();

        // collect segments and ranges for this
        SegmentRope fst = first, snd = second;
        int      fstLen = Math.min(fst.len, end)-begin;

        // collect segments and ranges for rope
        MemorySegment ofst, osnd;
        long ofstOff, osndOff;
        int ofstLen, osndLen;
        if (rope instanceof Term t) {
            SegmentRope f = t.first, s = t.second;
            ofst = f.segment; ofstOff = f.offset; ofstLen = f.len;
            osnd = s.segment; osndOff = s.offset; osndLen = s.len;
        } else if (rope instanceof SegmentRope s) {
            ofst = s.segment; ofstOff = s.offset; ofstLen = s.len;
            osnd = null;      osndOff = 0;        osndLen = 0;
        } else {
            TwoSegmentRope t = (TwoSegmentRope) rope;
            ofst = t.fst; ofstOff = t.fstOff; ofstLen = t.fstLen;
            osnd = t.snd; osndOff = t.sndOff; osndLen = t.sndLen;
        }

        // crop segments to [rBegin, rEnd)
        if (rBegin < ofstLen)  ofstOff += rBegin;
        if (rEnd > ofstLen)  { osndOff += Math.max(0, rBegin-ofstLen); osndLen = rEnd - ofstLen; }
        else                   ofstLen = rEnd;

        return compare2_2(fst.segment, fst.offset+begin, fstLen,
                snd.segment, snd.offset+Math.max(0, begin-fst.len), len-fstLen,
                ofst, ofstOff, ofstLen, osnd, osndOff, osndLen);
    }

    /* --- --- --- ExprEvaluator implementation --- --- --- */

    @Override public Term evaluate(Batch<?> batch, int row) {
        return this;
    }

    private static final class UnboundEvalautor implements ExprEvaluator {
        private static final UnboundEvalautor INSTANCE = new UnboundEvalautor();

        @Override public @Nullable Term evaluate(Batch<?> batch, int row) {
            return null;
        }
    }
    private static final class VarEvalautor implements ExprEvaluator {
        private final Term tmp = Term.mutable();
        private final int col;

        private VarEvalautor(int col) { this.col = col; }

        @Override public @Nullable Term evaluate(Batch<?> batch, int row) {
            return batch.getView(row, col, tmp) ? tmp : null;
        }
    }

    /* --- --- --- Expr implementation --- --- --- */

    @Override public int argCount() { return 0; }
    @Override public Expr arg(int i) { throw new IndexOutOfBoundsException(i); }
    @Override public Term eval(Binding binding) { return binding.getIf(this); }
    @Override public Expr bound(Binding binding) { return binding.getIf(this); }

    @Override public ExprEvaluator evaluator(Vars vars) {
        if (type() == Type.VAR) {
            int col = vars.indexOf(this);
            return col < 0 ? UnboundEvalautor.INSTANCE : new VarEvalautor(col);
        }
        return this;
    }

    @Override public Vars vars() {
        var name = name();
        if (name == null) return Vars.EMPTY;
        var singleton = new Vars.Mutable(1);
        singleton.add(name);
        return singleton;
    }

    /**
     * Get the SPARQL preferred representation of this {@link Term}. {@link #RDF_TYPE}
     * becomes "a" and literals typed as XSD integer, decimal double and boolean are replaced by
     * their lexical forms (without quotes and datatype suffix).
     */
    @Override public int toSparql(ByteSink<?, ?> dest, PrefixAssigner assigner) {
        SegmentRope local = local();
        return toSparql(dest, assigner, shared(),
                 local.segment, local.utf8, local.offset, local.len, (flags & IS_SUFFIX) != 0);
    }

    public static int toSparql(ByteSink<?, ?> dest, PrefixAssigner assigner, SegmentRope shared,
                               MemorySegment local, byte @Nullable [] localU8,
                               long localOff, int localLen, boolean isLit) {
        if (shared == null || shared.len == 0) {
            dest.append(local, localU8, localOff, localLen);
            return localLen;
        }
        int oldLen = dest.len();
        if (isLit) {
            if (shared == DT_DOUBLE) {
                boolean exp = false;
                for (long i = localOff+1; i < localLen; i++) {
                    byte c = local.get(JAVA_BYTE, i);
                    if (c == 'e' || c == 'E') { exp = true; break; }
                }
                if (exp) {
                    dest.append(local, localU8, localOff+1, localLen-1);
                    return localLen-1;
                }
            } else if (shared == DT_integer || shared == DT_decimal || shared == DT_BOOLEAN) {
                dest.append(local, localU8, localOff + 1, localLen - 1);
                return localLen-1;
            }
            dest.append(local, localU8, localOff, localLen); // write "\"LEXICAL_FORM"
            if (shared.get(1) == '^') {
                SegmentRope prefix = SHARED_ROPES.internPrefixOf(shared, 3/*"^^<*/, shared.len);
                Rope name = prefix == null ? null : assigner.nameFor(prefix);
                if (name == null) {
                    dest.append(shared);
                } else {
                    dest.append(shared, 0, 3).append(name).append(':')
                        .append(shared, 3+prefix.len, shared.len-1);
                }
            } else {
                dest.append(shared);
            }
        } else {
            if (shared == P_RDF && localLen == 5 /*type>*/
                    && SegmentRope.has(local, localOff, RDF_TYPE.local().segment, 0, 5)) {
                dest.append('a');
            } else {
                Rope name = assigner.nameFor(shared);
                if (name == null) {
                    dest.append(shared);
                } else {
                    dest.append(name).append(':');
                    --localLen; // do not write trailing >
                    byte last = localLen == 0 ? (byte)'a'
                                  : local.get(JAVA_BYTE, localOff+localLen-1);
                    if (!Rope.contains(PN_LOCAL_LAST, last)) {
                        if (!new SegmentRope(local, localOff, localLen).isEscaped(localLen-1)) {
                            dest.append(local, localU8, localOff, localLen - 1)
                                    .append('\\').append(last);
                            localLen = 0;
                        }
                    }
                }
                dest.append(local, localU8, localOff, localLen);
            }
        }
        return dest.len()-oldLen;
    }

    /* --- --- --- term methods --- --- --- */

    public enum Type {
        BLANK,
        VAR,
        LIT,
        IRI
    }
    private static final Type[] TYPES  = Type.values();

    public boolean isIri() { return type() == Type.IRI; }
    public boolean isVar() { return type() == Type.VAR; }

    public Type type() {
        int ordinal = (flags & TYPE_MASK) >>> TYPE_BIT;
        if (ordinal == 0) {
            ordinal = (flags & IS_SUFFIX) != 0 ? Type.LIT.ordinal() : switch (get(0)) {
                case '"'      -> Type.LIT.ordinal();
                case '_'      -> Type.BLANK.ordinal();
                case '<'      -> Type.IRI.ordinal();
                case '?', '$' -> Type.VAR.ordinal();
                default       -> throw new InvalidTermException(toString(), 0, "bad start");
            };
            flags |= (ordinal << TYPE_BIT);
        }
        return TYPES[ordinal];
   }

    public SegmentRope          first() { return first; }
    public SegmentRope         second() { return second; }
    public SegmentRope         shared() { return (flags & IS_SUFFIX) != 0 ? second : first; }
    public SegmentRope          local() { return (flags & IS_SUFFIX) != 0 ? first  : second; }
    public boolean     sharedSuffixed()  { return (flags & IS_SUFFIX) != 0; }


    /** If this is a var, gets its name (without leading '?'/'$'). Else, return {@code null}. */
    public @Nullable SegmentRope name() {
        return type() == Type.VAR ? second.sub(1, len) : null;
    }

    /** {@code lang} if this is a literal tagged with {@code @lang}, else {@code null}. */
    public @Nullable Rope lang() {
        int i = endLex();
        if (i > 0 && i+1 < len && get(i+1) == '@') return sub(i+2, len);
        return null;
    }

    /** Index of closing {@code "} if this is a literal, else {@code -1} */
    public int endLex() {
        if (type() != Type.LIT) return -1;
        int endLex = 0xff&cachedEndLex;
        if (endLex == 0) {
            endLex = reverseSkipUntil(1, len, '"');
            cachedEndLex = endLex > 0xff ? 0 : (byte)endLex;
        }
        return endLex;
    }

    /**
     * If {@link #type()} is {@link Type#LIT}, get the explicit or implicit (i.e.,
     * {@code xsd:string} and {@code rdf:langString}) type suffix ({@code "^^<...>)}.
     */
    public @Nullable SegmentRope datatypeSuff() {
        int endLex = endLex();
        if (endLex < 0) return null;
        return switch (endLex+1 == len ? 0 : get(endLex+1)) {
            case 0   -> DT_string;
            case '@' -> SharedRopes.DT_langString;
            case '^' -> {
                if (second.len > 0) yield second;
                yield SHARED_ROPES.internDatatypeOf(first, endLex, first.len);
            }
            default -> throw new InvalidTermException(this, endLex, "garbage after lexical form");
        };
    }

    /**
     * If this is an IRI of an XML schema or RDF datatype, get a {@code DT_} suffix from
     * {@link SharedRopes} such that it equals {@code Rope.of("\"^^", this)}.
     */
    public @Nullable SegmentRope asKnownDatatypeSuff() {
        if (first == SharedRopes.P_XSD) {
            for (int i = 0; i < FREQ_XSD_DT.length; ++i) {
                if (FREQ_XSD_DT[i] == this) return FREQ_XSD_DT_SUFF[i];
            }
        } else if (first == SharedRopes.P_RDF) {
            if      (this == RDF_LANGSTRING) return DT_langString;
            else if (this == RDF_HTML)       return DT_HTML;
            else if (this == RDF_JSON)       return DT_JSON;
            else if (this == RDF_XMLLITERAL) return DT_XMLLiteral;
        }
        return null;
    }

    public @Nullable SegmentRope asDatatypeSuff() {
        var suff = asKnownDatatypeSuff();
        return suff == null && type() == Type.IRI ? asDatatypeSuffCold() : suff;
    }

    private @Nullable SegmentRope asDatatypeSuffCold() {
        var r = new ByteRope(3 + len()).append("\"^^").append(this);
        return SHARED_ROPES.internDatatype(r, 0, r.len);
    }

    /**
     * Get the (explicit or implicit) datatype IRI or {@code null} if this is not a literal.
     */
    public @Nullable Term datatypeTerm() {
        SegmentRope suff = datatypeSuff();
        return suff == null ? null : valueOf(suff, 3/*"^^*/, suff.len);
    }

    /**
     * Get the lexical form of  this literal with escapes as required by {@code "}-quoted
     * N-Triples literals but without the surrounding quotes.
     */
    public @Nullable SegmentRope escapedLexical() {
        int endLex = endLex();
        if (endLex < 0) return null;
        return first.sub(1, endLex);
    }

    /** The number of UTF-8 bytes that would be output by {@link #unescapedLexical(ByteRope)}. */
    public int unescapedLexicalSize() {
        int endLex = endLex(), required = 0;
        if (endLex == 0) return 0;
        for (int i = 1, j, n; i < endLex; i += n) {
            j = skipUntil(i, endLex, '\\');
            n = j-i;
            byte c = j == endLex ? 0 : first.get(j + 1);
            required += n + switch (c) {
                case 0 -> 0;
                case 'u', 'U' -> {
                    int codePoint = parseCodePoint(j);
                    i += c == 'u' ? 6 : 10;
                    if (codePoint < 0 || codePoint >= 0x110000)
                        yield Character.toString(codePoint).length();
                    if      (codePoint < 0x80   ) yield 1;
                    else if (codePoint < 0x800  ) yield 2;
                    else if (codePoint < 0x10000) yield 3;
                    else                          yield 4;
                }
                default -> { i += 2; yield 1; }
            };
        }
        return required;
    }

    /**
     * Write the lexical form (the literal value without surrounding quotes and without the
     * language tag or datatype.) of this term into {@code dest} replacing {@code \}-escape
     * sequences with the represented characters (e.g., {@code \n} becomes a line feed.
     *
     * @param dest where the unescaped lexical form shall be appended to
     * @return total number of UTF-8 bytes written.
     */
    public int unescapedLexical(ByteRope dest) {
        int endLex = endLex();
        int before = dest.len;
        SegmentRope local = first;
        for (int i = 1, j; i < endLex; i = j+2) {
            j = skipUntil(i, endLex, '\\');
            dest.append(local, i, j);
            if (j >= endLex) break;
            byte c = local.get(j+1);
            switch (c) {
                case 'n'           -> dest.append('\n');
                case 'r'           -> dest.append('\r');
                case 't'           -> dest.append('\t');
                case '\\','"','\'' -> dest.append(c);
                case 'u','U'       -> {
                    dest.appendCodePoint(parseCodePoint(j));
                    // this escape is not just 2 bytes: \\u0123 \\U01230123
                    //                                   ++++@@  ++++++++@@
                    j += c == 'U' ? 8 : 4; // @@'s incremented on the loop
                }
                default            -> dest.appendCodePoint('\\');
            }
        }
        return dest.len-before;
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
        SegmentRope nLocal;
        if (second.len == 0) {
            var w = forLit(lex);
            int tail = endLex()+1;
            if (tail == 0) {
                throw new InvalidTermException(this, 0, "not a literal");
            } else if (tail == len-1) {
                nLocal = w.toRope(lex);
            } else {
                var tmp = new ByteRope(w.extraBytes() + lex.len + (len - tail));
                nLocal = w.append(tmp, lex).append(first, tail, len);
            }
        } else if (type() == Type.LIT) {
            nLocal = forOpenLit(lex).toRope(lex);
        } else {
            throw new InvalidExprTypeException(this, this, "literal");
        }
        return new Term(second, nLocal, true);
    }

    public boolean isNumeric() {
        SegmentRope s = second;
        if (s.len == 0) return false;
        int typeOrdinal = (flags & TYPE_MASK) >>> TYPE_BIT;
        if (typeOrdinal > 0 && typeOrdinal != Type.LIT.ordinal() || s.get(0) != '"') return false;
        return s == DT_INT || s == DT_unsignedShort || s == DT_DOUBLE || s == DT_FLOAT ||
               s == DT_integer || s == DT_positiveInteger || s == DT_nonPositiveInteger ||
               s == DT_nonNegativeInteger || s == DT_unsignedLong || s == DT_decimal ||
               s == DT_LONG || s == DT_unsignedInt || s == DT_SHORT || s == DT_unsignedByte ||
               s == DT_BYTE;
    }

    public static boolean isNumericDatatype(SegmentRope suff) {
        return suff == DT_INT || suff == DT_unsignedShort || suff == DT_DOUBLE || suff == DT_FLOAT ||
               suff == DT_integer || suff == DT_positiveInteger || suff == DT_nonPositiveInteger ||
               suff == DT_nonNegativeInteger || suff == DT_unsignedLong || suff == DT_decimal ||
               suff == DT_LONG || suff == DT_unsignedInt || suff == DT_SHORT || suff == DT_unsignedByte ||
               suff == DT_BYTE;
    }


    /** Get the {@link Number} for this term, or {@code null} if it is not a number. */
    public Number asNumber() {
        if (number != null || second.len == 0 || !isNumeric())
            return number;
        String lexical = first.toString(1, first.len);
        try {
            if (second == DT_INT || second == DT_unsignedShort) {
                number = Integer.valueOf(lexical);
            } else if (second == DT_DOUBLE) {
                number = Double.valueOf(lexical);
            } else if (second == DT_FLOAT) {
                number = Float.valueOf(lexical);
            } else if (second == DT_integer || second == DT_positiveInteger || second ==DT_nonPositiveInteger || second == DT_unsignedLong) {
                number = new BigInteger(lexical);
            } else if (second == DT_decimal) {
                number = new BigDecimal(lexical);
            } else if (second == DT_LONG || second == DT_unsignedInt) {
                number = Long.valueOf(lexical);
            } else if (second == DT_SHORT || second == DT_unsignedByte) {
                number = Short.valueOf(lexical);
            } else if (second == DT_BYTE) {
                number = (byte)Integer.parseInt(lexical);
            }
        } catch (NumberFormatException e) {
            Rope dt = second.sub(4, second.len);
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
        if (rhs == null)
            throw new ExprEvalException("cannot compare with unbound");
        int diff = cmp(0, len, rhs, 0, rhs.len);
        if (diff != 0 && isNumeric() && rhs.isNumeric())
            return compareNumeric(rhs);
        return diff;
    }

    public int compareNumeric(Term rhs) {
        Number l = asNumber(), r = rhs.asNumber();
        if (l instanceof BigInteger || r instanceof BigInteger) {
            return asBigInteger(l).compareTo(asBigInteger(r));
        } else if (l instanceof BigDecimal || r instanceof  BigDecimal) {
            return asBigDecimal(l, r).compareTo(asBigDecimal(r, l));
        } else if (l instanceof Float || l instanceof Double
                || r instanceof Float || r instanceof Double) {
            return Double.compare(l.doubleValue(), r.doubleValue());
        } else {
            return Long.compare(l.longValue(), r.longValue());
        }
    }

    public Term add(Term rhs) {
        Number l = requireNumeric("add"), r = rhs.requireNumeric("add");
        Number result;
        SegmentRope suffix;
        if (l instanceof BigDecimal || r instanceof BigDecimal) {
            result = asBigDecimal(l, r).add(asBigDecimal(r, l));
            suffix = DT_decimal;
        } else if (l instanceof BigInteger || r instanceof BigInteger) {
            result = asBigInteger(l).add(asBigInteger(r));
            suffix = DT_integer;
        } else if (l instanceof Double || r instanceof Double) {
            result = l.doubleValue() + r.doubleValue();
            suffix = DT_DOUBLE;
        } else if (l instanceof Float || r instanceof Float) {
            result = l.floatValue() + r.floatValue();
            suffix = DT_FLOAT;
        } else if (l instanceof Long || r instanceof Long) {
            result = l.longValue() + r.longValue();
            suffix = DT_LONG;
        } else if (l instanceof Integer || r instanceof Integer) {
            result = l.intValue() + r.intValue();
            suffix = DT_INT;
        } else if (l instanceof Short || r instanceof Short) {
            result = l.shortValue() + r.shortValue();
            suffix = DT_SHORT;
        } else if (l instanceof Byte || r instanceof Byte) {
            result = l.byteValue() + r.byteValue();
            suffix = DT_BYTE;
        } else {
            result = l.doubleValue() + r.doubleValue();
            suffix = DT_DOUBLE;
        }
        if (result.equals(l)) return this;
        return new Term(suffix, new ByteRope().append('"').append(result), true);
    }

    public Term subtract(Term rhs) {
        Number l = requireNumeric("subtract"), r = rhs.requireNumeric("subtract");
        Number result;
        SegmentRope datatype;
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
        return new Term(datatype, new ByteRope().append('"').append(result), true);
    }

    public Term negate() {
        if (isNumeric()) {
            if (first.get(1) == '-')
                return new Term(second, first.sub(1, first.len-1), true);
            ByteRope tmp = new ByteRope(first.len + 1);
            byte[] u8 = tmp.u8();
            u8[0] = '"';
            u8[1] = '-';
            tmp.append(first, 1, first.len);
            return new Term(second, tmp, true);
        }
        return asBool() ? FALSE : TRUE;
    }

    public Term multiply(Term rhs) {
        Number l = requireNumeric("multiply"), r = rhs.requireNumeric("multiply");
        Number result;
        SegmentRope datatype;
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
        return new Term(datatype, new ByteRope().append('"').append(result), true);
    }

    public Term divide(Term rhs) {
        Number l = requireNumeric("divide"), r = rhs.requireNumeric("divide");
        Number result;
        SegmentRope datatype;
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
        return new Term(datatype, new ByteRope().append('"').append(result), true);
    }

    public Term abs() {
        requireNumeric("abs");
        return first.get(1) == '-' ? negate() : this;
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
        return new Term(second, new ByteRope().append('"').append(result), true);
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
        return new Term(second, new ByteRope().append('"').append(result), true);
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
        return new Term(second, new ByteRope().append('"').append(result), true);
    }

    /** Evaluate this term as a Boolean, per the SPARQL boolean value rules. */
    public boolean asBool() {
        return switch (type()) {
            case LIT -> {
                if (second == DT_BOOLEAN) {
                    yield this == TRUE;
                } else if (second.len == 0) {
                    yield first.get(1) != '"';
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
        if (!(o instanceof Rope rope)) return false;
        if (o == this || (len == rope.len && cmp(0, len, rope, 0, rope.len) == 0))
            return true;
        if (isNumeric() && o instanceof Term t && t.isNumeric())
            return compareNumeric(t) == 0;
        return false;
    }

    @Override public int fastHash(int begin, int end) {
        SegmentRope fst = first, snd = second;
        int h = FNV_BASIS, nFst = Math.min(4, end-begin), nSnd = Math.min(12, end-(begin+4));
        if (begin+nFst < fst.len) {
            h = SegmentRope.hashCode(FNV_BASIS, fst.segment, fst.offset+begin, nFst);
        } else {
            for (int i = 0; i < nFst; i++)
                h = FNV_PRIME * (h ^ (0xff&get(begin+i)));
        }
        begin = end-nSnd;
        if (begin > fst.len) {
            h = SegmentRope.hashCode(h, snd.segment, snd.offset+(begin-fst.len), nSnd);
        } else {
            for (int i = 0; i < nFst; i++)
                h = FNV_PRIME * (h ^ (0xff&get(begin+i)));
        }
        return h;
    }

    @Override public int hashCode() {
        int hash = this.hash;
        if (hash == 0)  {
            if (isNumeric()) {
                SegmentRope local = first;
                hash = FNV_BASIS;
                boolean beforeNumber = true;
                for (int i = 1, end = local.len; i < end; i++) {
                    int c = 0xff & local.get(i);
                    if (beforeNumber) {
                        if (c == '0' || c == '+') continue;
                        if (c != '-') beforeNumber = false;
                    }
                    if (c == '.' || c == 'e' || c == 'E') break;
                    hash = FNV_PRIME * (hash ^ c);
                }
            } else {
                SegmentRope fst = first, snd = second;
                hash = SegmentRope.hashCode(FNV_BASIS, fst.segment, fst.offset, fst.len);
                hash = SegmentRope.hashCode(hash, snd.segment, snd.offset, snd.len);
            }
            this.hash = hash;
        }
        return hash;
    }

    static {
        int maxType = TYPES.length - 1;
        if (((maxType << TYPE_BIT) & TYPE_MASK) != TYPE_MASK)
            throw new AssertionError("TYPE_MASK too narrow");
        //noinspection ConstantValue
        if ((TYPE_MASK & IS_SUFFIX) != 0)
            throw new AssertionError("masks overlap");

    }
}
