package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.Rope.ALPHANUMERIC;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract sealed class ExprParser extends AbstractOwned<ExprParser> {
    public static final int BYTES = 16 + 8*4 + TermParser.BYTES;
    private SegmentRope in;
    private int consumedPos, pos, len;
    public final TermParser termParser;
    private @Nullable Symbol symbol;
    private @Nullable Expr term;
    private @Nullable SparqlParser spParser;

    public static Orphan<ExprParser> create() { return new Concrete(); }

    private ExprParser() { termParser = TermParser.create().takeOwnership(this).eager(); }

    public static final class Concrete extends ExprParser implements Orphan<ExprParser> {
        @Override public ExprParser takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable ExprParser recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        termParser.recycle(this);
        spParser = Owned.recycle(spParser, this);
        return null;
    }

    /**
     * Parse a {@link Term} starting at index 0 of {@code in}.
     *
     * <p>This method is memoized for non-{@link Term} expressions. The cache compares
     * {@link String} objects by identity, not equality.</p>
     *
     * @throws InvalidTermException if there is a syntax error.
     * @return A non-null {@link Term} parsed from the SPARQL string
     */
    public Expr parse(SegmentRope in) {
        requireAlive();
        if (in == null || in.len() == 0)
            throw new InvalidExprException("null and \"\" are not valid expressions");
        this.len = (this.in = in).len();
        this.pos = 0;
        this.symbol = null;
        this.term = null;
        read();
        Expr expr = pExpression();
        if (symbol != Symbol.EOF)
            throw new InvalidExprException(in, pos, "Expected EOF, found"+(symbol == null ? term : symbol));
        return expr;
    }

    /** Sets input String to be used in subsequent {@link ExprParser#parse(int)} calls. */
    public void input(SegmentRope in) {
        requireAlive();
        this.len = (this.in = in).len();
    }

    /** Parse an expression that starts at char {@code pos} of the previously set
     *  {@link ExprParser#input(SegmentRope)}. */
    public Expr parse(int pos) {
        requireAlive();
        assert in != null;
        this.consumedPos = this.pos = pos;
        this.symbol = null;
        this.term = null;
        read();
        return pExpression();
    }

    /**
     * Index of the first char not included in the {@link Expr} returned by the last
     * {@code parse} call.
     *
     * @return a non-negative integer.
     */
    public int consumedPos() { return consumedPos; }


    private enum Symbol {
        EOF,
        // symbol operators
        OR,
        NEG,
        NEQ,
        AND,
        LPAR,
        RPAR,
        MUL,
        PLUS,
        COMMA,
        MINUS,
        DIV,
        LT,
        LTE,
        EQ,
        GT,
        GTE,

        // named binary operators
        IN,
        NOT_IN,

        ABS,
        BNODE,
        BOUND,
        CEIL,
        COALESCE,
        CONCAT,
        CONTAINS,
        DATATYPE,
        DAY,
        ENCODE_FOR_URI,
        EXISTS,
        FLOOR,
        HOURS,
        IF,
        IRI,
        ISBLANK,
        ISIRI,
        ISLITERAL,
        ISNUMERIC,
        ISURI,
        LANG,
        LANGMATCHES,
        LCASE,
        MD5,
        MINUTES,
        MONTH,
        NOT_EXISTS,
        NOW,
        RAND,
        REGEX,
        REPLACE,
        ROUND,
        SAMETERM,
        SECONDS,
        SHA1,
        SHA256,
        SHA384,
        SHA512,
        STR,
        STRAFTER,
        STRBEFORE,
        STRDT,
        STRENDS,
        STRLANG,
        STRLEN,
        STRSTARTS,
        STRUUID,
        SUBSTR,
        TIMEZONE,
        TZ,
        UCASE,
        URI,
        UUID,
        YEAR;

        private static long UNARY0, UNARY1;
        private static long BINARY0;

        static {
            Symbol[] unary = {ABS, BOUND, CEIL, DATATYPE, DAY, ENCODE_FOR_URI, EXISTS,
                    FLOOR, HOURS, IRI, ISBLANK, ISIRI, ISLITERAL, ISNUMERIC, ISURI, LANG,
                    LCASE, MD5, MINUTES, MONTH, NOT_EXISTS, NOT_IN, ROUND, SECONDS, SHA1,
                    SHA256, SHA384, SHA512, STR, STRLEN, TIMEZONE, TZ, UCASE, URI, YEAR};
            Symbol[] binary = {CONTAINS, LANGMATCHES, SAMETERM, STRAFTER, STRBEFORE,
                               STRDT, STRENDS, STRLANG, STRSTARTS};
            for (Symbol s : unary) {
                int ordinal = s.ordinal();
                if (ordinal < 64)
                    UNARY0 |= (1L << ordinal);
                else
                    UNARY1 |= (1L << (ordinal-63));
            }
            for (Symbol s : binary) {
                BINARY0 |= (1L << s.ordinal());
            }
        }

        public boolean isUnaryFunction() {
            int o = ordinal();
            return o < 64 ? (UNARY0 & (1L << o)) != 0 : (UNARY1 & (1L << (o-63))) != 0;
        }
        public boolean isBinaryFunction() {
            int o = ordinal();
            return o < 64 && (BINARY0 & (1L << o)) != 0;
        }

        public FinalSegmentRope makeInput() {
            return FinalSegmentRope.asFinal(switch (this) {
                case NEG   -> "!";
                case NEQ   -> "!=";
                case AND   -> "&&";
                case LPAR  -> "(";
                case RPAR  -> ")";
                case MUL   -> "*";
                case PLUS  -> "+";
                case COMMA -> ",";
                case MINUS -> "-";
                case DIV   -> "/";
                case LT    -> "<";
                case LTE   -> "<=";
                case EQ    -> "=";
                case GT    -> ">";
                case GTE   -> ">=";
                case OR    -> "||";
                case EOF    -> "~EOF";
                default -> name().replace('_', ' ').toUpperCase();
            });
        }
    }

    /** The Symbol corresponding to {@code SYMBOL_INPUTS[i]} */
    private static final Symbol[] SYMBOL_INPUTS_VALUES;
    /** Sorted list of all {@link Symbol#makeInput()} values */
    private static final byte[][] SYMBOL_INPUTS;
    /** Given the first uppercase char {@code f} of a function name such name (in uppercase)
     *  should be between {@code FUNCTION_INPUTS_RANGE[2*(f-'A')+0]} and
     *  {@code FUNCTION_INPUTS_RANGE[2*(f-'A')+1]} (exclusive) in {@code SYMBOL_INPUTS}. */
    private static final byte[] SYMBOL_INPUTS_RANGE = new byte[2*('Z'-'A'+1)];
    /** Chars that by themselves constitute an operator */
    private static final long IS_SINGLE_CHAR_OP = (1L << '(') | (1L << ')') | (1L << ',')
                                                | (1L << '=') | (1L << '+') | (1L << '-')
                                                | (1L << '*') | (1L << '/');
    /** First chars of two-char operators  */
    private static final long  IS_TWO_CHAR_OP = (1L << '<') | (1L << '>')
                                              | (1L << '!') | (1L << '&'); // '|' > 64

    /** All the char values that constitute {@code IS_SINGLE_CHAR_OP},
     *  ordered by usage frequency */
    private static final byte[] SINGLE_CHARS = new byte[] {
            '(', ')', ',', '=', '+', '-', '*', '/'
    };
    /** The {@link Symbol} corresponding to {@code SINGLE_CHARS[i]} */
    private static final Symbol[] SINGLE_CHARS_SYMBOLS = new Symbol[] {
            Symbol.LPAR, Symbol.RPAR,  Symbol.COMMA, Symbol.EQ,
            Symbol.PLUS, Symbol.MINUS, Symbol.MUL,   Symbol.DIV
    }; // same order as SINGLE_CHARS

    static {
        Symbol[] symbols = Symbol.values();
        SYMBOL_INPUTS_VALUES = new Symbol[symbols.length];

        var inputs = Arrays.stream(symbols).map(Symbol::makeInput).toList();
        var sortedInputs = inputs.stream().sorted().toList();
        SYMBOL_INPUTS = sortedInputs.stream().map(s -> s.toArray(0, s.len)).toArray(byte[][]::new);
        for (int i = 0; i < SYMBOL_INPUTS.length; i++)
            SYMBOL_INPUTS_VALUES[i] = symbols[inputs.indexOf(sortedInputs.get(i))];

        for (char c = 'A'; c <= 'Z'; ++c) {
            int rangeIdx = 2*(c-'A');
            int len = SYMBOL_INPUTS.length, begin = len, end = len;
            for (int j = 0; begin == len && j < len; j++) {
                if (SYMBOL_INPUTS[j][0] == c)
                    begin = j;
            }
            for (int j = begin; end == len && j < len; j++) {
                if (SYMBOL_INPUTS[j][0] > c)
                    end = j;
            }
            SYMBOL_INPUTS_RANGE[rangeIdx] = (byte) begin;
            SYMBOL_INPUTS_RANGE[rangeIdx+1] = (byte) end;
        }
    }

    private static long symbolSet(boolean strict, Symbol... symbols) {
        long set = (strict ? 1L : 0L) << 63;
        for (Symbol s : symbols) {
            assert s.ordinal() < 63;
            set |= 1L << s.ordinal();
        }
        return set;
    }

    private Symbol takeSymbol(long set) {
        long bit = symbol == null ? 0 : 1L << symbol.ordinal();
        if ((set & bit) == 0) {
            if ((set & 0x8000000000000000L) == 0)
                return null; // set is nto "strict"
            throw new InvalidExprException(in, pos, "Expected a symbol, found"+term);
        } else {
            Symbol seen = symbol;
            symbol = null;
            read();
            return seen;
        }
    }

    void read() {
        if (symbol != null || term != null) return;
        consumedPos = pos;
        for (pos = in.skipWS(pos, len); pos != len && in.get(pos) == '#';)
            pos = in.skipWS(in.skipUntil(pos, len, '\n'), len);
        if (pos == len) {
            symbol = Symbol.EOF;
            return;
        }
        byte c = in.get(pos);
        if (c < 64) { // handle all operators except ||
            long bit = 1L << c;
            if ((IS_SINGLE_CHAR_OP & bit) != 0) {
                ++pos;
                int i = 0;
                while (i < SINGLE_CHARS.length && SINGLE_CHARS[i] != c) ++i;
                symbol = SINGLE_CHARS_SYMBOLS[i];
            } else if ((IS_TWO_CHAR_OP & bit) != 0) {
                ++pos; // consume c
                if (pos >= len)
                    throw new InvalidExprException(in, pos, "Premature EOF");
                byte next = in.get(pos);
                switch (c) {
                    case '<' -> { // could also be an <IRI>, try that first
                        if (termParser.parse(in, pos, len) == TermParser.Result.NT) {
                            pos = termParser.termEnd();
                            term = termParser.asTerm();
                            return;
                        }
                        if (next == '=') { ++pos; symbol = Symbol.LTE; }
                        else             { symbol = Symbol.LT; }
                    }
                    case '>' -> {
                        if (next == '=') { ++pos; symbol = Symbol.GTE; }
                        else             { symbol = Symbol.GT; }
                    }
                    case '!' -> {
                        if (next == '=') { ++pos; symbol = Symbol.NEQ; }
                        else             { symbol = Symbol.NEG; }
                    }
                    case '&' -> {
                        if (next == '&') { ++pos; symbol = Symbol.AND; }
                        else             { throw new InvalidExprException(in, pos-1, "Expected &&"); }
                    }
                    default -> throw new InvalidExprException(in, pos - 1, "Expected operator");
                }
            }
        } else if (c == '|') { // '|' > 64, so it couldn't be in the previous branch
            ++pos; // consume first |
            if (in.get(pos++) == '|') symbol = Symbol.OR;
            else                      throw new InvalidExprException(in, pos-1, "Expected ||");
        } else if ((c >= 'A' && c<='Z') || (c>='a' && c<='z')) {
            if (c >= 'a') c -= 'a'-'A';
            readNamedSymbol(c); // do not inline: colder than termParser.parse() below
        }
        if (symbol == null && termParser.parse(in, pos, len).isValid()) {
            term = termParser.asTerm();
            pos = termParser.termEnd();
        }
    }

    private static final byte[] NOT = "NOT".getBytes(UTF_8);
    private void readNamedSymbol(byte uppercaseFirst) {
        int rIdx = 2 * (uppercaseFirst - 'A');
        byte begin = SYMBOL_INPUTS_RANGE[rIdx], end = SYMBOL_INPUTS_RANGE[rIdx+1];
        if (end > begin) {
            // find next ( or {, then reverse any space before that
            int tokenLen = in.skip(pos, len, ALPHANUMERIC)-pos;
            if (tokenLen == 3 && in.hasAnyCase(pos, NOT)) {
                readNotNamedSymbol();
            } else if (tokenLen > 0) {
                int inputIdx = begin;
                while (inputIdx != end && !in.hasAnyCase(pos, SYMBOL_INPUTS[inputIdx]))
                    ++inputIdx;
                if (inputIdx != end) {
                    symbol = SYMBOL_INPUTS_VALUES[inputIdx];
                    pos += tokenLen;
                }
            }
        }
    }

    private static final byte[] IN = "IN".getBytes(UTF_8);
    private static final byte[] EXISTS = "EXISTS".getBytes(UTF_8);
    private void readNotNamedSymbol() {
        int wBegin = in.skipWS(pos+3, len);
        if (wBegin >= len)
            return;
        int wEnd = in.skip(wBegin, len, ALPHANUMERIC);
        if (in.hasAnyCase(wBegin, IN)) {
            symbol = Symbol.NOT_IN;
            pos = wEnd;
        } else if (in.hasAnyCase(wBegin, EXISTS)) {
            symbol = Symbol.NOT_EXISTS;
            pos = wEnd;
        }
    }

    private static final long OR_SET = symbolSet(false, Symbol.OR);
    private Expr pExpression() {
        Expr e = pAnd();
        while (takeSymbol(OR_SET) == Symbol.OR)
            e = new Expr.Or(e, pAnd());
        return e;
    }

    private static final long AND_SET = symbolSet(false, Symbol.AND);
    private Expr pAnd() {
        Expr e = pRelational();
        while (takeSymbol(AND_SET) == Symbol.AND)
            e = new Expr.And(e, pRelational());
        return e;
    }

    private static final long REL_SET = symbolSet(false, Symbol.EQ, Symbol.NEQ,
            Symbol.LT, Symbol.GT, Symbol.LTE, Symbol.GTE, Symbol.IN, Symbol.NOT_IN);

    private Expr pRelational() {
        Expr e = pAdditive();
        return switch (takeSymbol(REL_SET)) {
            case EQ -> new Expr.Eq(e, pAdditive());
            case NEQ -> new Expr.Neq(e, pAdditive());
            case LT -> new Expr.Lt(e, pAdditive());
            case GT -> new Expr.Gt(e, pAdditive());
            case LTE -> new Expr.Lte(e, pAdditive());
            case GTE -> new Expr.Gte(e, pAdditive());
            case IN -> new Expr.In(pExprList(e));
            case NOT_IN -> new Expr.NotIn(pExprList(e));
            case null, default -> e;
        };
    }

    private static final long ADDITIVE_SET = symbolSet(false, Symbol.PLUS, Symbol.MINUS);
    private Expr pAdditive() {
        Expr e = pMultiplicative();
        while (true) {
            switch (takeSymbol(ADDITIVE_SET)) {
                case PLUS -> e = new Expr.Add(e, pMultiplicative());
                case MINUS -> e = new Expr.Subtract(e, pMultiplicative());
                case null, default -> { return e; }
            }
        }
    }

    private static final long MULTIPLICATIVE_SET = symbolSet(false, Symbol.MUL, Symbol.DIV);
    private Expr pMultiplicative() {
        Expr e = pUnary();
        while (true) {
            switch (takeSymbol(MULTIPLICATIVE_SET)) {
                case MUL -> e = new Expr.Multiply(e, pUnary());
                case DIV -> e = new Expr.Divide(e, pUnary());
                case null, default -> { return e; }
            }
        }
    }

    private static final long LPAR_SET  = symbolSet(false, Symbol.LPAR);
    private static final long COMMA_SET = symbolSet(false, Symbol.COMMA);
    private static final long REQ_LPAR  = symbolSet(true, Symbol.LPAR);
    private static final long REQ_RPAR  = symbolSet(true, Symbol.RPAR);
    private static final long REQ_COMMA = symbolSet(true, Symbol.COMMA);
    private static final long UNARY_SET = symbolSet(false, Symbol.NEG, Symbol.PLUS, Symbol.MINUS);
    private Expr pUnary() {
        Symbol unary = takeSymbol(UNARY_SET);
        Expr primary;
        if (takeSymbol(LPAR_SET) == Symbol.LPAR) {
            primary = pExpression();
            takeSymbol(REQ_RPAR);
        } else if (term != null) {
            primary = term;
            term = null;
            read();
        } else {
            if (symbol == null)
                throw new InvalidExprException(in, pos, "Expected a symbol, found null");
            if (symbol == Symbol.EXISTS || symbol == Symbol.NOT_EXISTS)
                return pExists(); //special because arg is SPARQL, not Expr
            Symbol s = symbol;
            symbol = null;
            read();
            Expr l = null, r = null;
            Expr[] list = null;
            if (s.isUnaryFunction()) {
                takeSymbol(REQ_LPAR);
                l = pExpression();
                takeSymbol(REQ_RPAR);
            } else if (s.isBinaryFunction()) {
                takeSymbol(REQ_LPAR);
                l = pExpression();
                takeSymbol(REQ_COMMA);
                r = pExpression();
                takeSymbol(REQ_RPAR);
            } else {
                list = pExprList(null);
            }
            primary = switch (s) {
                case ABS -> new Expr.Abs(l);
                case BNODE -> new Expr.MakeBNode(list);
                case BOUND -> new Expr.Bound(l);
                case CEIL -> new Expr.Ceil(l);
                case COALESCE -> new Expr.Coalesce(list);
                case CONCAT -> new Expr.Concat(list);
                case CONTAINS -> new Expr.Contains(l, r);
                case DATATYPE -> new Expr.Datatype(l);
                case ENCODE_FOR_URI -> new Expr.Encode_for_uri(l);
                case FLOOR -> new Expr.Floor(l);
                case IF -> new Expr.If(list);
                case IRI, URI -> new Expr.Iri(l);
                case ISBLANK -> new Expr.IsBlank(l);
                case ISIRI, ISURI -> new Expr.IsIRI(l);
                case ISLITERAL -> new Expr.IsLit(l);
                case ISNUMERIC -> new Expr.IsNumeric(l);
                case LANG -> new Expr.Lang(l);
                case LCASE -> new Expr.LCase(l);
                case RAND -> new Expr.Rand();
                case REGEX -> new Expr.Regex(list);
                case REPLACE -> new Expr.Replace(list);
                case ROUND -> new Expr.Round(l);
                case SAMETERM -> new Expr.SameTerm(l, r);
                case STR -> new Expr.Str(l);
                case STRAFTER -> new Expr.Strafter(l, r);
                case STRBEFORE -> new Expr.Strbefore(l, r);
                case STRDT -> new Expr.Strdt(l, r);
                case STRENDS -> new Expr.Strends(l, r);
                case STRLANG -> new Expr.Strlang(l, r);
                case STRLEN -> new Expr.Strlen(l);
                case STRSTARTS -> new Expr.Strstarts(l, r);
                case STRUUID -> new Expr.Struuid();
                case SUBSTR -> new Expr.Substr(list);
                case UCASE -> new Expr.UCase(l);
                case UUID -> new Expr.Uuid();
                case null ->
                        throw new InvalidExprException(in, pos, "Expected a symbol, found " + term);
                default ->
                        throw new InvalidExprException(in, pos, s + " not supported (yet)");
            };
        }
        return switch (unary) {
            case NEG   -> new Expr.Neg(primary);
            case MINUS -> new Expr.Minus(primary);
            case null, default -> primary;
        };
    }

    private Expr.Exists pExists() {
        boolean negate = symbol == Symbol.NOT_EXISTS;
        SparqlParser spParser = this.spParser;
        if (spParser == null)
            this.spParser = spParser = SparqlParser.create().takeOwnership(this);
        Plan filter = spParser.parseGroup(in, pos, termParser.prefixMap());
        pos = spParser.pos();
        symbol = null;
        read();
        return new Expr.Exists(filter, negate);
    }

    private Expr[] pExprList(@Nullable Expr first) {
        Expr[] a = new Expr[3];
        takeSymbol(REQ_LPAR);
        if (symbol == Symbol.RPAR)
            return first == null ? new Expr[0] : new Expr[] {first};
        int size = 0;
        if (first != null)
            a[size++] = first;
        a[size++] = pExpression();
        while (takeSymbol(COMMA_SET) == Symbol.COMMA) {
            if (size > a.length)
                a = Arrays.copyOf(a, Math.max(10, a.length + (a.length >> 1)));
            a[size++] = pExpression();
        }
        takeSymbol(REQ_RPAR);
        return size == a.length ? a : Arrays.copyOf(a, size);
    }
}
