package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;

import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class JsonParser<B extends Batch<B>> extends ResultsParser<B> {
    private @Nullable MutableRope partial = null, allocPartial = null;
    private final ArrayDeque<JsonState> jsonStack = new ArrayDeque<>();
    private final ArrayDeque<SparqlState> sparqlStack = new ArrayDeque<>();
    private boolean hadSparqlProperties = false;
    private int column;
    private Vars vars;
    private final MutableRope value = new MutableRope(32), lang = new MutableRope(16);
    private final MutableRope dtSuffix = new MutableRope(64);
    private final SegmentRopeView varName = new SegmentRopeView();
    private Term. @Nullable Type type;

    public static final class JsonFactory implements Factory {
        @Override public SparqlResultFormat name() { return SparqlResultFormat.JSON; }
        @Override
        public <B extends Batch<B>> ResultsParser<B> create(CompletableBatchQueue<B> d) {
            return new JsonParser<>(d);
        }
    }

    public JsonParser(CompletableBatchQueue<B> destination) {
        super(destination);
        push(SparqlState.ROOT);
        this.vars = destination.vars();
    }

    /* --- --- --- implement/override ResultsParserBIt methods --- --- --- */

    @Override public SparqlResultFormat format() {return SparqlResultFormat.JSON;}

    @Override public void reset(CompletableBatchQueue<B> downstream) {
        super.reset(downstream);
        if (partial      != null)      partial.close();
        if (allocPartial != null) allocPartial.close();
        jsonStack  .clear();
        sparqlStack.clear();
        value      .close();
        lang       .close();
        dtSuffix   .close();
        hadSparqlProperties = false;
        column              = 0;
        type                = null;
        vars                = downstream.vars();
        push(SparqlState.ROOT);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (partial      != null) partial.close();
        if (allocPartial != null) allocPartial.close();
        value   .close();
        lang    .close();
        dtSuffix.close();
    }

    @Override protected void doFeedShared(SegmentRope rope) throws CancelledException, TerminatedException {
        if (partial != null) {
            rope = partial.append(rope);
            partial = null;
        }
        int begin = 0, end = rope.len;
        // the first begin < end may seem redundant but is necessary since maybe rope == partial
        // and maybe suspend() reduced partial.len making begin out of bounds.
        while (begin < end && (begin = rope.skipWS(begin, end)) < end)
            begin = jsonStack.getFirst().parse(this, rope, begin, end);
    }

    @Override protected @Nullable Throwable doFeedEnd() {
        if (!hadSparqlProperties)
            return new InvalidSparqlResultsException("No \"results\" object nor \"boolean\" value in JSON");
        return null;
    }

    /* --- --- --- constants --- --- --- */

    private static final byte[] CONSTS;
    private static final int L_BRACKET_O, L_BRACKET_LEN;
    private static final int L_BRACE_O, L_BRACE_LEN;
    private static final int NULL_O, NULL_LEN;
    private static final int TRUE_O, TRUE_LEN;
    private static final int FALSE_O, FALSE_LEN;
    private static final int IRI_O, IRI_LEN;
    private static final int URI_O, URI_LEN;
    private static final int LITERAL_O, LITERAL_LEN;
    private static final int LIT_O, LIT_LEN;
    private static final int TYPED_LITERAL_O, TYPED_LITERAL_LEN;
    private static final int BNODE_O, BNODE_LEN;
    private static final int BLANK_O, BLANK_LEN;
    private static final int P_HEAD_O, P_HEAD_LEN;
    private static final int P_RESULTS_O, P_RESULTS_LEN;
    private static final int P_BOOLEAN_O, P_BOOLEAN_LEN;
    private static final int P_VARS_O, P_VARS_LEN;
    private static final int P_BINDINGS_O, P_BINDINGS_LEN;
    private static final int VALUE_O, VALUE_LEN;
    private static final int TYPE_O, TYPE_LEN;
    private static final int DATATYPE_O, DATATYPE_LEN;
    private static final int XMLLANG_O, XMLLANG_LEN;
    static {
        StringBuilder sb = new StringBuilder();
        L_BRACKET_O  = sb.length(); L_BRACKET_LEN  = sb.append("["       ).length()-L_BRACKET_O;
        L_BRACE_O    = sb.length(); L_BRACE_LEN    = sb.append("{"       ).length()-L_BRACE_O;
        NULL_O       = sb.length(); NULL_LEN       = sb.append("NULL"    ).length()-NULL_O;
        TRUE_O       = sb.length(); TRUE_LEN       = sb.append("TRUE"    ).length()-TRUE_O;
        FALSE_O      = sb.length(); FALSE_LEN      = sb.append("FALSE"   ).length()-FALSE_O;
        IRI_O        = sb.length(); IRI_LEN        = sb.append("IRI"     ).length()-IRI_O;
        URI_O        = sb.length(); URI_LEN        = sb.append("URI"     ).length()-URI_O;
        LITERAL_O    = sb.length(); LITERAL_LEN    = sb.append("LITERAL" ).length()-LITERAL_O;
        LIT_O        = sb.length(); LIT_LEN        = sb.append("LIT"     ).length()-LIT_O;
        TYPED_LITERAL_O = sb.length(); TYPED_LITERAL_LEN = sb.append("TYPED-LITERAL").length()-TYPED_LITERAL_O;
        BNODE_O      = sb.length(); BNODE_LEN      = sb.append("BNODE"   ).length()-BNODE_O;
        BLANK_O      = sb.length(); BLANK_LEN      = sb.append("BLANK"   ).length()-BLANK_O;
        P_HEAD_O     = sb.length(); P_HEAD_LEN     = sb.append("HEAD"    ).length()-P_HEAD_O;
        P_RESULTS_O  = sb.length(); P_RESULTS_LEN  = sb.append("RESULTS" ).length()-P_RESULTS_O;
        P_BOOLEAN_O  = sb.length(); P_BOOLEAN_LEN  = sb.append("BOOLEAN" ).length()-P_BOOLEAN_O;
        P_VARS_O     = sb.length(); P_VARS_LEN     = sb.append("VARS"    ).length()-P_VARS_O;
        P_BINDINGS_O = sb.length(); P_BINDINGS_LEN = sb.append("BINDINGS").length()-P_BINDINGS_O;
        VALUE_O      = sb.length(); VALUE_LEN      = sb.append("VALUE"   ).length()-VALUE_O;
        TYPE_O       = sb.length(); TYPE_LEN       = sb.append("TYPE"    ).length()-TYPE_O;
        DATATYPE_O   = sb.length(); DATATYPE_LEN   = sb.append("DATATYPE").length()-DATATYPE_O;
        XMLLANG_O    = sb.length(); XMLLANG_LEN    = sb.append("XML:LANG").length()-XMLLANG_O;
        CONSTS = sb.toString().getBytes(UTF_8);
    }

    private static final FinalSegmentRope L_BRACKET = new FinalSegmentRope(CONSTS, L_BRACKET_O, L_BRACKET_LEN);
    private static final FinalSegmentRope L_BRACE   = new FinalSegmentRope(CONSTS, L_BRACE_O, L_BRACE_LEN);
    private static final FinalSegmentRope NULL      = new FinalSegmentRope(CONSTS, NULL_O, NULL_LEN);
    private static final FinalSegmentRope TRUE      = new FinalSegmentRope(CONSTS, TRUE_O, TRUE_LEN);
    private static final FinalSegmentRope FALSE     = new FinalSegmentRope(CONSTS, FALSE_O, FALSE_LEN);

    private static final int[] UNQUOTED_VALUE = Rope.invert(Rope.alphabet(",}]", Rope.Range.WS));


    /* --- --- --- exception builders --- --- --- */

    private static InvalidSparqlResultsException ex(SparqlState state, SegmentRope r, int b, int e) {
        e = r.skip(b = r.skipWS(b, e), e, SegmentRope.UNTIL_WS);
        var msg = String.format("JSON parser at state %s expected %s but got %s",
                state, state.expected(), b == e ? "End-Of-Input" : r.sub(b, e).toString());
        return new InvalidSparqlResultsException(msg);
    }

    private static InvalidSparqlResultsException ex(JsonState state, SegmentRope r, int b, int e) {
        e = r.skip(b = r.skipWS(b, e), e, SegmentRope.UNTIL_WS);
        var got = b == e ? "End-Of-Input" : r.sub(b, e).toString();
        var msg = String.format("Malformed JSON: expected %s but got %s",
                         state.name().toLowerCase(), got);
        return new InvalidSparqlResultsException(msg);
    }

    private static InvalidSparqlResultsException badProperty(SparqlState state, SegmentRope r, int b, int e) {
        var name = r.sub(b, e);
        var msg = String.format("Unexpected property %s at state %s, expected %s",
                         name, state, join("/", state.expectedPropertiesString()));
        return new InvalidSparqlResultsException(msg);
    }

    private InvalidSparqlResultsException noType() {
        var msg = String.format("No \"type\" property given for RDF value \"%s\" with " +
                         "datatype=%s and xml:lang=%s", value,
                         dtSuffix.isEmpty() ? "null" : dtSuffix.sub(3, dtSuffix.len-1),
                         lang.len > 0 ? lang : "null");
        return new InvalidSparqlResultsException(msg);
    }

    /* --- --- --- helpers --- --- --- */

    private void pop() {
        jsonStack.pop();
        sparqlStack.pop();
    }

    private void push(SparqlState state) {
        sparqlStack.push(state);
        jsonStack.push(JsonState.VALUE);
    }

    private int suspend(SegmentRope r, int b, int e) {
        if (b >= e) return e;
        partial = allocPartial == null ? allocPartial=new MutableRope(32+(e-b)) : allocPartial;
        if (r == partial)
            partial.erase(e, partial.len).erase(0, b);
        else
            partial.clear().append(r, b, e);
        return e;
    }


    /* --- --- --- SPARQL-level parsing --- --- --- */

    private enum SparqlState {
        ROOT,
        IGNORE,
        HEAD,
        VARS,
        BOOLEAN,
        RESULTS,
        BINDINGS,
        BINDING_ROW,
        BINDING_VALUE,
        BINDING_VALUE_TYPE,
        BINDING_VALUE_DATATYPE,
        BINDING_VALUE_VALUE,
        BINDING_VALUE_LANG;

        public String expected() {
            return switch (this) {
                case ROOT -> "SPARQL results object ({\"head\": {\"vars\": [...]}, \"results\" : {\"bindings\": [...]}})";
                case IGNORE -> "any JSON value";
                case HEAD -> "{\"vars\": [...]} object";
                case VARS -> "JSON array";
                case BOOLEAN -> "true/false";
                case RESULTS -> "{\"bindings\": [...]} object";
                case BINDINGS -> "JSON array of objects whose properties are var names";
                case BINDING_ROW -> "JSON object whose properties are var names";
                case BINDING_VALUE -> "JSON object with value/type/datatype/xml:lang properties";
                case BINDING_VALUE_TYPE -> "\"uri\", \"bnode\" or \"literal\"";
                case BINDING_VALUE_DATATYPE -> "datatype IRI as a JSON string";
                case BINDING_VALUE_VALUE -> "JSON string with lexical form, bnode label or iri";
                case BINDING_VALUE_LANG -> "JSON string with language tag";
            };
        }

        public SparqlState forProperty(JsonParser<?> p, SegmentRope r, int b, int e) {
            int l = e - b;
             SparqlState next = switch (this) {
                case ROOT -> {
                    SparqlState s;
                    if      (l==4 && r.hasAnyCase(b, CONSTS, P_HEAD_O, P_HEAD_LEN))
                        s = HEAD;
                    else if (l==7 && r.hasAnyCase(b, CONSTS, P_RESULTS_O, P_RESULTS_LEN))
                        s = RESULTS;
                    else if (l==7 && r.hasAnyCase(b, CONSTS, P_BOOLEAN_O, P_BOOLEAN_LEN))
                        s = BOOLEAN;
                    else
                        s = IGNORE;
                    if (s != IGNORE)
                        p.hadSparqlProperties = true;
                    yield s;
                }
                case IGNORE -> IGNORE;
                case HEAD    -> l==4 && r.hasAnyCase(b, CONSTS, P_VARS_O, P_VARS_LEN)
                              ? VARS     : IGNORE;
                case RESULTS -> l==8 && r.hasAnyCase(b, CONSTS, P_BINDINGS_O, P_BINDINGS_LEN)
                              ? BINDINGS : IGNORE;
                case BINDING_ROW -> {
                    p.dtSuffix.clear();
                    p.type = null;
                    p.value.clear();
                    p.lang.clear();
                    p.varName.wrap(r.segment, r.utf8, r.offset+b, e-b);
                    yield (p.column = p.vars.indexOf(p.varName)) >= 0 ? BINDING_VALUE : IGNORE;
                }
                case BINDING_VALUE -> {
                    if (l==5 && r.hasAnyCase(b, CONSTS, VALUE_O, VALUE_LEN))
                        yield BINDING_VALUE_VALUE;
                    if (l==4 && r.hasAnyCase(b, CONSTS, TYPE_O, TYPE_LEN))
                        yield BINDING_VALUE_TYPE;
                    if (l==8 && r.hasAnyCase(b, CONSTS, DATATYPE_O, DATATYPE_LEN))
                        yield BINDING_VALUE_DATATYPE;
                    if (l==8 && r.hasAnyCase(b, CONSTS, XMLLANG_O, XMLLANG_LEN))
                        yield BINDING_VALUE_LANG;
                    yield null;
                }
                default -> null;
            };
            if (next == null) throw badProperty(this, r, b, e);
            return next;
        }

        public String expectedPropertiesString() {
            return switch (this) {
                case ROOT -> "head/results/boolean";
                case HEAD -> "vars";
                case BINDING_ROW -> "var names";
                case BINDING_VALUE -> "type/datatype/value/xml:lang";
                default -> "(no properties expected)";
            };
        }

        public SparqlState forArrayItem(JsonParser<?> p) {
            return switch (this) {
                case ROOT -> ROOT;
                case IGNORE, VARS -> IGNORE;
                case BINDINGS -> {
                    p.beginRow();
                    yield BINDING_ROW;
                }
                default -> throw ex(this, L_BRACKET, 0, 1);
            };
        }

        public void onObjectEnd(JsonParser<?> p) throws CancelledException, TerminatedException {
            switch (this) {
                case ROOT -> p.feedEnd();
                case IGNORE, HEAD, RESULTS -> {}
                case BINDING_VALUE -> {
                    final MutableRope v = p.value;
                    Batch<?> rb = p.batch;
                    int col = p.column;
                    switch (p.type) {
                        case null -> {
                            if (p.value.len > 0 || p.lang.len > 0 || p.dtSuffix.len > 0)
                                throw p.noType();
                        }
                        case IRI -> {
                            var prefix = SHARED_ROPES.internPrefixOf(v, 0, v.len);
                            int prefixLen = prefix == null ? 0 : prefix.len;
                            rb.putTerm(col, prefix, v.segment, v.u8(), prefixLen,
                                       v.len-prefixLen, false);
                        }
                        case LIT -> {
                            byte[] u8 = v.u8();
                            u8[0] = '"';  u8[v.len-1] = '"'; //replace <> with ""
                            FinalSegmentRope sh;
                            int localLen;
                            if (p.dtSuffix.len == 0) {
                                sh = null;
                                if (p.lang.len > 0) {
                                    p.lang.replace('_', '-');
                                    v.append('@').append(p.lang);
                                }
                                localLen = v.len;
                            } else  {
                                sh = SHARED_ROPES.internDatatype(p.dtSuffix, 0, p.dtSuffix.len);
                                // fastersparql convention (Batch & Term) is never store
                                // datatypes for lang strings or strings. RDF 1.1 allows that
                                // and turtle disallows appending DT_langString
                                if (sh == SharedRopes.DT_string || sh == SharedRopes.DT_langString)
                                    sh = null;
                                localLen = sh == null ? v.len : v.len-1;
                            }
                            rb.putTerm(col, sh, v.segment, v.u8(), 0,
                                       localLen, true);
                        }
                        case BLANK -> {
                            p.dtSuffix.clear().append('_').append(':')
                                      .append(v, 1, v.len-1);
                            rb.putTerm(col, null, p.dtSuffix.segment, p.dtSuffix.u8(),
                                       0, p.dtSuffix.len, false);
                        }
                        default -> throw new UnsupportedOperationException();
                    }
                }
                case BINDING_ROW -> p.commitRow();
                case VARS, BOOLEAN, BINDINGS, BINDING_VALUE_TYPE,
                        BINDING_VALUE_DATATYPE, BINDING_VALUE_LANG, BINDING_VALUE_VALUE
                        -> throw ex(this, L_BRACE, 0, 1);
            }
        }

        public void onArrayEnd(JsonParser<?> ignored) {
            switch (this) {
                case ROOT, IGNORE, VARS, BINDINGS  -> { }
                default -> throw ex(this, L_BRACKET, 0, 1);
            }
        }

        public void onNull(JsonParser<?> parser) throws CancelledException, TerminatedException {
            switch (this) {
                case BINDING_VALUE_VALUE, ROOT -> throw ex(this, NULL, 0, NULL.len);
                case IGNORE -> { }
                case BOOLEAN -> onBool(parser, false);
            }
        }

        public void onBool(JsonParser<?> p, boolean value) throws CancelledException, TerminatedException {
            var r = value ? TRUE : FALSE;
            switch (this) {
                case BOOLEAN -> {
                    if (value) {
                        if (!p.incompleteRow) p.beginRow();
                        p.commitRow();
                    }
                }
                case IGNORE -> {}
                case BINDING_VALUE_VALUE
                        -> p.value.clear().append('<').append(r, 0, r.len).append('>');
                default -> throw ex(this, r, 0, r.len);
            }
        }

        public void onNumber(JsonParser<?> p, SegmentRope r, int b, int e) throws CancelledException, TerminatedException {
            switch (this) {
                case BINDING_VALUE_VALUE -> p.value.clear().append('<').append(r, b, e).append('>');
                case BOOLEAN -> {
                    switch (b == e-1 ? r.get(b) : 0) {
                        case '0' -> onBool(p, false);
                        case '1' -> onBool(p, true);
                        default  -> throw ex(this, r, b, e);
                    }
                }
                case IGNORE -> {}
                default -> throw ex(this, r, b, e);
            }
        }

        public void onString(JsonParser<?> p, SegmentRope r, int b, int e) throws CancelledException, TerminatedException {
            switch (this) {
                case IGNORE -> {}
                case BOOLEAN -> {
                    int len = e - b;
                    if      (len == 4 && r.hasAnyCase(b, CONSTS,  TRUE_O,  TRUE_LEN))
                        onBool(p, true);
                    else if (len == 5 && r.hasAnyCase(b, CONSTS, FALSE_O, FALSE_LEN))
                        onBool(p, false);
                    else
                        throw ex(this, r, b, e);
                }
                case BINDING_VALUE_TYPE -> {
                    int off1 = 0, len1 = 0, off2 = 0, len2 = 0;
                    p.type = switch (b < e ? r.get(b) : 0) {
                        case 'i', 'I' -> {off1 = IRI_O; len1 = IRI_LEN; yield Term.Type.IRI;}
                        case 'u', 'U' -> {off1 = URI_O; len1 = URI_LEN; yield Term.Type.IRI;}
                        case 'b', 'B' -> {
                            off1 = BNODE_O; len1 = BNODE_LEN;
                            off2 = BLANK_O; len2 = BLANK_LEN;
                            yield Term.Type.BLANK;
                        }
                        case 'l', 'L' -> {
                            off1 = LITERAL_O; len1 = LITERAL_LEN;
                            off2 = LIT_O; len2 = LIT_LEN;
                            yield Term.Type.LIT;
                        }
                        case 't', 'T' -> { // virtuoso uses typed-literal
                            off1 = TYPED_LITERAL_O; len1 = TYPED_LITERAL_LEN;
                            yield Term.Type.LIT;
                        }
                        default -> Term.Type.LIT;
                    };
                    if (       (len1 == 0 || !r.hasAnyCase(b, CONSTS, off1, len1))
                            && (len2 == 0 || !r.hasAnyCase(b, CONSTS, off2, len2))) {
                        throw ex(this, r, b, e);
                    }
                }
                case BINDING_VALUE_DATATYPE ->
                    p.dtSuffix.clear().append(FinalSegmentRope.DT_MID_LT).append(r, b, e).append('>');
                case BINDING_VALUE_VALUE -> p.value.clear().append('<').append(r, b, e).append('>');
                case BINDING_VALUE_LANG  -> p.lang.clear().append(r, b, e);
                default -> throw ex(this, r, b, e);
            }
        }
    }

    /* --- --- --- JSON-level parsing --- --- --- */

    private enum JsonState {
        VALUE,
        OBJECT,
        ARRAY;

        public int parse(JsonParser<?> parser, SegmentRope r, int b, int e) throws CancelledException, TerminatedException {
            byte c = r.get(b);
            if (c == ',' && this != VALUE) {
                if ((b = r.skipWS(b+1, e)) == e) return parser.suspend(r, b, e);
                c = r.get(b);
            }
            SparqlState spState = parser.sparqlStack.peek();
            if (spState == null)
                throw new IllegalStateException("No SPARQL state");
            int stop = switch (this) {
                case VALUE -> switch (c) {
                    case '[', '{' -> {
                        parser.jsonStack.pop();
                        parser.jsonStack.push(c == '[' ? ARRAY : OBJECT);
                        yield b+1;
                    }
                    case '"' -> {
                        int dq = r.skipUntilUnescaped(b + 1, e, (byte)'"');
                        if (dq == e) yield -1;
                        parser.pop();
                        spState.onString(parser, r, b+1, dq);
                        yield dq+1;
                    }
                    default -> {
                        int ue = r.skip(b, e, UNQUOTED_VALUE);
                        if (ue == e) yield -1;
                        parser.pop();
                        if ((c == 'n' || c == 'N') && r.hasAnyCase(b, CONSTS, NULL_O, NULL_LEN))
                            spState.onNull(parser);
                        else if ((c == 'f' || c == 'F') && r.hasAnyCase(b, CONSTS, FALSE_O, FALSE_LEN))
                            spState.onBool(parser, false);
                        else if ((c == 't' || c == 'T') && r.hasAnyCase(b, CONSTS, TRUE_O, TRUE_LEN))
                            spState.onBool(parser, true);
                        else if (Rope.contains(SparqlSkip.NUMBER_FIRST, c))
                            spState.onNumber(parser, r, b, ue);
                        else
                            throw ex(this, r, b, ue);
                        yield ue;
                    }
                };
                case OBJECT -> {
                    switch (c) {
                        case '"' -> {
                            int dq = r.skipUntilUnescaped(b+1, e, (byte)'"');
                            int colon = r.skipUntil(dq, e, (byte)':');
                            if (colon == e) yield -1;
                            parser.push(spState.forProperty(parser, r, b+1, dq));
                            yield colon+1;
                        }
                        case '}' -> {
                            parser.pop();
                            spState.onObjectEnd(parser);
                            yield b+1;
                        }
                        default -> throw ex(OBJECT, r, b, e);
                    }
                }
                case ARRAY -> {
                    if (c == ']') {// consume array close and do not return to stack
                        parser.pop();
                        spState.onArrayEnd(parser);
                        yield b + 1;
                    }
                    //parse item value:
                    parser.push(spState.forArrayItem(parser));
                    yield b;
                }
            };
            return stop == -1 ? parser.suspend(r, b, e) : stop;
        }
    }
}
