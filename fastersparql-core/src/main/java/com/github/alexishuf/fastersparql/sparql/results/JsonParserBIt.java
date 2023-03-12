package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;

import static java.lang.String.format;
import static java.lang.String.join;

public final class JsonParserBIt<R> extends ResultsParserBIt<R> {
    private @Nullable ByteRope partial = null, allocPartial = null;
    private final ArrayDeque<JsonState> jsonStack = new ArrayDeque<>();
    private final ArrayDeque<SparqlState> sparqlStack = new ArrayDeque<>();
    private final TermParser termParser = new TermParser();
    private boolean hadSparqlProperties = false;
    private int column;
    private @Nullable Term datatype;
    private final ByteRope value = new ByteRope(), lang = new ByteRope();
    private final ByteRope datatypeTmp = new ByteRope();
    private Term. @Nullable Type type;

    public static final class JsonFactory implements Factory {
        @Override public SparqlResultFormat name() { return SparqlResultFormat.JSON; }
        @Override public <R> ResultsParserBIt<R> create(RowType<R> rowType, Vars vars) {
            return new JsonParserBIt<>(rowType, vars);
        }
        @Override public <R> ResultsParserBIt<R> create(RowType<R> rowType, CallbackBIt<R> dst) {
            return new JsonParserBIt<>(rowType, dst);
        }
    }

    public JsonParserBIt(RowType<R> rowType, Vars vars) {
        super(rowType, vars);
        push(SparqlState.ROOT);
    }

    public JsonParserBIt(RowType<R> rowType, CallbackBIt<R> destination) {
        super(rowType, destination);
        push(SparqlState.ROOT);
    }

    /* --- --- --- implement/override ResultsParserBIt methods --- --- --- */

    @Override protected void doFeedShared(Rope rope) {
        if (partial != null) {
            rope = partial.append(rope);
            partial = null;
        }
        int begin = 0, end = rope.len();
        while ((begin = rope.skipWS(begin, end)) < end)
            begin = jsonStack.getFirst().parse(this, rope, begin, end);
    }

    @Override public void complete(@Nullable Throwable error) {
        if (error == null && !isComplete() && !hadSparqlProperties)
            error = new InvalidSparqlResultsException("No \"results\" object nor \"boolean\" value in JSON");
        super.complete(error);
    }

    /* --- --- --- constants --- --- --- */

    private static final ByteRope L_BRACKET = new ByteRope("[");
    private static final ByteRope L_BRACE = new ByteRope("{");
    private static final ByteRope NULL = new ByteRope("NULL");
    private static final ByteRope TRUE = new ByteRope("TRUE");
    private static final ByteRope FALSE = new ByteRope("FALSE");
    private static final ByteRope IRI = new ByteRope("IRI");
    private static final ByteRope URI = new ByteRope("URI");
    private static final ByteRope LITERAL = new ByteRope("LITERAL");
    private static final ByteRope LIT = new ByteRope("LIT");
    private static final ByteRope BNODE = new ByteRope("BNODE");
    private static final ByteRope BLANK = new ByteRope("BLANK");
    private static final ByteRope P_HEAD = new ByteRope("HEAD");
    private static final ByteRope P_RESULTS = new ByteRope("RESULTS");
    private static final ByteRope P_BOOLEAN = new ByteRope("BOOLEAN");
    private static final ByteRope P_VARS = new ByteRope("VARS");
    private static final ByteRope P_BINDINGS = new ByteRope("BINDINGS");
    private static final ByteRope VALUE = new ByteRope("VALUE");
    private static final ByteRope TYPE = new ByteRope("TYPE");
    private static final ByteRope DATATYPE = new ByteRope("DATATYPE");
    private static final ByteRope XMLLANG = new ByteRope("XML:LANG");

    private static final int[] UNQUOTED_VALUE = Rope.invert(Rope.alphabet(",}]", Rope.Range.WS));


    /* --- --- --- exception builders --- --- --- */

    private static InvalidSparqlResultsException ex(SparqlState state, Rope r, int b, int e) {
        e = r.skip(b = r.skipWS(b, e), e, Rope.UNTIL_WS);
        var msg = format("JSON parser at state %s expected %s but got %s",
                state, state.expected(), b == e ? "End-Of-Input" : r.sub(b, e).toString());
        return new InvalidSparqlResultsException(msg);
    }

    private static InvalidSparqlResultsException ex(JsonState state, Rope r, int b, int e) {
        e = r.skip(b = r.skipWS(b, e), e, Rope.UNTIL_WS);
        var got = b == e ? "End-Of-Input" : r.sub(b, e).toString();
        var msg = format("Malformed JSON: expected %s but got %s",
                         state.name().toLowerCase(), got);
        return new InvalidSparqlResultsException(msg);
    }

    private static InvalidSparqlResultsException badProperty(SparqlState state, Rope r, int b, int e) {
        var name = r.sub(b, e);
        var msg = format("Unexpected property %s at state %s, expected %s",
                         name, state, join("/", state.expectedPropertiesString()));
        return new InvalidSparqlResultsException(msg);
    }

    private InvalidSparqlResultsException noType() {
        var msg = format("No \"type\" property given for RDF value \"%s\" with " +
                         "datatype=%s and xml:lang=%s",
                         value, datatype==null ? "null" : datatype, lang.len>0 ? lang : "null");
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

    private int suspend(Rope r, int b, int e) {
        if (b >= e) return e;
        partial = allocPartial == null ? allocPartial=new ByteRope(32+(e-b)) : allocPartial;
        if (r == partial)
            partial.erase(e, partial.len).erase(0, b);
        else
            partial.clear().append(r, b, e);
        return e;
    }

    private void emit() { feed(builder.build()); }

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

        public SparqlState forProperty(JsonParserBIt<?> p, Rope r, int b, int e) {
            int l = e - b;
             SparqlState next = switch (this) {
                case ROOT -> {
                    SparqlState s;
                    if      (l==4 && r.hasAnyCase(b, P_HEAD.utf8))    s = HEAD;
                    else if (l==7 && r.hasAnyCase(b, P_RESULTS.utf8)) s = RESULTS;
                    else if (l==7 && r.hasAnyCase(b, P_BOOLEAN.utf8)) s = BOOLEAN;
                    else                                              s = null;
                    if (s != null) p.hadSparqlProperties = true;
                    yield s;
                }
                case IGNORE -> IGNORE;
                case HEAD    -> l==4 && r.hasAnyCase(b, P_VARS.utf8)     ? VARS     : IGNORE;
                case RESULTS -> l==8 && r.hasAnyCase(b, P_BINDINGS.utf8) ? BINDINGS : IGNORE;
                case BINDING_ROW -> {
                    p.datatype = null;
                    p.type = null;
                    p.value.clear();
                    p.lang.clear();
                    yield (p.column = p.vars.indexOf(r.sub(b, e))) >= 0 ? BINDING_VALUE : IGNORE;
                }
                case BINDING_VALUE -> {
                    if   (l==5 && r.hasAnyCase(b, VALUE.utf8))    yield BINDING_VALUE_VALUE;
                    if   (l==4 && r.hasAnyCase(b, TYPE.utf8))     yield BINDING_VALUE_TYPE;
                    if   (l==8 && r.hasAnyCase(b, DATATYPE.utf8)) yield BINDING_VALUE_DATATYPE;
                    yield l==8 && r.hasAnyCase(b, XMLLANG.utf8)   ?     BINDING_VALUE_LANG : null;
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

        public SparqlState forArrayItem() {
            return switch (this) {
                case ROOT -> ROOT;
                case IGNORE, VARS -> IGNORE;
                case BINDINGS -> BINDING_ROW;
                default -> throw ex(this, L_BRACKET, 0, 1);
            };
        }

        public void onObjectEnd(JsonParserBIt<?> p) {
            switch (this) {
                case ROOT -> p.complete(null);
                case IGNORE, HEAD, RESULTS -> {}
                case BINDING_VALUE -> {
                    final ByteRope v = p.value;
                    Term term = switch (p.type) {
                        case null -> {
                            if (p.value.len > 0 || p.lang.len > 0 || p.datatype != null)
                                throw p.noType();
                            yield null;
                        }
                        case IRI -> Term.valueOf(v);
                        case LIT -> {
                            v.utf8[0] = v.utf8[v.len-1] = '"'; //replace <> with ""
                            if (p.lang.len > 0) {
                                p.lang.replace('_', '-');
                                yield Term.wrap(v.append('@').append(p.lang).toArray(0, v.len));
                            } else if (p.datatype == null) {
                                yield Term.wrap(v.toArray(0, v.len));
                            } else  {
                                int id = p.datatype.asDatatypeId();
                                if (id > 0) {
                                    byte[] openLex = v.toArray(0, v.len - 1);
                                    yield Term.typed(openLex, 0, openLex.length, id);
                                }
                                yield Term.valueOf(v.append(p.datatype));
                            }
                        }
                        case BLANK -> {
                            byte[] prefixed = new byte[v.len];
                            prefixed[0] = '_';
                            prefixed[1] = ':';
                            v.copy(1, v.len-1, prefixed, 2);
                            yield Term.wrap(prefixed);
                        }
                        default -> throw new UnsupportedOperationException();
                    };
                    p.builder.set(p.column, term);
                }
                case BINDING_ROW -> p.emit();
                case VARS, BOOLEAN, BINDINGS, BINDING_VALUE_TYPE,
                        BINDING_VALUE_DATATYPE, BINDING_VALUE_LANG, BINDING_VALUE_VALUE
                        -> throw ex(this, L_BRACE, 0, 1);
            }
        }

        public void onArrayEnd(JsonParserBIt<?> ignored) {
            switch (this) {
                case ROOT, IGNORE, VARS, BINDINGS  -> { }
                default -> throw ex(this, L_BRACKET, 0, 1);
            }
        }

        public void onNull(JsonParserBIt<?> parser) {
            switch (this) {
                case BINDING_VALUE_VALUE, ROOT -> throw ex(this, NULL, 0, NULL.len);
                case IGNORE -> { }
                case BOOLEAN -> onBool(parser, false);
            }
        }

        public void onBool(JsonParserBIt<?> p, boolean value) {
            var r = value ? TRUE : FALSE;
            switch (this) {
                case BOOLEAN -> { if (value) p.emit(); }
                case IGNORE -> {}
                case BINDING_VALUE_VALUE
                        -> p.value.clear().append('<').append(r, 0, r.len).append('>');
                default -> throw ex(this, r, 0, r.len);
            }
        }

        public void onNumber(JsonParserBIt<?> p, Rope r, int b, int e) {
            switch (this) {
                case BINDING_VALUE_VALUE -> p.value.clear().append('<').append(r, b, e).append('>');
                case BOOLEAN -> {
                    switch (b == e-1 ? r.get(b) : 0) {
                        case '0' -> onBool(p, false);
                        case '1' -> onBool(p, true);
                        default  -> throw ex(this, r, b, e);
                    };
                }
                case IGNORE -> {}
                default -> throw ex(this, r, b, e);
            }
        }

        public void onString(JsonParserBIt<?> p, Rope r, int b, int e) {
            switch (this) {
                case IGNORE -> {}
                case BOOLEAN -> {
                    int len = e - b;
                    if      (len == 4 && r.hasAnyCase(b,  TRUE.utf8)) onBool(p, true);
                    else if (len == 5 && r.hasAnyCase(b, FALSE.utf8)) onBool(p, false);
                    else throw ex(this, r, b, e);
                }
                case BINDING_VALUE_TYPE -> {
                    ByteRope v1 = null, v2 = null;
                    p.type = switch (b < e ? r.get(b) : 0) {
                        case 'i', 'I' -> {v1 = IRI; yield Term.Type.IRI;}
                        case 'u', 'U' -> {v1 = URI; yield Term.Type.IRI;}
                        case 'b', 'B' -> {v1 = BNODE; v2 = BLANK; yield Term.Type.BLANK;}
                        case 'l', 'L' -> {v1 = LITERAL; v2 = LIT; yield Term.Type.LIT;}
                        default -> Term.Type.LIT;
                    };
                    if ((v1 == null || !r.hasAnyCase(b, v1.utf8))
                            && (v2 == null || !r.hasAnyCase(b, v2.utf8))) {
                        throw ex(this, r, b, e);
                    }
                }
                case BINDING_VALUE_DATATYPE -> {
                    ByteRope iri = p.datatypeTmp.clear().append('<').append(r, b, e).append('>');
                    p.datatype = p.termParser.parseTerm(iri);
                }
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

        public int parse(JsonParserBIt<?> parser, Rope r, int b, int e) {
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
                        int dq = r.skipUntilUnescaped(b + 1, e, '"');
                        if (dq == e) yield -1;
                        parser.pop();
                        spState.onString(parser, r, b+1, dq);
                        yield dq+1;
                    }
                    default -> {
                        int ue = r.skip(b, e, UNQUOTED_VALUE);
                        if (ue == e) yield -1;
                        parser.pop();
                        if ((c == 'n' || c == 'N') && r.hasAnyCase(b, NULL.utf8))
                            spState.onNull(parser);
                        else if ((c == 'f' || c == 'F') && r.hasAnyCase(b, FALSE.utf8))
                            spState.onBool(parser, false);
                        else if ((c == 't' || c == 'T') && r.hasAnyCase(b, TRUE.utf8))
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
                            int dq = r.skipUntilUnescaped(b+1, e, '"');
                            int colon = r.skipUntil(dq, e, ':');
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
                    parser.push(spState.forArrayItem());
                    yield b;
                }
            };
            return stop == -1 ? parser.suspend(r, b, e) : stop;
        }
    }
}
