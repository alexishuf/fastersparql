package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.Skip;
import com.github.alexishuf.fastersparql.operators.FSOps;
import com.github.alexishuf.fastersparql.operators.FSOpsProperties;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Values;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.expr.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.Skip.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;

public class SparqlParser<R, I> {
    private static final String BASE = ":BASE";
    private static final long[] GROUP_FOLLOW = alphabet("{").whitespace().get();
    private static final long[] VALUES_FOLLOW = alphabet("({").whitespace().get();
    private static final long[] FILTER_FOLLOW = alphabet("(").whitespace().get();

    private final RowType<R, I> rt;
    private String in;
    private int pos, end;
    private final ExprParser exprParser = new ExprParser();
    private final GroupParser groupParser = new GroupParser();
    private boolean distinct, reduce;
    private long limit;
    private @Nullable Vars projection;

    public SparqlParser(RowType<R, I> rt) {
        this.exprParser.rowType(this.rt = rt);
        this.exprParser.termParser.prefixMap = new PrefixMap().resetToBuiltin();
    }

    /**
     * Parse a {@code {...}} block (i.e., {@code GroupGraphPattern} in SPARQL grammar).
     *
     * @param sparql The string containing the block.
     * @param start index of the {@code {}} block in {@code sparql}.
     * @param prefixMap Use the given prefixes when parsing prefixed IRIs and
     *                  datatypes within the group. Any prefix set for this parser due to an
     *                  earlier {@code parse()}/{@code parseGroup()} will be discarded.
     *                  The {@code rdf:} and {@code xsd:} prefixes will always be defined but the
     *                  given {@code prefixMap} may redefine them.
     * @return The {@link Plan} equivalent to the block.
     */
    public Plan<R, I> parseGroup(String sparql, int start, PrefixMap prefixMap) {
        this.end = (this.in = sparql).length();
        this.pos = start;
        exprParser.termParser.prefixMap.resetToBuiltin().addAll(prefixMap);
        exprParser.input(sparql);
        require("{");
        Plan<R, I> plan = new GroupParser().read();
        require("}");
        return plan;

    }
    public Plan<R, I> parse(String query, int start) {
        end = (in = query).length();
        pos = start;
        exprParser.termParser.prefixMap.resetToBuiltin();
        groupParser.reset();
        distinct = reduce = false;
        limit = Long.MAX_VALUE;
        projection = null;
        exprParser.input(query);
        pPrologue();
        pVerb();
        require("{");
        Plan<R, I> where = groupParser.read();
        require("}");
        return pModifiers(where);
    }

    /** Get index of the first char not consumed by this parser from its input string. */
    public int pos() { return pos; }

    private InvalidSparqlException ex(String expected, int where) {
        int acBegin = skip(in, where, end, WS), acEnd = skip(in, acBegin, end, UNTIL_WS);
        String actual = acBegin >= end ? "EOF" : "\""+in.substring(acBegin, acEnd)+"\"";
        throw new InvalidSparqlException("Expected "+expected+" at position "+where+", got "+actual);
    }

    private I pTerm() {
        skipWS();
        var termParser = exprParser.termParser;
        if (!termParser.parse(in, pos, end))
            throw ex("RDF term/var", pos);
        I term;
        if (termParser.isNTOrVar()) {
            term = rt.fromNT(in, pos, termParser.termEnd());
        } else {
            String nt = termParser.asNT();
            term = rt.fromNT(nt, 0, nt.length());
        }
        pos = termParser.termEnd();
        return term;
    }

    private String pVar() {
        int begin = pos;
        if (isVar(in, pos, end))
            return in.substring(begin+1, pos = skip(in, begin+1, end, VARNAME));
        throw ex("var", begin);
    }

    private String pIri() {
        skipWS();
        if (pos < end && in.charAt(pos) == '<') {
            int e = skip(in, pos+1, end, IRIREF);
            if (e < end && in.charAt(e) == '>') {
                String iri = in.substring(pos + 1, e);
                pos = e+1;
                return iri;
            }
        }
        throw ex("IRI", pos);
    }

    private long pLong() {
        int begin = pos;
        String str = in.substring(begin, pos = skip(in, pos, end, DIGITS));
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) { throw  ex("integer", begin); }
    }

    private char skipWS() {
        while (true) {
            pos = skip(in, pos, end, Skip.WS);
            char c = pos == end ? '\0' : in.charAt(pos);
            if (c != '#') return c;
            pos = skipUntil(in, pos, end, '\n');
        }
    }

    /** Whether the next token (after WS and comments) is {@code token} (case insensitive). */
    private boolean peek(String token) {
        skipWS();
        return in.regionMatches(true, pos, token, 0, token.length());
    }

    /**
     * If the next token is {@code token} update {@code pos} to consume it.
     *
     * @return true iff next token was {@code token}.
     */
    private boolean poll(String token, long [] follow) {
        if (peek(token)) {
            int next = pos + token.length();
            if (contains(follow, next < end ? in.charAt(next) : '\0')) {
                pos = next;
                skipWS();
                return true;
            }
        }
        return false;
    }

    /** Consume {@code token} or raise an exception if next token is not {@code token}. */
    private char require(String token) {
        if (!peek(token))
            throw ex(token, pos);
        pos += token.length();
        return skipWS();
    }

    private void pPrologue() {
        PrefixMap prefixMap = exprParser.termParser.prefixMap;
        while (true) {
            switch (skipWS()) {
                case 'p', 'P' -> {
                    require("PREFIX");
                    prefixMap.add(pPNameNS(), pIri());
                    poll(".", ANY); // tolerate TTL '.' that should not be here
                }
                case 'b', 'B' -> {
                    require("BASE");
                    prefixMap.add(BASE, pIri());
                    poll(".", ANY); // tolerate TTL '.' that should not be here
                }
                default -> { return; }
            }
        }
    }

    private String pPNameNS() {
        if (skipWS() == '\0')
            throw ex("PNAME_NS", pos);
        int nameBegin = pos;
        String name = in.substring(nameBegin, pos = skip(in, pos, end, PN_PREFIX));
        require(":");
        return name;
    }

    private void pVerb() {
        switch (skipWS()) {
            case 'a', 'A' -> {
                require("ASK");
                limit = 1;
                projection = Vars.EMPTY;
                reduce = true;
            }
            case 's', 'S' -> pSelect();
            default -> throw ex("SELECT or ASK", pos);
        }
        char c = skipWS();
        if      (c == 'W' || c == 'w') require("WHERE");
        else if (c != '{')             throw ex("WHERE or {", pos);
    }

    private void pSelect() {
        char c = require("SELECT");
        while (true) {
            switch (c) {
                case '{', 'w', 'W', '\0' -> { return; }
                case 'd', 'D'            -> { require("DISTINCT"); distinct = true; }
                case 'r', 'R'            -> { require("REDUCED"); reduce = true; }
                case '*'                 -> { pos++; projection = null; }
                case '('                 -> throw new InvalidSparqlException("binding vars to expressions '(expr AS ?var)' is not supported yet");
                case 'f', 'F'            -> {
                    require("FROM");
                    throw new InvalidSparqlException("FROM clauses are not supported");
                }
                case '?', '$'           -> {
                    if (projection == null)
                        projection = new Vars.Mutable(10);
                    projection.add(pVar());
                }
            }
            c = skipWS();
        }
    }

    private Plan<R, I> pModifiers(Plan<R, I> where) {
        long offset = 0;
        for (char c = skipWS(); c != '\0'; c = skipWS()) {
            switch (c) {
                case 'l', 'L' -> {
                    require("LIMIT");
                    limit = pLong();
                }
                case 'o', 'O' -> {
                    require("OFFSET");
                    offset = pLong();
                }
                default -> {
                    String actual = in.substring(pos, skip(in, pos, end, UNTIL_WS));
                    throw new InvalidSparqlException("Unsupported modifier: "+actual);
                }
            }
        }
        if (limit != Long.MAX_VALUE || offset > 0 || projection != null || distinct || reduce) {
            int distinctWindow = reduce ? FSOpsProperties.reducedCapacity()
                               : distinct ? Integer.MAX_VALUE : 0;
            return FSOps.modifiers(where, projection, distinctWindow, offset, limit, List.of());
        }
        return where;
    }

    private class GroupParser {
        private final List<Plan<R, I>> operands = new ArrayList<>();
        private List<Plan<R, I>> optionals, minus;
        private Values<R, I> values;
        private List<Expr> filters;

        void reset() {
            operands.clear();
            if (optionals != null)
                optionals.clear();
            if (minus != null)
                minus.clear();
            values = null;
            if (filters != null)
                filters.clear();
        }

        Plan<R, I> read() {
            if (skipWS() == '{')
                operands.add(readBlock());

            I s = null, p = null, o;
            char c = skipWS();
            while (c != '}' && c != '\0') {
                if (s != null || p != null || !readModifiers()) {
                    // preceded by ','/';' or no modifiers
                    if (s == null) s = pTerm(); // read subject   if not retained (','/';')
                    if (p == null) p = pTerm(); // read predicate if not retained (',')
                    o = pTerm();
                    operands.add(new TriplePattern<>(rt, s, p, o));
                } // else: parsed one or more modifiers, check for and consume '.'
                switch ((c = skipWS())) {
                    case ',' -> { ++pos; c = skipWS();               } // next TP shares s & p
                    case ';' -> { ++pos; c = skipWS(); p = null;     } // next TP shares s
                    case '.' -> { ++pos; c = skipWS(); s = p = null; } // consume '.'
                    default  ->                        s = p = null;   // no '.' (yet)
                }
            }

            Plan<R, I> plan = switch (operands.size()) {
                case 0  -> throw ex("Non-empty GroupGraphPattern", pos);
                case 1  -> operands.get(0);
                default -> new Join<>(new ArrayList<>(operands), null, null);
            };
            plan = optionals == null ? plan : withOptionals(plan);
            plan = minus     == null ? plan : withMinus(plan);
            plan = filters   == null ? plan : withFilters(plan);
            plan = values    == null ? plan : FSOps.join(values, plan);
            return plan;
        }

        @RequiresNonNull("optionals")
        private Plan<R, I> withOptionals(Plan<R, I> left) {
            for (Plan<R, I> right : optionals)
                left = FSOps.leftJoin(left, right);
            return left;
        }

        @RequiresNonNull("minus")
        private Plan<R, I> withMinus(Plan<R, I> left) {
            for (Plan<R, I> right : minus)
                left = FSOps.minus(left, right);
            return left;
        }

        @RequiresNonNull("filters")
        private Plan<R, I> withFilters(Plan<R, I> left) {
            List<Expr.Exists<R, I>> existsList = null;
            List<Expr> filtersList = null;
            for (int i = 0; i < filters.size(); i++) {
                Expr e = filters.get(i);
                if (e instanceof Expr.Exists<?, ?> exists) {
                    if (existsList == null) {
                        existsList = new ArrayList<>(filters.size());
                        filtersList = new ArrayList<>(filters.size());
                        for (int j = 0; j < i; j++)
                            filtersList.add(filters.get(i));
                    }
                    //noinspection unchecked
                    existsList.add((Expr.Exists<R, I>) exists);
                } else if (filtersList != null) {
                    filtersList.add(e);
                }
            }
            if (filtersList == null)
                filtersList = filters;
            if (!filtersList.isEmpty())
                left = FSOps.filter(left, filtersList);
            if (existsList != null) {
                for (Expr.Exists<R, I> ex : existsList)
                    left = FSOps.exists(left, ex.negate(), ex.filter());
            }
            return left;
        }

        private boolean readModifiers() {
            int startPos = pos;
            for (boolean has = true; has; ) {
                switch (skipWS()) {
                    case 'o', 'O' -> {
                        has = poll("OPTIONAL", GROUP_FOLLOW);
                        if (has) {
                            if (optionals == null)
                                optionals = new ArrayList<>();
                            optionals.add(new GroupParser().read());
                        }
                    }
                    case 'f', 'F' -> {
                        has = poll("FILTER", FILTER_FOLLOW);
                        if (has) {
                            if (filters == null)
                                filters = new ArrayList<>();
                            filters.add(exprParser.parse(pos));
                            pos = exprParser.consumedPos();
                        }
                    }
                    case 'm', 'M' -> {
                        has = poll("MINUS", GROUP_FOLLOW);
                        if (has) {
                            if (minus == null)
                                minus = new ArrayList<>();
                            minus.add(new GroupParser().read());
                        }
                    }
                    case 'v', 'V' -> {
                        has = poll("VALUES", VALUES_FOLLOW);
                        if (has)
                            mergeValues(readValues());
                    }
                    default -> has = false; // found nothing, exit this loop
                }
            }
            return pos != startPos;
        }

        private Values<R, I> readValues() {
            require("(");
            var vars = new Vars.Mutable(10);
            while (pos < end && isVar(in, pos, end)) {
                vars.add(pVar());
                skipWS();
            }
            require(")");
            int n = vars.size();
            require("{");
            List<R> rows = new ArrayList<>();
            while (poll("(", ANY)) {
                R row = rt.createEmpty(vars);
                for (int i = 0; i < n; i++)
                    rt.set(row, i, pTerm());
                require(")");
            }
            require("}");
            return FSOps.values(rt, vars, rows);
        }

        private void mergeValues(Values<R, I> v) {
            if (this.values == null) {
                this.values = v;
            } else {
                Vars union = this.values.publicVars().union(v.publicVars());
                if (union == this.values.publicVars()) {
                    this.values.rows().addAll(v.rows());
                } else {
                    var rows = new ArrayList<R>();
                    var projector = Merger.forProjection(rt, union, this.values.publicVars());
                    for (R row : this.values.rows())
                        rows.add(projector.merge(row, null));
                    projector = Merger.forProjection(rt, union, v.publicVars());
                    for (R row : v.rows())
                        rows.add(projector.merge(row, null));
                    this.values = FSOps.values(rt, union, rows);
                }
            }
        }

        private Plan<R, I> readBlock() {
            Plan<R, I> plan = new GroupParser().read();
            require("}");
            if (poll("UNION", GROUP_FOLLOW))
                return FSOps.union(List.of(plan, new GroupParser().read()));
            else if (peek("{"))
                return FSOps.union(List.of(plan, new GroupParser().read()));
            return plan;
        }
    }
}
