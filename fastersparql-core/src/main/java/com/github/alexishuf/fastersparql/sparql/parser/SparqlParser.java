package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.operators.plan.Values;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprParser;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.model.rope.Rope.*;
import static com.github.alexishuf.fastersparql.model.rope.Rope.Range.WS;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SparqlParser {

    private SegmentRope in;
    private int start, pos, end;
    private final ExprParser exprParser = new ExprParser();
    private final GroupParser groupParser = new GroupParser();
    private boolean distinct, reduce;
    private long limit;
    private @Nullable Vars projection;

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
    public Plan parseGroup(SegmentRope sparql, int start, PrefixMap prefixMap) {
        this.end = (this.in = sparql).len();
        this.pos = start;
        exprParser.termParser.prefixMap.resetToBuiltin().addAll(prefixMap);
        exprParser.input(sparql);
        require('{');
        Plan plan = new GroupParser().read();
        require('}');
        return plan;

    }
    public Plan parse(SparqlQuery q) {
        return q instanceof Plan p ? p : parse(q.sparql());
    }
    public Plan parse(SegmentRope query) { return parse(query, 0); }
    public Plan parse(SegmentRope query, int start) {
        end = (in = query).len();
        this.start = start;
        pos = start;
        exprParser.termParser.prefixMap.resetToBuiltin();
        groupParser.reset();
        distinct = reduce = false;
        limit = Long.MAX_VALUE;
        projection = null;
        exprParser.input(query);
        pPrologue();
        pVerb();
        require('{');
        Plan where = groupParser.read();
        require('}');
        return pModifiers(where);
    }

    /** Get index of the first char not consumed by this parser from its input string. */
    public int pos() { return pos; }

    private InvalidSparqlException ex(Object expected, int where) {
        int acBegin = in.skipWS(where, end), acEnd = in.skip(acBegin, end, UNTIL_WS);
        String actual = acBegin >= end ? "EOF" : "\""+in.sub(acBegin, acEnd)+"\"";
        String ex = (expected instanceof byte[] ? new ByteRope(expected) : expected).toString();
        throw new InvalidSparqlException("Expected "+ex+" at position "+(where-start)+", got "+actual+" Full query: "+in.sub(start, end));
    }

    private Term pTerm() {
        skipWS();
        var termParser = exprParser.termParser;
        if (!termParser.parse(in, pos, end).isValid())
            throw ex("RDF term/var", pos);
        pos = termParser.termEnd();
        return termParser.asTerm();
    }

    private Rope pVarName() {
        int begin = pos+1, e = in.skip(begin, end, VARNAME);
        while (e > begin && in.get(e) == '.') --e;
        if (e == begin)
            throw ex("non-empty var name", pos);
        var rope = new ByteRope(e-begin).append(in, begin, e);
        pos = e;
        return rope;
    }

    private Term pIri() {
        skipWS();
        int begin = pos;
        if (begin < end && in.get(begin) == '<') {
            pos = in.requireThenSkipUntil(begin+1, end, Rope.LETTERS, '>');
            if (pos > begin+1 && pos < end) return Term.valueOf(in, begin, ++pos);
        }
        throw ex("IRI", pos = begin);
    }

    private long pLong() {
        int begin = pos;
        Rope str = in.sub(begin, pos = in.skip(pos, end, DIGITS));
        try {
            return Long.parseLong(str.toString());
        } catch (NumberFormatException e) { throw  ex("integer", begin); }
    }

    private byte skipWS() {
        while ((pos = in.skipWS(pos, end)) != end && in.get(pos) == '#')
            pos = in.skipUntil(pos, end, '\n');
        return pos == end ? 0 : in.get(pos);
    }

    /** If the next token is {@code token} update {@code pos} to consume it and return true. */
    private boolean poll(byte[] token, int [] follow) {
        skipWS();
        if (in.hasAnyCase(pos, token)) {
            int next = pos + token.length;
            if (Rope.contains(follow, next == end ? 0 : in.get(next))) {
                pos = next;
                skipWS();
                return true;
            }
        }
        return false;
    }

    /** If the next token is {@code token} update {@code pos} to consume it and return true. */
    private boolean poll(char token) {
        byte c = skipWS();
        if (c >= 'a' && c <= 'z') c -= 'a'-'A';
        if (c == token) {
            ++pos;
            skipWS();
            return true;
        }
        return false;
    }

    /** Consume {@code token} or raise an exception if next token is not {@code token}. */
    private byte require(byte[] token) {
        skipWS();
        if (!in.hasAnyCase(pos, token))
            throw ex(token, pos);
        pos += token.length;
        return skipWS();
    }

    /** Consume {@code token} or raise an exception if next token is not {@code token}. */
    private void require(char uppercase) {
        byte c = skipWS();
        if (c >= 'a' && c <= 'z') c -= 'a'-'A';
        if (c != uppercase)
            throw ex(new byte[]{c}, pos);
        ++pos;
        skipWS();
    }

    private static final Rope BASE = new ByteRope(":BASE");

    private void pPrologue() {
        PrefixMap prefixMap = exprParser.termParser.prefixMap;
        while (true) {
            switch (skipWS()) {
                case 'p', 'P' -> {
                    require(PREFIX_u8);
                    prefixMap.add(pPNameNS(), pIri());
                    poll('.'); // tolerate TTL '.' that should not be here
                }
                case 'b', 'B' -> {
                    require(BASE_u8);
                    prefixMap.add(BASE, pIri());
                    poll('.'); // tolerate TTL '.' that should not be here
                }
                default -> { return; }
            }
        }
    }

    private Rope pPNameNS() {
        if (skipWS() == '\0')
            throw ex("PNAME_NS", pos);
        int begin = pos;
        Rope name = in.sub(begin, pos = in.skip(pos, end, PN_PREFIX));
        require(':');
        return name;
    }

    private void pVerb() {
        switch (skipWS()) {
            case 'a', 'A' -> {
                require(ASK_u8);
                limit = 1;
                projection = Vars.EMPTY;
                reduce = false;
            }
            case 's', 'S' -> pSelect();
            default -> throw ex("SELECT or ASK", pos);
        }
        byte c = skipWS();
        if      (c == 'W' || c == 'w') require(WHERE_u8);
        else if (c != '{')             throw ex("WHERE or {", pos);
    }

    private void pSelect() {
        byte c = require(SELECT_u8);
        while (true) {
            switch (c) {
                case '{', 'w', 'W', '\0' -> { return; }
                case 'd', 'D'            -> { require(DISTINCT_u8); distinct = true; }
                case 'r', 'R'            -> { require(REDUCED_u8); reduce = true; }
                case '*'                 -> { pos++; projection = null; }
                case '('                 -> throw new InvalidSparqlException("binding vars to expressions '(expr AS ?var)' is not supported yet");
                case 'f', 'F'            -> {
                    require(FROM_u8);
                    throw new InvalidSparqlException("FROM clauses are not supported");
                }
                case '?', '$'           -> {
                    if (projection == null)
                        projection = new Vars.Mutable(10);
                    projection.add(pVarName());
                }
            }
            c = skipWS();
        }
    }

    private Plan pModifiers(Plan where) {
        long offset = 0;
        for (byte c = skipWS(); c != '\0'; c = skipWS()) {
            switch (c) {
                case 'l', 'L' -> {
                    require(LIMIT_u8);
                    limit = pLong();
                }
                case 'o', 'O' -> {
                    require(OFFSET_u8);
                    offset = pLong();
                }
                default -> throw ex("LIMIT/OFFSET", pos);
            }
        }
        if (limit != Long.MAX_VALUE || offset > 0 || projection != null || distinct || reduce) {
            int distinctWindow = reduce ? FSProperties.reducedCapacity()
                               : distinct ? Integer.MAX_VALUE : 0;
            return FS.modifiers(where, projection, distinctWindow, offset, limit, List.of());
        }
        return where;
    }

    private class GroupParser {
        private final List<Plan> operands = new ArrayList<>();
        private List<Plan> optionals, minus;
        private Values values;
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

        Plan read() {
            if (skipWS() == '{')
                operands.add(readBlock());

            Term s = null, p = null, o;
            byte c = skipWS();
            while (c != '}' && c != '\0') {
                if (s != null || p != null || !readModifiers()) {
                    // preceded by ','/';' or no modifiers
                    if (s == null) s = pTerm(); // read subject   if not retained (','/';')
                    if (p == null) p = pTerm(); // read predicate if not retained (',')
                    o = pTerm();
                    operands.add(new TriplePattern(s, p, o));
                } // else: parsed one or more modifiers, check for and consume '.'
                switch ((c = skipWS())) {
                    case ',' -> { ++pos; c = skipWS();               } // next TP shares s & p
                    case ';' -> { ++pos; c = skipWS(); p = null;     } // next TP shares s
                    case '.' -> { ++pos; c = skipWS(); s = p = null; } // consume '.'
                    default  ->                        s = p = null;   // no triple (yet)
                }
            }

            Plan plan = switch (operands.size()) {
                case 0  -> throw ex("Non-empty GroupGraphPattern", pos);
                case 1  -> operands.get(0);
                case 2  -> new Join(null, operands.get(0), operands.get(1));
                default -> new Join(null, operands.toArray(Plan[]::new));
            };
            plan = optionals == null ? plan : withOptionals(plan);
            plan = minus     == null ? plan : withMinus(plan);
            plan = filters   == null ? plan : withFilters(plan);
            plan = values    == null ? plan : FS.join(values, plan);
            return plan;
        }

        @RequiresNonNull("optionals")
        private Plan withOptionals(Plan left) {
            for (Plan right : optionals)
                left = FS.leftJoin(left, right);
            return left;
        }

        @RequiresNonNull("minus")
        private Plan withMinus(Plan left) {
            for (Plan right : minus)
                left = FS.minus(left, right);
            return left;
        }

        @RequiresNonNull("filters")
        private Plan withFilters(Plan left) {
            List<Expr.Exists> existsList = null;
            List<Expr> filtersList = null;
            for (int i = 0; i < filters.size(); i++) {
                Expr e = filters.get(i);
                if (e instanceof Expr.Exists exists) {
                    if (existsList == null) {
                        existsList = new ArrayList<>(filters.size());
                        filtersList = new ArrayList<>(filters.size());
                        for (int j = 0; j < i; j++)
                            filtersList.add(filters.get(i));
                    }
                    existsList.add(exists);
                } else if (filtersList != null) {
                    filtersList.add(e);
                }
            }
            if (filtersList == null)
                filtersList = filters;
            if (!filtersList.isEmpty())
                left = FS.filter(left, filtersList);
            if (existsList != null) {
                for (Expr.Exists(var filter, var negate) : existsList)
                        left = FS.exists(left, negate, filter);
            }
            return left;
        }

        private static final byte[] OPTIONAL_u8 = "OPTIONAL".getBytes(UTF_8);
        private static final byte[] FILTER_u8 = "FILTER".getBytes(UTF_8);
        private static final byte[] MINUS_u8 = "MINUS".getBytes(UTF_8);
        private static final byte[] VALUES_u8 = "VALUES".getBytes(UTF_8);
        private static final int[] GROUP_FOLLOW = alphabet("{", WS);
        private static final int[] VALUES_FOLLOW = alphabet("({", WS);
        private static final int[] FILTER_FOLLOW = alphabet("(", WS);
        private boolean readModifiers() {
            int startPos = pos;
            for (boolean has = true; has; ) {
                switch (skipWS()) {
                    case 'o', 'O' -> {
                        has = poll(OPTIONAL_u8, GROUP_FOLLOW);
                        if (has) {
                            if (optionals == null)
                                optionals = new ArrayList<>();
                            optionals.add(new GroupParser().read());
                        }
                    }
                    case 'f', 'F' -> {
                        has = poll(FILTER_u8, FILTER_FOLLOW);
                        if (has) {
                            if (filters == null)
                                filters = new ArrayList<>();
                            filters.add(exprParser.parse(pos));
                            pos = exprParser.consumedPos();
                        }
                    }
                    case 'm', 'M' -> {
                        has = poll(MINUS_u8, GROUP_FOLLOW);
                        if (has) {
                            if (minus == null)
                                minus = new ArrayList<>();
                            minus.add(new GroupParser().read());
                        }
                    }
                    case 'v', 'V' -> {
                        has = poll(VALUES_u8, VALUES_FOLLOW);
                        if (has)
                            mergeValues(readValues());
                    }
                    default -> has = false; // found nothing, exit this loop
                }
            }
            return pos != startPos;
        }

        private Values readValues() {
            require('(');
            var vars = new Vars.Mutable(10);
            for (byte c = 0; pos != end && (c = in.get(pos)) == '?' || c == '$'; c = skipWS())
                vars.add(pVarName());
            require(')');
            int n = vars.size();
            require('{');
            var batch = TERM.create(8, n, 0);
            while (poll('(')) {
                batch.beginPut();
                for (int c = 0; c < n; c++)
                    batch.putTerm(c, pTerm());
                batch.commitPut();
                require(')');
            }
            require('}');
            return new Values(vars, batch);
        }

        private void mergeValues(Values v) {
            if (this.values == null)
                this.values = v;
            else if (values.publicVars().equals(v.publicVars()))
                this.values.values().put(v.values());
            else
                coldMergeValues(v);
        }

        private void coldMergeValues(Values v) {
            Vars currentVars = this.values.publicVars();
            Vars union = currentVars.union(v.publicVars());
            TermBatch current = this.values.values();
            var p = TERM.projector(union, currentVars);
            current = p == null ? current : p.projectInPlace(current);
            p = TERM.projector(union, v.publicVars());
            if (p == null) current.put(v.values());
            else           p.project(current, v.values());
            this.values = new Values(union, current);
        }

        private static final byte[] UNION_u8 = "UNION".getBytes(UTF_8);
        private Plan readBlock() {
            ++pos; // consume '{'
            Plan plan = new GroupParser().read();
            require('}');
            if (poll(UNION_u8, GROUP_FOLLOW))
                return FS.union(plan, new GroupParser().read());
            poll('.');
            return plan;
        }
    }
}
