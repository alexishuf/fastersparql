package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.operators.plan.Values;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprParser;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.model.rope.Rope.*;
import static com.github.alexishuf.fastersparql.model.rope.Rope.Range.WS;
import static com.github.alexishuf.fastersparql.sparql.DistinctType.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public sealed abstract class SparqlParser extends AbstractOwned<SparqlParser> {
    private static final int BYTES = 16 + 8+4 + 8 + ExprParser.BYTES + GroupParser.BYTES;
    private static final Supplier<SparqlParser> FAC = new Supplier<>() {
        @Override public SparqlParser get() {return new Concrete().takeOwnership(RECYCLED);}
        @Override public String toString() {return "SparqlParser.FAC";}
    };
    private static final Alloc<SparqlParser> ALLOC = new Alloc<>(SparqlParser.class,
            "SparqlParser.ALLOC", Alloc.THREADS*32, FAC, BYTES);
    static { Primer.INSTANCE.sched(ALLOC::prime); }

    private SegmentRope in;
    private int start, pos, end;
    private final ExprParser exprParser = ExprParser.create().takeOwnership(this);
    private final GroupParser groupParser = new GroupParser();
    private DistinctType distinct;
    private long limit;
    private @Nullable Vars projection;
    private final PrivateRopeFactory ropeFactory = new PrivateRopeFactory(64);

    private SparqlParser() {}

    public static Orphan<SparqlParser> create() {
        return ALLOC.create().releaseOwnership(RECYCLED);
    }

    private static final class Concrete extends SparqlParser implements Orphan<SparqlParser> {
        @Override public SparqlParser takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable SparqlParser recycle(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        if (ALLOC.offer(this) != null)
            internalMarkGarbage(RECYCLED);
        return null;
    }

    @Override protected @Nullable SparqlParser internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        exprParser.recycle(this);
        return null;
    }

    public static Plan parse(SparqlQuery q) {
        if (q instanceof Plan p)
            return p;
        var parser = create().takeOwnership(PARSE);
        try {
            return parser.parse(q.sparql(), 0);
        } finally {
            parser.recycle(PARSE);
        }
    }
    private static final StaticMethodOwner PARSE = new StaticMethodOwner("SparqlParser.parse");

    public static Plan parse(SegmentRope rope) {
        var parser = create().takeOwnership(PARSE);
        try {
            return parser.parse(rope, 0);
        } finally {
            parser.recycle(PARSE);
        }
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
    public Plan parseGroup(SegmentRope sparql, int start, PrefixMap prefixMap) {
        requireAlive();
        this.end = (this.in = sparql).len();
        this.pos = start;
        exprParser.termParser.prefixMap().resetToCopy(prefixMap);
        exprParser.input(sparql);
        require('{');
        Plan plan = new GroupParser().read();
        require('}');
        return plan;

    }
    public Plan parse(SegmentRope query, int start) {
        requireAlive();
        end = (in = query).len();
        this.start = start;
        pos = start;
        exprParser.termParser.prefixMap().resetToBuiltin();
        groupParser.reset();
        distinct = null;
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

    private InvalidSparqlException ex(String expected, int where) {
        int acBegin = in.skipWS(where, end), acEnd = in.skip(acBegin, end, UNTIL_WS);
        String actual = acBegin >= end ? "EOF" : "\""+in.sub(acBegin, acEnd)+"\"";
        return new InvalidSparqlException("Expected "+expected+" at position "+(where-start)+", got "+actual+" Full query: "+in.sub(start, end));
    }
    private InvalidSparqlException ex(byte[] expected, int where) {
        return ex(new String(expected, UTF_8), where);
    }

    private Term pTerm() {
        skipWS();
        var termParser = exprParser.termParser;
        if (!termParser.parse(in, pos, end).isValid())
            throw ex("RDF term/var", pos);
        pos = termParser.termEnd();
        return termParser.asTerm();
    }

    private FinalSegmentRope pVarName() {
        int begin = pos+1, e = in.skip(begin, end, VARNAME);
        while (e > begin && in.get(e) == '.') --e;
        if (e == begin)
            throw ex("non-empty var name", pos);
        pos = e;
        return ropeFactory.asFinal(in, begin, e);
    }

    private Term pIri() {
        skipWS();
        int begin = pos;
        if (begin < end && in.get(begin) == '<') {
            pos = in.requireThenSkipUntil(begin+1, end, Rope.LETTERS, (byte)'>');
            if (pos > begin+1 && pos < end) return Term.valueOf(in, begin, ++pos);
        }
        throw ex("IRI", pos = begin);
    }

    private long pLong() {
        try {
            long val = in.parseLong(pos);
            pos = in.skip(pos+1, end, DIGITS);
            return val;
        } catch (NumberFormatException e) {
            throw ex("integer", pos);
        }
    }

    private byte skipWS() {
        while ((pos = in.skipWS(pos, end)) != end && in.get(pos) == '#')
            pos = in.skipUntil(pos, end, (byte)'\n');
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

    private static final FinalSegmentRope BASE = asFinal(":BASE");

    private void pPrologue() {
        PrefixMap prefixMap = exprParser.termParser.prefixMap();
        while (true) {
            switch (skipWS()) {
                case 'p', 'P' -> {
                    require(PREFIX_u8);
                    prefixMap.addRef(pPNameNS(), pIri());
                    poll('.'); // tolerate TTL '.' that should not be here
                }
                case 'b', 'B' -> {
                    require(BASE_u8);
                    prefixMap.addRef(BASE, pIri());
                    poll('.'); // tolerate TTL '.' that should not be here
                }
                default -> { return; }
            }
        }
    }

    private FinalSegmentRope pPNameNS() {
        if (skipWS() == '\0')
            throw ex("PNAME_NS", pos);
        int begin = pos;
        pos = in.skip(pos, end, PN_PREFIX);
        var name = ropeFactory.asFinal(in, begin, pos);
        require(':');
        return name;
    }

    private void pVerb() {
        switch (skipWS()) {
            case 'a', 'A' -> {
                require(ASK_u8);
                limit = 1;
                projection = Vars.EMPTY;
                distinct = null;
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
                case 'd', 'D'            -> { require(DISTINCT_u8); distinct = STRONG; }
                case 'r', 'R'            -> { require(REDUCED_u8);  distinct = REDUCED; }
                case 'p', 'P'            -> { require(PRUNED_u8);   distinct = WEAK; }
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

        if (limit != Long.MAX_VALUE || offset > 0 || projection != null || distinct != null)
            return FS.modifiers(where, projection, distinct, offset, limit, List.of());
        return where;
    }

    private class GroupParser {
        private static final int BYTES = 16 + 4*4 + 16+8;
        private final List<Plan> operands = new ArrayList<>();
        private List<Plan> optionals, minus;
        private Values values;
        private List<Expr> filters;
        private BitSet undefValuesCols;
        private Vars undefValuesVars;

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
            if (undefValuesVars != null && !plan.publicVars().containsAll(undefValuesVars))
                plan = FS.project(plan, plan.publicVars().union(undefValuesVars));
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
            List<Expr.Exists> exists = null;
            List<Expr> exprs = null;
            int n = filters.size();
            for (Expr e : filters) {
                if (e instanceof Expr.Exists ex)
                    (exists == null ? exists = new ArrayList<>(n) : exists).add(ex);
                else
                    (exprs  == null ? exprs  = new ArrayList<>(n) : exprs ).add(e);
            }
            if (exprs != null)
                left = FS.filter(left, exprs);
            if (exists != null) {
                for (var e : exists)
                        left = FS.exists(left, e.negate(), e.filter());
            }
            return left;
        }

        private static final byte[] OPTIONAL_u8 = "OPTIONAL".getBytes(UTF_8);
        private static final byte[] FILTER_u8 = "FILTER".getBytes(UTF_8);
        private static final byte[] MINUS_u8 = "MINUS".getBytes(UTF_8);
        private static final byte[] VALUES_u8 = "VALUES".getBytes(UTF_8);
        private static final byte[] UNDEF_u8 = "UNDEF".getBytes(UTF_8);
        private static final int[] GROUP_FOLLOW = alphabet("{", WS);
        private static final int[] VALUES_FOLLOW = alphabet("({", WS);
        private static final int[] FILTER_FOLLOW = alphabet("(", WS);
        private static final int[] UNDEF_FOLLOW = alphabet(")", WS);
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
            BitSet undefValuesCols = this.undefValuesCols;
            if (undefValuesCols == null)
                this.undefValuesCols = undefValuesCols = new BitSet(n);
            undefValuesCols.set(0, n);
            var batch = TERM.create(n).takeOwnership(this);
            try {
                while (poll('(')) {
                    batch.beginPut();
                    for (int c = 0; c < n; c++) {
                        skipWS();
                        if (!poll(UNDEF_u8, UNDEF_FOLLOW)) {
                            undefValuesCols.clear(c);
                            batch.putTerm(c, pTerm());
                        }
                    }
                    batch.commitPut();
                    require(')');
                }
                require('}');
            } catch (Throwable t) {
                Owned.safeRecycle(batch, this);
                throw t;
            }
            Orphan<TermBatch> batchOrphan;
            if (undefValuesCols.isEmpty()) {
                batchOrphan = batch.releaseOwnership(this);
            } else {
                // FedX sets VALUES clauses with columns of UNDEFINED values when it has
                // the intent of not binding that column (even listing the var in the outermost
                // projection)
                Vars undefValuesVars = this.undefValuesVars;
                if (undefValuesVars == null)
                    this.undefValuesVars = undefValuesVars = new Vars.Mutable(n);
                Vars origVars = vars;
                vars = new Vars.Mutable(n);
                for (int c = 0; c < n; c++) {
                    var name = origVars.get(c);
                    if  (undefValuesCols.get(c)) undefValuesVars.add(name);
                    else                         vars.add(name);
                }
                var p = requireNonNull(TERM.projector(vars, origVars)).takeOwnership(this);
                try {
                    batchOrphan = p.projectInPlace(batch.releaseOwnership(this));
                } finally {
                    Owned.safeRecycle(p, this);
                }
            }
            return new Values(vars, batchOrphan);
        }

        private void mergeValues(Values v) {
            if (this.values == null)
                this.values = v;
            else if (values.publicVars().equals(v.publicVars()))
                this.values.append(v.values());
            else
                coldMergeValues(v);
        }

        private void coldMergeValues(Values v) {
            TermBatch current = Orphan.takeOwnership(this.values.takeValues(), this);
            Vars currentVars  = this.values.publicVars();
            Vars union        = currentVars.union(v.publicVars());
            var pOrphan = TERM.projector(union, currentVars);
            var p = pOrphan == null ? null : pOrphan.takeOwnership(this);
            var add = v.values();
            if (p == null) {
                if (current == null)
                    current = TERM.create(union.size()).takeOwnership(this);
                current.copy(add);
            } else {
                var dst = Owned.releaseOwnership(current, this);
                current = p.project(dst, add).takeOwnership(this);
                p.recycle(this);
            }
            this.values = new Values(union, current.releaseOwnership(this));
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
