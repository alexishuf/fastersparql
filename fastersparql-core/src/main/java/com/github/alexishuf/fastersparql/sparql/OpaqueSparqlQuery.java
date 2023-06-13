package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.Rope.Range.WS;
import static com.github.alexishuf.fastersparql.model.rope.Rope.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static java.util.Arrays.copyOf;

public class OpaqueSparqlQuery implements SparqlQuery {
    public final SegmentRope sparql;
    public final boolean isGraph;
    public final Vars publicVars;
    public final Vars allVars;
    public final Vars aliasVars;
    private final int verbBegin;
    private final int verbEnd;
    final int[] varPos; // visible for testing

    private OpaqueSparqlQuery(SegmentRope sparql, boolean isGraph, Vars publicVars, Vars allVars,
                              Vars aliasVars, int[] varPos, int verbBegin, int verbEnd) {
        this.sparql = sparql;
        this.isGraph = isGraph;
        this.publicVars = publicVars;
        this.allVars = allVars;
        this.varPos = varPos;
        this.aliasVars = aliasVars;
        this.verbBegin = verbBegin;
        this.verbEnd = verbEnd;
    }

    public OpaqueSparqlQuery(CharSequence sparql) {
        this.sparql = SegmentRope.of(sparql);
        var s = new Scan(this.sparql);
        this.isGraph = s.isGraph;
        this.publicVars = s.publicVars;
        this.allVars = s.allVars;
        this.varPos = s.varPositions;
        this.verbBegin = s.verbBegin;
        this.verbEnd = s.verbEnd;
        this.aliasVars = s.aliasVars;
    }

    @Override public boolean isAsk() {
        byte c = verbBegin < sparql.len ? sparql.get(verbBegin) : 0;
        return c == 'a' || c == 'A';
    }

    @Override public SegmentRope sparql() { return sparql; }
    @Override public boolean    isGraph() { return isGraph; }
    @Override public Vars    publicVars() { return publicVars; }
    @Override public Vars       allVars() { return allVars; }

    private static final int[] WS_WILDCARD = alphabet("*", WS);

    @Override public OpaqueSparqlQuery toAsk() {
        if (publicVars.isEmpty()) return this; // no-op
        Binder b = new Binder(Vars.EMPTY, Vars.EMPTY, new Vars.Mutable(allVars.size()));
        b.replaceWithAsk();
        for (; b.posIdx < varPos.length; b.posIdx += 2) {
            int begin = varPos[b.posIdx], end = varPos[b.posIdx+1];
            b.nAllVars.add(sparql.sub(begin+1, end));
            b.nVarPos[b.nVarPosSize++] = begin + b.growth;
            b.nVarPos[b.nVarPosSize++] = end + b.growth;
        }
        return b.build();
    }

    @Override public OpaqueSparqlQuery toDistinct(DistinctType distinctType) {
        int len = sparql.len();
        if (isGraph || verbBegin >= len) return this; // does not apply
        byte c = sparql.get(verbBegin);
        if (c != 's' && c != 'S') return this; // not a select
        int i = sparql.skip(sparql.skip(verbBegin, len, UNTIL_WS), len, Rope.WS);
        var current = sparql.hasAnyCase(i, DISTINCT_u8) ? DistinctType.STRONG
                    : sparql.hasAnyCase(i, REDUCED_u8) ? DistinctType.WEAK : null;
        if (current == distinctType || current == DistinctType.STRONG)
            return this; // distinctType already satisfied
        Binder b = new Binder(publicVars, aliasVars, allVars);
        b.b.append(sparql, 0, b.consumed = verbBegin+6);
        if (current == DistinctType.WEAK)
            b.consumed = sparql.skipWS(b.consumed, sparql.len())+7; // skip over REDUCED
        b.b.append(' ').append(distinctType.sparql());
        b.growth = b.b.len()-b.consumed;
        for (; b.posIdx < varPos.length; b.posIdx += 2) {
            int begin = varPos[b.posIdx], end = varPos[b.posIdx+1];
            b.nVarPos[b.nVarPosSize++] = begin + b.growth;
            b.nVarPos[b.nVarPosSize++] = end + b.growth;
        }
        return b.build();
    }

    @Override public OpaqueSparqlQuery bound(Binding binding) {
        if (!allVars.intersects(binding.vars))
            return this; // no-op
        Binder b = new Binder(binding);
        if (b.nPublicVars.isEmpty() && !publicVars.isEmpty())
            b.replaceWithAsk();
        var name = new SegmentRope(sparql.segment, sparql.utf8, sparql.offset, sparql.len);
        for (; b.posIdx < varPos.length; b.posIdx += 2) {
            int vBegin = varPos[b.posIdx], vEnd = varPos[b.posIdx + 1], gap = vEnd-vBegin;
            // if vBegin points to '(', gap spans the whole "( ... AS ?name)" segment
            if (sparql.get(vBegin) == '(')
                aliasVar(name, vEnd);
            else
                name.slice(sparql.offset+vBegin+1, vEnd-(vBegin+1));
            Term term = binding.get(name);
            if (term != null) {
                b.b.append(sparql, b.consumed, vBegin);
                b.consumed = vEnd;
                if (vBegin < verbEnd && publicVars.contains(name)) { //erase ?name/(... AS ?name)
                    b.growth -= gap;
                    b.nVerbEnd -= gap;
                } else { // replace ?name with term
                    term.toSparql(b.b, PrefixAssigner.NOP);
                    b.growth += term.len() - gap;
                }
            } else { // adjust varPos for ?name in the bound sparql
                int nBegin = vBegin+ b.growth;
                b.nVarPos[b.nVarPosSize++] = nBegin;
                b.nVarPos[b.nVarPosSize++] = nBegin+gap;
            }
        }
        return b.build();
    }

    @Override public boolean equals(Object obj) {
        return obj == this || obj instanceof OpaqueSparqlQuery q && q.sparql.equals(sparql);
    }
    @Override public int    hashCode() { return sparql.hashCode(); }
    @Override public String toString() { return sparql.toString(); }

    /* --- --- --- implementation details --- --- --- */

    private void aliasVar(SegmentRope name, int end) {
        int nameBegin = sparql.reverseSkip(0, end, VAR_MARK)+1;
        int nameEnd = sparql.skip(nameBegin, end, VARNAME);
        name.slice(sparql.offset+nameBegin, nameEnd-nameBegin);
    }

    private final class Binder {
        final Vars nAllVars;
        Vars nPublicVars;
        Vars nAliasVars;
        ByteRope b;
        int [] nVarPos;
        int consumed, posIdx, nVarPosSize, growth, nVerbEnd = verbEnd;

        private Binder(Vars pub, Vars alias, Vars all) {
            nPublicVars = pub;
            nAliasVars = alias;
            nAllVars = all;
            b = new ByteRope(sparql.len());
            nVarPos = new int[varPos.length];
        }

        private Binder(Binding binding) {
            nAllVars = allVars.minus(binding.vars);
            if (nAllVars == allVars)
                return; // no-op
            nPublicVars = publicVars.minus(binding.vars);
            nAliasVars =  aliasVars.minus(binding.vars);
            b = new ByteRope(sparql.len() + (binding.size() << 7));
            nVarPos = new int[varPos.length];
        }

        void replaceWithAsk() {
            b.append(sparql, 0, consumed = verbBegin); // copy prologue
            b.append("ASK");                     // add ASK
            // find end of last "?var" or "?alas)" and mark that point as consumed
            for (; posIdx < varPos.length && varPos[posIdx+1] <= verbEnd; posIdx += 2)
                consumed = varPos[posIdx+1];
            // if there was no varPos before {, this is a SELECT *
            if (consumed == verbBegin) {
                consumed = sparql.skip(verbBegin + 6, verbEnd - 1, WS_WILDCARD);
                b.append(' ');
            }
            growth = b.len()-consumed;       // account for decrease in size
            nVerbEnd = verbEnd +growth;         // adjust verbEnd for replaced SELECT
        }

        OpaqueSparqlQuery build() {
            // copy last stretch of sparql that has no vars in it
            var nQuery = b.append(sparql, consumed, sparql.len());
            // nVarPosSize <= nVarsPos.length since a no-op bind would've returned earlier
            nVarPos = copyOf(nVarPos, nVarPosSize);
            return new OpaqueSparqlQuery(nQuery, isGraph, nPublicVars, nAllVars, nAliasVars,
                                         nVarPos, verbBegin, nVerbEnd);
        }
    }

    private static final class Scan {
        final SegmentRope in;
        int pos;
        final int len;
        boolean isGraph;
        Vars publicVars, allVars, aliasVars = Vars.EMPTY;
        int[] varPositions;
        int nVarPositions, verbBegin, verbEnd;
        TermParser termParser = new TermParser();

        public Scan(SegmentRope query) {
            len = (in = query).len();
            findQueryVerb();
            if (isGraph) {
                publicVars = allVars = Vars.EMPTY;
                varPositions = new int[0];
            } else {
                varPositions = new int[22];
                allVars = new Vars.Mutable(10);
                if (publicVars == null)
                    readProjection();
                readAllVars();
                if (allVars.size() == publicVars.size())
                    allVars = publicVars; // release memory
                if (aliasVars.size() == 0)
                    aliasVars = Vars.EMPTY; // release memory
                if (varPositions.length > nVarPositions)
                    varPositions = Arrays.copyOf(varPositions, nVarPositions);
            }
        }

        private static final int[] PROLOGUE = alphabet("PBpb#");
        private void findQueryVerb() {
            while (pos < len) {
                pos = in.skip(pos, len, Rope.WS);
                byte c = in.get(pos);
                // on PREFIX, BASE and #, consume whole line
                if (c > 0 && (PROLOGUE[c>>5] & (1<<c)) != 0)
                    pos = in.skipUntil(pos, len, '\n');
                else break; // else we reached the query verb
            }
            if (pos >= len)
                throw new InvalidSparqlException("No SELECT/ASK/CONSTRUCT/DESCRIBE. sparql="+in);
            verbBegin = pos;
            byte[] ex = switch (in.get(pos)) {
                case 's', 'S' -> { aliasVars = new Vars.Mutable(10); yield SELECT_u8; }
                case 'a', 'A' -> { publicVars = Vars.EMPTY; verbEnd = pos; yield ASK_u8; }
                case 'c', 'C' -> { isGraph = true; yield CONSTRUCT_u8; }
                case 'd', 'D' -> { isGraph = true; yield DESCRIBE_u8; }
                default -> null;
            };
            if (ex == null || !in.hasAnyCase(pos, ex)) {
                Rope actual = in.sub(pos, in.skip(pos, len, UNTIL_WS));
                throw new InvalidSparqlException("Expected SELECT/ASK/CONSTRUCT/DESCRIBE, found " + actual + " in sparql="+in);
            }
        }

        private void addVarPosition(int begin, int end) {
            int capacity = varPositions.length;
            if (nVarPositions == capacity) // grow by 100%
                varPositions = Arrays.copyOf(varPositions, capacity<<1);
            varPositions[nVarPositions++] = begin;
            varPositions[nVarPositions++] = end;
        }

        private void readVar(int aliasStart) { //in.charAt(pos) is '?' or '$'
            int start = pos;
            pos = in.skip(start+1, len, VARNAME);
            SegmentRope name = in.sub(start+1, pos);
            allVars.add(name);
            if (aliasStart != -1) {
                aliasVars.add(name);
                pos = in.skipUntil(pos, len, ')')+1;  // include ')' in varPositions
            }
            addVarPosition(start, pos);
        }

        private void skipQuoted() {
            byte c = in.get(pos);
            if ((c == '<' || c == '"' || c == '\'') && termParser.parse(in, pos, len).isValid())
                pos = termParser.termEnd();
            else
                ++pos;
        }

        private static final int[] STOP_PROJ = invert(alphabet("?$#{*(<\"'"));
        private void readProjection() {// !graph && publicVars == null
            while ((pos = in.skip(pos, len, STOP_PROJ)) < len && verbEnd == 0) {
                switch (in.get(pos)) {
                    case '?', '$' -> readVar(-1);
                    case '#'      -> pos = in.skipUntil(pos, len, '\n');
                    case '*'      -> { ++pos; publicVars = allVars; }
                    case '{'      -> verbEnd = pos;
                    case '('      -> readAs();
                    default       -> skipQuoted();
                }
            }
            int n = allVars.size();
            if (n > 0) {
                publicVars = allVars;
                // allVars includes all publicVars in the same order
                allVars = Vars.from(publicVars, Math.max(10, n + (n >> 1)));
            } else if (publicVars == null) {
                publicVars = Vars.EMPTY; // no ?var and no *
            }
        }

        private static final int[] STOP_AS = invert(alphabet("#aA?$<\"'"));
        private static final int[] AFT_AS = alphabet("?$", WS);
        private void readAs() {
            boolean as = false;
            int aliasStart = pos;
            pos = in.skip(++pos, len, STOP_AS);
            while (pos < len) {
                switch (in.get(pos)) {
                    default       -> skipQuoted();
                    case '#'      -> pos = in.skipUntil(pos, len, '\n');
                    case '?', '$' -> {
                        if (as) {
                            readVar(aliasStart);
                            return;
                        } else {
                            int s = pos;
                            addVarPosition(s, pos = in.skip(++pos, len, VARNAME));
                        }
                    }
                    case 'a','A'  -> {
                        if (pos+2 >= len)
                            pos = len;
                        byte bfr = in.get(pos-1), c1 = in.get(pos+1), c2 = in.get(pos+2);
                        as |= (bfr <= ' ' || bfr == ')')
                           && (c1=='S' || c1=='s')
                           && Rope.contains(AFT_AS, c2);
                        pos += 2;
                    }
                }
                pos = in.skip(pos, len, STOP_AS);
            }
        }

        private static final int[] STOP_VAR = invert(alphabet("$?#<\"'"));
        private void readAllVars() {
            while ((pos = in.skip(pos, len, STOP_VAR)) < len) {
                switch (in.get(pos)) {
                    case '?', '$' -> readVar(-1);
                    case '#'      -> pos = in.skipUntil(pos, len, '\n');
                    default       -> skipQuoted();
                }
            }
        }

    }
}
