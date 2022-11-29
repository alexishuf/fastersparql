package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.util.Skip;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.client.util.Skip.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.VARNAME;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.VAR_MARK;
import static java.util.Arrays.copyOf;

public class OpaqueSparqlQuery implements SparqlQuery {
    public final String sparql;
    public final boolean isGraph;
    public final Vars publicVars;
    public final Vars allVars;
    public final Vars aliasVars;
    private final int verbBegin;
    private final int verbEnd;
    final int[] varPos; // visible for testing

    private OpaqueSparqlQuery(String sparql, boolean isGraph, Vars publicVars, Vars allVars,
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

    public OpaqueSparqlQuery(String sparql) {
        this.sparql = sparql;
        var s = new Scan(sparql);
        this.isGraph = s.isGraph;
        this.publicVars = s.publicVars;
        this.allVars = s.allVars;
        this.varPos = s.varPositions;
        this.verbBegin = s.verbBegin;
        this.verbEnd = s.verbEnd;
        this.aliasVars = s.aliasVars;
    }

    @Override public String      sparql() { return sparql; }
    @Override public boolean    isGraph() { return isGraph; }
    @Override public Vars    publicVars() { return publicVars; }
    @Override public Vars       allVars() { return allVars; }

    private static final long[] WS_WILDCARD = alphabet("*").whitespace().control().get();

    @Override public OpaqueSparqlQuery toAsk() {
        if (publicVars.isEmpty()) return this; // no-op
        Binder b = new Binder(Vars.EMPTY, Vars.EMPTY, new Vars.Mutable(allVars.size()));
        b.replaceWithAsk();
        for (; b.posIdx < varPos.length; b.posIdx += 2) {
            int begin = varPos[b.posIdx], end = varPos[b.posIdx+1];
            b.nAllVars.add(sparql.substring(begin+1, end));
            b.nVarPos[b.nVarPosSize++] = begin + b.growth;
            b.nVarPos[b.nVarPosSize++] = end + b.growth;
        }
        return b.build();
    }

    @Override public OpaqueSparqlQuery toDistinct(DistinctType distinctType) {
        int len = sparql.length();
        if (isGraph || verbBegin >= len) return this; // does not apply
        char c = sparql.charAt(verbBegin);
        if (c != 's' && c != 'S') return this; // not a select
        int i = skip(sparql, skip(sparql, verbBegin, len, UNTIL_WS), len, WS);
        if (sparql.regionMatches(true, i, "distinct", 0, 8)
            || sparql.regionMatches(true, i, "reduced", 0, 7)) {
            return this; // already distinct
        }
        Binder b = new Binder(publicVars, aliasVars, allVars);
        b.b.append(sparql, 0, b.consumed = verbBegin+6);
        b.b.append(' ').append(distinctType.sparql());
        b.growth = b.b.length()-b.consumed;
        for (; b.posIdx < varPos.length; b.posIdx += 2) {
            int begin = varPos[b.posIdx], end = varPos[b.posIdx+1];
            b.nVarPos[b.nVarPosSize++] = begin + b.growth;
            b.nVarPos[b.nVarPosSize++] = end + b.growth;
        }
        return b.build();
    }

    @Override public OpaqueSparqlQuery bind(Binding binding) {
        Binder b = new Binder(binding);
        if (b.nAllVars == allVars)
            return this; // no-op
        if (b.nPublicVars.isEmpty() && !publicVars.isEmpty())
            b.replaceWithAsk();
        for (; b.posIdx < varPos.length; b.posIdx += 2) {
            int vBegin = varPos[b.posIdx], vEnd = varPos[b.posIdx + 1], gap = vEnd-vBegin;
            // if vBegin points to '(', gap spans the whole "( ... AS ?name)" segment
            String name = sparql.charAt(vBegin) == '('
                    ? aliasVar(sparql, vEnd)
                    : sparql.substring(vBegin+1, vEnd);
            String nt = binding.get(name);
            if (nt != null) {
                b.b.append(sparql, b.consumed, vBegin);
                b.consumed = vEnd;
                if (vBegin < verbEnd && publicVars.contains(name)) { //erase ?name/(... AS ?name)
                    b.growth -= gap;
                    b.nVerbEnd -= gap;
                } else { // replace ?name with nt
                    b.b.append(nt);
                    b.growth += nt.length() - gap;
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
    @Override public String toString() { return sparql; }

    /* --- --- --- implementation details --- --- --- */

    private static String aliasVar(String sparql, int end) {
        int nameBegin = reverseSkip(sparql, 0, end, VAR_MARK)+1;
        int nameEnd = skip(sparql, nameBegin, end, VARNAME);
        return sparql.substring(nameBegin, nameEnd);
    }

    private final class Binder {
        final Vars nAllVars;
        Vars nPublicVars;
        Vars nAliasVars;
        StringBuilder b;
        int [] nVarPos;
        int consumed, posIdx, nVarPosSize, growth, nVerbEnd = verbEnd;

        private Binder(Vars pub, Vars alias, Vars all) {
            nPublicVars = pub;
            nAliasVars = alias;
            nAllVars = all;
            b = new StringBuilder(sparql.length());
            nVarPos = new int[varPos.length];
        }

        private Binder(Binding binding) {
            nAllVars = allVars.minus(binding.vars);
            if (nAllVars == allVars)
                return; // no-op
            nPublicVars = publicVars.minus(binding.vars);
            nAliasVars =  aliasVars.minus(binding.vars);
            b = new StringBuilder(sparql.length() + (binding.size() << 7));
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
                consumed = skip(sparql, verbBegin + 6, verbEnd - 1, WS_WILDCARD);
                b.append(' ');
            }
            growth = b.length()-consumed;       // account for decrease in size
            nVerbEnd = verbEnd +growth;         // adjust verbEnd for replaced SELECT
        }

        OpaqueSparqlQuery build() {
            // copy last stretch of sparql that has no vars in it
            var nQuery = b.append(sparql, consumed, sparql.length()).toString();
            // nVarPosSize <= nVarsPos.length since a no-op bind would've returned earlier
            nVarPos = copyOf(nVarPos, nVarPosSize);
            return new OpaqueSparqlQuery(nQuery, isGraph, nPublicVars, nAllVars, nAliasVars,
                    nVarPos, verbBegin, nVerbEnd);
        }
    }

    private static final class Scan {
        final String in;
        int pos;
        final int len;
        boolean isGraph;
        Vars publicVars, allVars, aliasVars = Vars.EMPTY;
        int[] varPositions;
        int nVarPositions, verbBegin, verbEnd;
        TermParser termParser = new TermParser();

        public Scan(String query) {
            len = (in = query).length();
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

        private static final long[] PROLOGUE = alphabet("PBpb#").get();
        private void findQueryVerb() {
            while (pos < len) {
                pos = skip(in, pos, len, WS);
                char c = in.charAt(pos);
                // on PREFIX, BASE and #, consume whole line
                if (c < 128 && (PROLOGUE[(c>>6)&1] & (1L << c)) != 0)
                    pos = skipUntil(in, pos, len, '\n');
                else break; // else we reached the query verb
            }
            if (pos >= len)
                throw new InvalidSparqlException("No SELECT/ASK/CONSTRUCT/DESCRIBE. sparql="+in);
            verbBegin = pos;
            String e = switch (in.charAt(pos)) {
                case 's', 'S' -> { aliasVars = new Vars.Mutable(10); yield "SELECT"; }
                case 'a', 'A' -> { publicVars = Vars.EMPTY; verbEnd = pos; yield "ASK"; }
                case 'c', 'C' -> { isGraph = true; yield "CONSTRUCT"; }
                case 'd', 'D' -> { isGraph = true; yield "DESCRIBE"; }
                default -> null;
            };
            if (e == null || !in.regionMatches(true, pos, e, 0, e.length())) {
                String actual = in.substring(pos, skip(in, pos, len, UNTIL_WS));
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
            pos = skip(in, ++pos, len, VARNAME);
            String name = in.substring(start+1, pos);
            allVars.add(name);
            if (aliasStart != -1) {
                aliasVars.add(name.intern());
                pos = skipUntil(in, pos, len, ')')+1; // include ')' in varPositions
            }
            addVarPosition(start, pos);
        }

        private static final long[] QUOTED_FIRST = alphabet("<\"'").get();
        private void skipQuoted() {
            char c = in.charAt(pos);
            if ((QUOTED_FIRST[c>>6] & (1L << c)) != 0 && termParser.parse(in, pos, len))
                pos = termParser.termEnd();
            else
                ++pos;
        }

        private static final long[] STOP_PROJ = alphabet("?$#{*(<\"'").invert().get();
        private void readProjection() {// !graph && publicVars == null
            pos = skip(in, pos, len, STOP_PROJ);
            while (pos < len && verbEnd == 0) {
                switch (in.charAt(pos)) {
                    case '?', '$' -> readVar(-1);
                    case '#'      -> pos = skipUntil(in, pos, len, '\n');
                    case '*'      -> { ++pos; publicVars = allVars; }
                    case '{'      -> verbEnd = pos;
                    case '('      -> readAs();
                    default       -> skipQuoted();
                }
                pos = skip(in, pos, len, STOP_PROJ);
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

        private static final long[] STOP_AS = alphabet("#aA?$<\"'").invert().get();
        private static final long[] AFT_AS = alphabet("?$").whitespace().control().get();
        private void readAs() {
            boolean as = false;
            int aliasStart = pos;
            pos = skip(in, ++pos, len, STOP_AS);
            while (pos < len) {
                switch (in.charAt(pos)) {
                    default       -> skipQuoted();
                    case '#'      -> pos = skipUntil(in, pos, len, '\n');
                    case '?', '$' -> {
                        if (as) {
                            readVar(aliasStart);
                            return;
                        } else {
                            int s = pos;
                            addVarPosition(s, pos = skip(in, ++pos, len, VARNAME));
                        }
                    }
                    case 'a','A'  -> {
                        if (pos+2 >= len)
                            pos = len;
                        char bfr = in.charAt(pos-1), c1 = in.charAt(pos+1), c2 = in.charAt(pos+2);
                        as |= ((bfr <= ' ' || bfr == ')') && (c1=='S' || c1=='s')
                           && Skip.contains(AFT_AS, c2));
                        pos += 2;
                    }
                }
                pos = skip(in, pos, len, STOP_AS);
            }
        }

        private static final long[] STOP_VAR = alphabet("$?#<\"'").invert().get();
        private void readAllVars() {
            pos = skip(in, pos, len, STOP_VAR);
            while (pos < len) {
                switch (in.charAt(pos)) {
                    case '?', '$' -> readVar(-1);
                    case '#'      -> pos = skipUntil(in, pos, len, '\n');
                    default       -> skipQuoted();
                }
                pos = skip(in, pos, len, STOP_VAR);
            }
        }

    }
}
