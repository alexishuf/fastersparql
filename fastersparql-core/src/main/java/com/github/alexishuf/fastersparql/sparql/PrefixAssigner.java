package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PrefixAssigner {
    public static final PrefixAssigner NOP = new PrefixAssigner(new RopeArrayMap()) {
        @Override public String toString() { return "NOP"; }
    };
    public static final PrefixAssigner CANON;

    static {
        PrefixAssigner canon = new PrefixAssigner(new RopeArrayMap()) {
            @Override public String toString() {return "CANON";}
        };
        canon.reset();
        CANON = canon;
    }

    protected RopeArrayMap prefix2name;

    public PrefixAssigner(RopeArrayMap prefix2name) {
        this.prefix2name = prefix2name;
    }

    public void reset() {
        prefix2name.clear();
        prefix2name.put(new ByteRope(Term.XSD.toArray(0, Term.XSD.len-1)), PrefixMap.XSD_NAME);
        prefix2name.put(new ByteRope(Term.RDF.toArray(0, Term.RDF.len-1)), PrefixMap.RDF_NAME);
    }

    public @Nullable Rope nameFor(SegmentRope prefix) {
        return prefix2name.get(prefix);
    }
}
