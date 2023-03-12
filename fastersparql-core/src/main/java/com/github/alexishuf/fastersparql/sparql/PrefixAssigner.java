package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PrefixAssigner {
    public static final PrefixAssigner NOP = new PrefixAssigner(new RopeArrayMap()) {
        @Override public String toString() { return "NOP"; }
    };
    public static final PrefixAssigner CANON;

    static {
        RopeArrayMap map = new RopeArrayMap();
        map.put(Term.XSD.sub(0, Term.XSD.len()-1), PrefixMap.XSD_NAME);
        map.put(Term.RDF.sub(0, Term.RDF.len()-1), PrefixMap.RDF_NAME);
        CANON = new PrefixAssigner(map) {
            @Override public String toString() { return "CANON"; }
        };
    }

    protected RopeArrayMap prefix2name;

    public PrefixAssigner(RopeArrayMap prefix2name) {
        this.prefix2name = prefix2name;
    }

    public @Nullable Rope nameFor(Rope prefix) {
        return prefix2name.get(prefix);
    }
}
