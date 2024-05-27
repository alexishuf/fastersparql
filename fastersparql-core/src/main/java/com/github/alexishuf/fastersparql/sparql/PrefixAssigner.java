package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.RDF;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.XSD;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.CONSTANT;

@SuppressWarnings("StaticInitializerReferencesSubClass")
public class PrefixAssigner extends AbstractOwned<PrefixAssigner> {
    public static final PrefixAssigner NOP;
    public static final PrefixAssigner CANON;
    protected static final RopeArrayMap GARBAGE_PREFIX2NAME;
    protected static final RopeArrayMap DEFAULT_PREFIX2NAME;

    static {
        GARBAGE_PREFIX2NAME = RopeArrayMap.create().takeOwnership(CONSTANT);
        DEFAULT_PREFIX2NAME = RopeArrayMap.create().takeOwnership(CONSTANT);
        var xsd = RopeFactory.make(XSD.len-1).add(XSD, 0, XSD.len-1).take();
        var rdf = RopeFactory.make(RDF.len-1).add(RDF, 0, RDF.len-1).take();
        DEFAULT_PREFIX2NAME.put(xsd, PrefixMap.XSD_NAME);
        DEFAULT_PREFIX2NAME.put(rdf, PrefixMap.RDF_NAME);
        Concrete canon = new PrefixAssigner.Concrete(RopeArrayMap.create()) {
            @Override public String toString() {return "CANON";}
        };
        Concrete nop = new PrefixAssigner.Concrete(RopeArrayMap.create()) {
            @Override public String toString() {return "NOP";}
        };
        canon.reset();
        CANON = canon.takeOwnership(CONSTANT);
        NOP   =   nop.takeOwnership(CONSTANT);
    }

    protected RopeArrayMap prefix2name;

    public static Orphan<PrefixAssigner> create() {return new Concrete(RopeArrayMap.create());}
    public static Orphan<PrefixAssigner> create(Orphan<RopeArrayMap> map) {return new Concrete(map);}
    protected PrefixAssigner(Orphan<RopeArrayMap> map) {prefix2name = map.takeOwnership(this);}
    private static class Concrete extends PrefixAssigner implements Orphan<PrefixAssigner> {
        public Concrete(Orphan<RopeArrayMap> map) {super(map);}
        @Override public PrefixAssigner takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable PrefixAssigner recycle(Object currentOwner) {
        return internalMarkGarbage(currentOwner);
    }

    @Override protected @Nullable PrefixAssigner internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        if (prefix2name != GARBAGE_PREFIX2NAME)
            prefix2name.recycle(this);
        prefix2name = GARBAGE_PREFIX2NAME;
        return null;
    }

    public void reset() {
        if (prefix2name == GARBAGE_PREFIX2NAME)
            prefix2name = RopeArrayMap.create().takeOwnership(this);
        prefix2name.resetToCopy(DEFAULT_PREFIX2NAME);
    }

    public int size() { return prefix2name.size(); }

    public @Nullable Rope nameFor(SegmentRope prefix) {
        return prefix2name.get(prefix);
    }
}
