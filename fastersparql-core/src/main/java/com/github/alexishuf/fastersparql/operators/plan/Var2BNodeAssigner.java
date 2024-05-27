package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledSegmentRopeView;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;

public class Var2BNodeAssigner implements SafeCloseable {
    private RopeArrayMap name2nt = RopeArrayMap.create().takeOwnership(this);

    @Override public void close() {
        name2nt = Owned.safeRecycle(name2nt, this);
    }

    public FinalSegmentRope ntForName(MemorySegment segment, byte @Nullable[] u8,
                                      long offset, int len) {
        try (var view = PooledSegmentRopeView.of(segment, u8, offset, len)) {
            return ntForName(view);
        }
    }
    public FinalSegmentRope ntForName(SegmentRope varName) {
        var nt = (FinalSegmentRope)name2nt.get(varName);
        if (nt == null) {
            int id = name2nt.size();
            nt = id < BNODES.length ? BNODES[id] : makeBNodeNT(id);
            name2nt.put(FinalSegmentRope.asFinal(varName), nt);
        }
        return nt;
    }

    private static final FinalSegmentRope[] BNODES;
    private static final FinalSegmentRope BNODE_PREFIX = FinalSegmentRope.asFinal("_:b");
    private static final int MAX_BNODE_LEN = BNODE_PREFIX.len
                                           + String.valueOf(Integer.MAX_VALUE).length();
    static {
        var nts = new FinalSegmentRope[1000];
        for (int i = 0; i < nts.length; i++)
            nts[i] = makeBNodeNT(i);
        BNODES = nts;
    }
    private static FinalSegmentRope makeBNodeNT(int i) {
        return RopeFactory.make(MAX_BNODE_LEN).add(BNODE_PREFIX).add(i).take();
    }
}
