package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

public interface NTVisitor {
    /** Visits a string that contains an RDF term in N-Triples syntax. */
    void visit(SegmentRope string);
}
