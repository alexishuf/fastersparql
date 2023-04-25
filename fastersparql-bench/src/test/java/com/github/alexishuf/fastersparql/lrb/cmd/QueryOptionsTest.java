package com.github.alexishuf.fastersparql.lrb.cmd;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexishuf.fastersparql.lrb.query.QueryName.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryOptionsTest {
    @Test void test() {
        QueryOptions o = new QueryOptions(List.of("S(2|3)", "S1.*"));
        assertEquals(List.of(S1, S2, S3, S10, S11, S12, S13, S14), o.queries());
    }
}