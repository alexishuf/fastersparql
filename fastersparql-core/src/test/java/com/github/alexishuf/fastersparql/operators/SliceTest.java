package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexishuf.fastersparql.util.Results.results;

public class SliceTest {
    static List<Results> data() {
        var base = results("?x0", "?x1",
                           "_:1", "_:b1",
                           "_:2", "_:b2",
                           "_:3", "_:b3",
                           "_:4", "_:b4",
                           "_:5", "_:b5"
        );
        return Results.resultsList(
                results().query(FS.slice(results().asPlan(), 0, 0)),
                results().query(FS.slice(results().asPlan(), 0, 1024)),
                results().query(FS.slice(results().asPlan(), 512, 1024)),
                base.query(FS.slice(base.asPlan(), 0, 1024)),
                base.query(FS.slice(base.asPlan(), 0, 5)),
                base.sub(0, 4).query(FS.slice(base.asPlan(), 0, 4)),
                base.sub(0, 3).query(FS.slice(base.asPlan(), 0, 3)),
                base.sub(0, 1).query(FS.slice(base.asPlan(), 0, 1)),
                results("?x0", "?x1").query(FS.slice(base.asPlan(), 1, 0)),
                base.sub(1, 2).query(FS.slice(base.asPlan(), 1, 1)),
                base.sub(1, 3).query(FS.slice(base.asPlan(), 1, 2)),
                base.sub(1, 5).query(FS.slice(base.asPlan(), 1, 4)),
                base.sub(1, 5).query(FS.slice(base.asPlan(), 1, 5))
        );
    }

    @Test
    void test() {
        for (Results results : data())
            results.check();
    }
}