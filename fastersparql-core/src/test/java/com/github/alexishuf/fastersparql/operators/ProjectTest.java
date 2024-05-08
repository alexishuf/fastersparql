package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.Results;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.Results.results;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ProjectTest {

    static Stream<Arguments> test() {
        var base = results("?x0", "?x1", "?x2",
                           "_:b00", "_:b01", "_:b02",
                           "_:b10", "_:b11", "_:b12"
        );
        return Stream.of(
        /*  1 */arguments(results(), results(Vars.of("x"))),
        /*  2 */arguments(results(), results("?x", "?y")),
        /*  3 */arguments(results("?x0", "<a>"),
                          results("?x0", "<a>")),
        /*  4 */arguments(base.sub(0, 1),
                          results("?x0", "_:b00")),
        /*  5 */arguments(base.sub(0, 1),
                          results("?x2", "_:b02")),
        /*  6 */arguments(base.sub(0, 1),
                          results("?x0", "?x1", "_:b00", "_:b01")),
        /*  7 */arguments(base.sub(0, 1),
                          results("?x0", "?y", "_:b00", null)),
        /*  8 */arguments(base.sub(0, 1),
                          results("?y", "?x2", null, "_:b02")),
        /*  9 */arguments(base.sub(0, 1),
                        results("?x1", "?y", "?x2", "_:b01", null, "_:b02")),
        /* 10 */arguments(base.sub(0, 2),
                          results("?x0", "_:b00", "_:b10")),
        /* 11 */arguments(base.sub(0, 2),
                          results("?x1",   "?y", "?x0",
                                  "_:b01", null, "_:b00",
                                  "_:b11", null, "_:b10"))
        );
    }

    @ParameterizedTest @MethodSource
    void test(Results in, Results expected) {
        expected.check(FS.project(in.asPlan(), expected.vars()).execute(TermBatchType.TERM));
        ThreadJournal.resetJournals();
        try {
            var em = FS.project(in.asPlan(), expected.vars()).emit(TermBatchType.TERM, Vars.EMPTY);
            expected.check(em);
        } catch (Throwable t ) {
            ThreadJournal.dumpAndReset(System.err, 80);
            throw t;
        }
        var em = FS.project(in.asPlan(), expected.vars()).emit(CompressedBatchType.COMPRESSED, Vars.EMPTY);
        expected.check(em);
    }

}