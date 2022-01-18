package com.github.alexishuf.fastersparql.client.parser.results;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TestConsumer implements ResultsParserConsumer {
    List<String> vars;
    List<String[]> rows = new ArrayList<>();
    List<String> errors = new ArrayList<>();
    int endCalls;

    @Override public void vars(List<String> vars) {
        this.vars = vars;
    }

    @Override public void row(@Nullable String[] row) {
        this.rows.add(row);
    }

    @Override public void end() {
        ++endCalls;
    }

    @Override public void onError(String message) {
        this.errors.add(message);
    }

    void check(@Nullable List<String> vars, @Nullable List<@Nullable String[]> rows) {
        assertEquals(vars, this.vars);
        if (rows != null) {
            assertNotNull(this.rows);
            int validRows = rows.size()
                    - (!rows.isEmpty() && rows.get(rows.size() - 1) == null ? 1 : 0);
            assertEquals(validRows, this.rows.size());
            for (int i = 0; i < validRows; i++)
                assertArrayEquals(rows.get(i), this.rows.get(i), "i=" + i);
        }
        boolean expectError = vars == null || rows == null || rows.contains(null);
        assertEquals(!expectError, errors.isEmpty(), "errors="+errors);
        assertEquals(endCalls, 1);
    }
}
