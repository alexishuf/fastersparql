package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.github.alexishuf.fastersparql.client.util.HeaderUtils.sanitizeHeaderName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HeaderUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            " ",
            " \r\n",
            "\t \r\n",
            ";",
            ",",
            "X-Rate-Limit,",
            "X-Rate-Limit;",
            "X-Rate-Limit ,",
            "X-Rate-Limit ;",
            "X Rate Limit",
            "X Rate",
            "X Rate ",
    })
    public void testSanitizeInvalidHeaderName(String input) {
        assertThrows(SparqlClientInvalidArgument.class, () -> sanitizeHeaderName(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "x-vnd-header | x-vnd-header",
            "X-Rate-Limit | x-rate-limit",
            "Content-Type | content-type",
            "Content-type | content-type",
            "CONTENT-type | content-type",
            "Accept | accept",
            "Accept\t | accept",
            "Accept\r\n | accept",
            "Accept: | accept",
            "\tAccept | accept",
            "\r\nAccept | accept",
            " Accept | accept"
    })
    public void testSanitizeHeaderName(String dataString) {
        String[] data = dataString.split(" *\\| *");
        assertEquals(data[1], sanitizeHeaderName(data[0]));
    }

    @Test
    public void testNullHeaderName() {
        assertThrows(SparqlClientInvalidArgument.class, () -> sanitizeHeaderName(null));
    }
}