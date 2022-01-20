package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.CSUtils;

public class TSVParser extends AbstractSVResultsParser {
    public TSVParser(ResultsParserConsumer consumer) {
        super(consumer, "\n");
    }

    @Override protected int readTerm(CharSequence input, int from) {
        int i = CSUtils.skipUntil(input, from, '\n', '\t');
        if (i == input.length())
            carry(input, from);
        else
            addTerm(input, from, i, input.charAt(i) == '\n');
        return i+1;
    }
}
