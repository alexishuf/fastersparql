package com.github.alexishuf.fastersparql.client.parser.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;

import java.util.Collection;

public class CharSequenceArrayRowParser implements RowParser<CharSequence[]> {
    public static final CharSequenceArrayRowParser INSTANCE = new CharSequenceArrayRowParser();

    @Override public Class<CharSequence[]> rowClass() {
        return CharSequence[].class;
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<CharSequence[]> parseStringsArray(Results<? extends CharSequence[]> source) {
        return (FSPublisher<CharSequence[]>) source.publisher();
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<CharSequence[]>
    parseStringsList(Results<? extends Collection<? extends CharSequence>> source) {
        return new MappingPublisher<>((FSPublisher<Collection<CharSequence>>) source.publisher(),
                                      csColl);
    }

    @SuppressWarnings("unchecked")
    @Override public FSPublisher<CharSequence[]> parseBytesArray(Results<byte[][]> source) {
        FSPublisher<?> publisher = StringArrayRowParser.INSTANCE.parseBytesArray(source);
        return (FSPublisher<CharSequence[]>) publisher;
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<CharSequence[]> parseBytesList(Results<? extends Collection<byte[]>> source) {
        FSPublisher<?> publisher = StringArrayRowParser.INSTANCE.parseBytesList(source);
        return (FSPublisher<CharSequence[]>) publisher;
    }

    /* --- --- --- mapping function instances --- --- --- */

    private static final Throwing.Function<Collection<CharSequence>, CharSequence[]> csColl =
            in -> in.toArray(new CharSequence[0]);
}
