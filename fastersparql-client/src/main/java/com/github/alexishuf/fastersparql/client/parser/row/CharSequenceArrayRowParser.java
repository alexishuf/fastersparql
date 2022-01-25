package com.github.alexishuf.fastersparql.client.parser.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;
import org.reactivestreams.Publisher;

import java.util.Collection;

public class CharSequenceArrayRowParser implements RowParser<CharSequence[]> {
    public static final CharSequenceArrayRowParser INSTANCE = new CharSequenceArrayRowParser();

    @Override public Class<CharSequence[]> rowClass() {
        return CharSequence[].class;
    }

    @SuppressWarnings("unchecked") @Override
    public Publisher<CharSequence[]> parseStringsArray(Results<? extends CharSequence[]> source) {
        return (Publisher<CharSequence[]>) source.publisher();
    }

    @SuppressWarnings("unchecked") @Override
    public Publisher<CharSequence[]>
    parseStringsList(Results<? extends Collection<? extends CharSequence>> source) {
        return new MappingPublisher<>((Publisher<Collection<CharSequence>>) source.publisher(),
                                      csColl);
    }

    @SuppressWarnings("unchecked")
    @Override public Publisher<CharSequence[]> parseBytesArray(Results<byte[][]> source) {
        Publisher<?> publisher = StringArrayRowParser.INSTANCE.parseBytesArray(source);
        return (Publisher<CharSequence[]>) publisher;
    }

    @SuppressWarnings("unchecked") @Override
    public Publisher<CharSequence[]> parseBytesList(Results<? extends Collection<byte[]>> source) {
        Publisher<?> publisher = StringArrayRowParser.INSTANCE.parseBytesList(source);
        return (Publisher<CharSequence[]>) publisher;
    }

    /* --- --- --- mapping function instances --- --- --- */

    private static final Throwing.Function<Collection<CharSequence>, CharSequence[]> csColl =
            in -> in.toArray(new CharSequence[0]);
}
