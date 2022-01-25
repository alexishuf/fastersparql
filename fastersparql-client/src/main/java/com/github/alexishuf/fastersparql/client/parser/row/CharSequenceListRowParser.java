package com.github.alexishuf.fastersparql.client.parser.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class CharSequenceListRowParser implements RowParser<List<CharSequence>> {
    public static final CharSequenceListRowParser INSTANCE = new CharSequenceListRowParser();

    @SuppressWarnings("rawtypes") @Override public Class<List> rowClass() {
        return List.class;
    }

    @SuppressWarnings("unchecked") @Override public Publisher<List<CharSequence>>
    parseStringsArray(Results<? extends CharSequence[]> source) {
        return new MappingPublisher<>((Publisher<CharSequence[]>)source.publisher(), csArray);
    }

    @SuppressWarnings("unchecked") @Override public Publisher<List<CharSequence>>
    parseStringsList(Results<? extends Collection<? extends CharSequence>> source) {

        return new MappingPublisher<>((Publisher<Collection<CharSequence>>) source.publisher(),
                                      csColl);
    }

    @SuppressWarnings("unchecked")
    @Override public Publisher<List<CharSequence>> parseBytesArray(Results<byte[][]> source) {
        Publisher<?> publisher = StringListRowParser.INSTANCE.parseBytesArray(source);
        return (Publisher<List<CharSequence>>) publisher;
    }

    @SuppressWarnings("unchecked") @Override public Publisher<List<CharSequence>>
    parseBytesList(Results<? extends Collection<byte[]>> source) {
        Publisher<?> publisher = StringListRowParser.INSTANCE.parseBytesList(source);
        return (Publisher<List<CharSequence>>) publisher;
    }

    /* --- --- --- map function instances --- --- --- */
    private static final Throwing.Function<CharSequence[], List<CharSequence>>
            csArray = Arrays::asList;
    private static final Throwing.Function<Collection<CharSequence>, List<CharSequence>>
            csColl = in -> in instanceof List ? (List<CharSequence>)in : new ArrayList<>(in);
}
