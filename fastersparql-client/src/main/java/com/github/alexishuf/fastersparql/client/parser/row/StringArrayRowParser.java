package com.github.alexishuf.fastersparql.client.parser.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;
import org.reactivestreams.Publisher;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class StringArrayRowParser implements RowParser<String[]> {
    public static final StringArrayRowParser INSTANCE = new StringArrayRowParser();

    @Override public Class<String[]> rowClass() {
        return String[].class;
    }

    @SuppressWarnings("unchecked") @Override
    public Publisher<String[]> parseStringsArray(Results<? extends CharSequence[]> source) {
        if (source.rowClass().equals(String[].class))
            return (Publisher<String[]>) source.publisher();
        return new MappingPublisher<>((Publisher<CharSequence[]>) source.publisher(), csArrayMapper);
    }

    @SuppressWarnings("unchecked") @Override public Publisher<String[]>
    parseStringsList(Results<? extends Collection<? extends CharSequence>> source) {
        return new MappingPublisher<>(
                (Publisher<Collection<? extends CharSequence>>) source.publisher(), csCollMapper);
    }

    @Override public Publisher<String[]> parseBytesArray(Results<byte[][]> source) {
        return new MappingPublisher<>(source.publisher(), bArrayMapper);
    }

    @SuppressWarnings("unchecked") @Override
    public Publisher<String[]> parseBytesList(Results<? extends Collection<byte[]>> source) {
        return new MappingPublisher<>((Publisher<Collection<byte[]>>) source.publisher(),
                                      bCollMapper);
    }

    /* --- --- --- static function objects --- --- --- */

    private static final Throwing.Function<CharSequence[], String[]> csArrayMapper = in -> {
        String[] out = new String[in.length];
        for (int i = 0; i < in.length; i++)
            out[i] = ((CharSequence[])in)[i].toString();
        return out;
    };
    private static final Throwing.Function<Collection<? extends CharSequence>, String[]>
            csCollMapper = in -> {
        int i = 0;
        String[] out = new String[in.size()];
        for (CharSequence charSequence : in)
            out[i++] = charSequence.toString();
        return out;
    };
    private static final  Throwing.Function<byte[][], String[]> bArrayMapper = in -> {
        String[] out = new String[in.length];
        for (int i = 0; i < in.length; i++)
            out[i] = new String(in[i], StandardCharsets.UTF_8);
        return out;
    };
    private static final Throwing.Function<Collection<byte[]>, String[]> bCollMapper = in -> {
        String[] out = new String[in.size()];
        int i = 0;
        for (byte[] bytes : in)
            out[i++] = new String(bytes, StandardCharsets.UTF_8);
        return out;
    };
}
