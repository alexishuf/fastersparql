package com.github.alexishuf.fastersparql.client.parser.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class StringListRowParser implements RowParser<List<String>> {
    public static final StringListRowParser INSTANCE = new StringListRowParser();

    @SuppressWarnings("rawtypes") @Override public Class<List> rowClass() {
        return List.class;
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<List<String>> parseStringsArray(Results<? extends CharSequence[]> source) {
        if (source.rowClass().equals(String[].class))
            return new MappingPublisher<>((FSPublisher<String[]>) source.publisher(), strArray);
        return new MappingPublisher<>((FSPublisher<CharSequence[]>) source.publisher(), csArray);
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<List<String>>
    parseStringsList(Results<? extends Collection<? extends CharSequence>> source) {
        FSPublisher<Collection<CharSequence>> publisher =
                (FSPublisher<Collection<CharSequence>>) source.publisher();
        if (List.class.isAssignableFrom(source.rowClass()))
            return new MappingPublisher<>(publisher, new StringCollectionMapper());
        return new MappingPublisher<>(publisher, convertCollection);
    }

    @Override public FSPublisher<List<String>> parseBytesArray(Results<byte[][]> source) {
        return new MappingPublisher<>(source.publisher(), bArray);
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<List<String>> parseBytesList(Results<? extends Collection<byte[]>> source) {
        return new MappingPublisher<>((FSPublisher<Collection<byte[]>>)source.publisher(), bColl);
    }

    /* --- --- --- mapping function classes --- --- --- */

    private static class StringCollectionMapper implements
            Throwing.Function<Collection<CharSequence>, List<String>> {
        private boolean failedPassThrough = false;

        @Override
        public List<String> apply(Collection<CharSequence> collection) {
            if (!failedPassThrough) {
                boolean ok = collection instanceof List;
                if (ok) {
                    for (CharSequence cs : collection) {
                        if (!(cs instanceof String)) {
                            ok = false;
                            break;
                        }
                    }
                }
                if (!ok)
                    failedPassThrough = true; // do not retry for next items
            }
            if (failedPassThrough) {
                ArrayList<String> list = new ArrayList<>(collection.size());
                for (CharSequence cs : collection)
                    list.add(cs.toString());
                return list;
            }
            List<?> list = (List<?>) collection;
            //noinspection unchecked
            return (List<String>)list;
        }
    }

    /* --- --- --- mapping function instances --- --- --- */

    private static final Throwing.Function<String[], List<String>> strArray = Arrays::asList;
    private static final Throwing.Function<CharSequence[], List<String>> csArray = in -> {
        ArrayList<String> out = new ArrayList<>(in.length);
        for (CharSequence cs : in) out.add(cs.toString());
        return out;
    };
    private static final  Throwing.Function<Collection<CharSequence>, List<String>>
            convertCollection = in -> {
        ArrayList<String> list = new ArrayList<>(in.size());
        for (CharSequence cs : in) list.add(cs.toString());
        return list;
    };
    private static final Throwing.Function<byte[][], List<String>> bArray = in -> {
        ArrayList<String> out = new ArrayList<>(in.length);
        for (byte[] bytes : in) out.add(new String(bytes, StandardCharsets.UTF_8));
        return out;
    };
    private static final Throwing.Function<Collection<byte[]>, List<String>> bColl = in -> {
        ArrayList<String> out = new ArrayList<>(in.size());
        for (byte[] bytes : in) out.add(new String(bytes, StandardCharsets.UTF_8));
        return out;
    };
}
