package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;

import java.nio.charset.Charset;
import java.util.concurrent.Future;

public class StringFragmentParser implements FragmentParser<String> {
    public static final StringFragmentParser INSTANCE = new StringFragmentParser();

    @Override public Class<String> fragmentClass() {
        return String.class;
    }

    @SuppressWarnings("unchecked") @Override
    public FSPublisher<String> parseStrings(Graph<? extends CharSequence> source) {
        if (source.fragmentClass().equals(String.class))
            return (FSPublisher<String>) source.publisher();
        return new MappingPublisher<>(source.publisher(), false, CharSequence::toString);
    }

    @Override public FSPublisher<String> parseBytes(Graph<byte[]> source) {
        Future<Charset> cs = source.charset();
        return new MappingPublisher<>(source.publisher(), a -> new String(a, cs.get()));
    }
}
