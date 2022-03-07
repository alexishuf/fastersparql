package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;

public class CharSequenceFragmentParser implements FragmentParser<CharSequence> {
    public static CharSequenceFragmentParser INSTANCE = new CharSequenceFragmentParser();

    @Override public Class<CharSequence> fragmentClass() {
        return CharSequence.class;
    }

    @Override public FSPublisher<CharSequence> parseStrings(Graph<? extends CharSequence> source) {
        //noinspection unchecked
        return (FSPublisher<CharSequence>) source.publisher();
    }

    @Override public FSPublisher<CharSequence> parseBytes(Graph<byte[]> source) {
        FSPublisher<?> strings = new StringFragmentParser().parseBytes(source);
        //noinspection unchecked
        return (FSPublisher<CharSequence>) strings;
    }
}
