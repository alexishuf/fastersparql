package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.model.Graph;
import org.reactivestreams.Publisher;

public class CharSequenceFragmentParser implements FragmentParser<CharSequence> {
    public static CharSequenceFragmentParser INSTANCE = new CharSequenceFragmentParser();

    @Override public Class<CharSequence> fragmentClass() {
        return CharSequence.class;
    }

    @Override public Publisher<CharSequence> parseStrings(Graph<? extends CharSequence> source) {
        //noinspection unchecked
        return (Publisher<CharSequence>) source.publisher();
    }

    @Override public Publisher<CharSequence> parseBytes(Graph<byte[]> source) {
        Publisher<?> strings = new StringFragmentParser().parseBytes(source);
        //noinspection unchecked
        return (Publisher<CharSequence>) strings;
    }
}
