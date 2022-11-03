package com.github.alexishuf.fastersparql.client.parser.fragment;

import java.nio.charset.Charset;

public class CharSequenceFragmentParser implements FragmentParser<CharSequence> {
    public static final CharSequenceFragmentParser INSTANCE = new CharSequenceFragmentParser();

    @Override public Class<CharSequence> fragmentClass() {
        return CharSequence.class;
    }

    @Override public CharSequence parseString(CharSequence fragment, Charset charset) {
        return fragment;
    }

    @Override public CharSequence parseBytes(byte[] fragment, Charset charset) {
        return new String(fragment, charset);
    }
}
