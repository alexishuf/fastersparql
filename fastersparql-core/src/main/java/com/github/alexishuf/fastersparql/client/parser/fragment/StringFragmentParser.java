package com.github.alexishuf.fastersparql.client.parser.fragment;

import java.nio.charset.Charset;

public class StringFragmentParser implements FragmentParser<String> {
    public static final StringFragmentParser INSTANCE = new StringFragmentParser();

    @Override public Class<String> fragmentClass() {
        return String.class;
    }

    @Override public String parseString(CharSequence fragment, Charset charset) {
        return fragment.toString();
    }

    @Override public String parseBytes(byte[] fragment, Charset charset) {
        return new String(fragment, charset);
    }
}
