package com.github.alexishuf.fastersparql.client.parser.fragment;

import java.nio.charset.Charset;


/**
 * Converts strings and bytes into instances of {@code F}.
 * @param <F> the resulting Fragment type.
 */
public interface FragmentParser<F> {
    /**
     * The {@link Class} of the produced fragment instances.
     * @return a non-null {@link Class};
     */
    Class<F> fragmentClass();

    /** Parse a {@link CharSequence} fragment, using {@code charset} if {@code R} requires it */
    F parseString(CharSequence fragment, Charset charset);

    /** Parse a {@code byte[]} fragment */
    F parseBytes(byte[] fragment, Charset charset);
}
