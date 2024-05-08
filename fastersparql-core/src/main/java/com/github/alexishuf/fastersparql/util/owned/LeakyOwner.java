package com.github.alexishuf.fastersparql.util.owned;

/**
 * An owner (see {@link Orphan#takeOwnership(Object)}) whose owned objects shall not be
 * considered as leaked. If the particular {@link Owned} implementation is subject to leak
 * detection.
 */
public interface LeakyOwner { }
