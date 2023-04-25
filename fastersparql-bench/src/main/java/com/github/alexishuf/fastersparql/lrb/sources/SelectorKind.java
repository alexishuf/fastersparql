package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.fed.selectors.DictionarySelector;

import static com.github.alexishuf.fastersparql.fed.Selector.*;
import static com.github.alexishuf.fastersparql.fed.selectors.DictionarySelector.FETCH_CLASSES;
import static com.github.alexishuf.fastersparql.fed.selectors.DictionarySelector.FETCH_PREDICATES;

public enum SelectorKind {
    ASK,
    DICT,
    DICT_WITH_CLASSES;

    public Spec createSpec(SourceHandle source) {
        String filename = source.source.name()+name().toLowerCase();
        return switch (this) {
            case ASK -> Spec.of(
                    TYPE, AskSelector.NAME,
                    STATE, Spec.of(STATE_FILE, filename));
            case DICT -> Spec.of(
                    TYPE,             DictionarySelector.NAME,
                    FETCH_PREDICATES, true,
                    FETCH_CLASSES,    false,
                    STATE, Spec.of(STATE_FILE, filename));
            case DICT_WITH_CLASSES -> Spec.of(
                    TYPE,             DictionarySelector.NAME,
                    FETCH_PREDICATES, true,
                    FETCH_CLASSES,    true,
                    STATE, Spec.of(STATE_FILE, filename));
        };
    }

}
