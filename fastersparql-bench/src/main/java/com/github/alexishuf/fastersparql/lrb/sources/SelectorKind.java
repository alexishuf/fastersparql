package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.fed.selectors.DictionarySelector;
import com.github.alexishuf.fastersparql.fed.selectors.FSStoreSelector;

import static com.github.alexishuf.fastersparql.fed.Selector.STATE;
import static com.github.alexishuf.fastersparql.fed.Selector.TYPE;
import static com.github.alexishuf.fastersparql.fed.selectors.DictionarySelector.FETCH_CLASSES;
import static com.github.alexishuf.fastersparql.fed.selectors.DictionarySelector.FETCH_PREDICATES;

public enum SelectorKind {
    ASK,
    DICT,
    DICT_WITH_CLASSES,
    FS_STORE;

    public Spec createSpec(SourceHandle source) {
        String fileOrDirName = source.source.name()+name().toLowerCase();
        return switch (this) {
            case ASK -> Spec.of(
                    TYPE, AskSelector.NAME,
                    STATE, Spec.of(AskSelector.STATE_FILE, fileOrDirName));
            case DICT -> Spec.of(
                    TYPE,             DictionarySelector.NAME,
                    FETCH_PREDICATES, true,
                    FETCH_CLASSES,    false,
                    STATE, Spec.of(DictionarySelector.STATE_DIR, fileOrDirName));
            case DICT_WITH_CLASSES -> Spec.of(
                    TYPE,             DictionarySelector.NAME,
                    FETCH_PREDICATES, true,
                    FETCH_CLASSES,    true,
                    STATE, Spec.of(DictionarySelector.STATE_DIR, fileOrDirName));
            case FS_STORE -> Spec.of(TYPE, FSStoreSelector.NAME);
        };
    }

}
