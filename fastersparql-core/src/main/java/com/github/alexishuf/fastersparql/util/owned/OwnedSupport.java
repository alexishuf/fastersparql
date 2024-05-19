package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.util.concurrent.JournalNamed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;

public class OwnedSupport {
    static final Logger OWNED_LOG = LoggerFactory.getLogger(Owned.class);
    static final Logger ORPHAN_LOG = LoggerFactory.getLogger(Orphan.class);

    public static String makeJournalName(Owned<?> o) {
        Class<?> cls = o.getClass();
        if (o instanceof Orphan<?>) {
            Class<?> sup = cls.getSuperclass();
            if (Owned.class.isAssignableFrom(sup))
                cls = sup;
        }
        String name = cls.getSimpleName();
        if (name.isEmpty())
            name = cls.getName();
        return name+'@'+toHexString(identityHashCode(o));
    }

    public static void handleRecycleError(Logger logger, String kind, Object object, Throwable err) {
        String str;
        try {
            str = ((JournalNamed)object).journalName();
        } catch (Throwable strError)  {
            str = object.getClass().getName()+'@'+toHexString(identityHashCode(object));
        }
        logger.error("Error recycling {} {}", kind, str, err);

    }
}
