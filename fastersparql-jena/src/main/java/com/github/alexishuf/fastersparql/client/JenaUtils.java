package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.org.apache.jena.atlas.lib.Closeable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JenaUtils {
    private static final Logger log = LoggerFactory.getLogger(JenaUtils.class);

    public static <O> @Nullable O safeClose(@Nullable AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                log.error("Failed to close {}", closeable, t);
            }
        }
        return null;
    }

    public static <O> @Nullable O safeClose(@Nullable Closeable jenaCloseable) {
        if (jenaCloseable != null) {
            try {
                jenaCloseable.close();
            } catch (Throwable t) {
                log.error("Failed to close {}", jenaCloseable, t);
            }
        }
        return null;
    }
}
