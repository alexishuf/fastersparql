package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import com.github.alexishuf.fastersparql.util.SafeCloseable;

import java.util.Objects;

public final class SourceHandle implements SafeCloseable {
    public final String specUrl;
    public final LrbSource source;
    public final SourceKind kind;
    private final AutoCloseableSet<?> closeables;

    public SourceHandle(String specUrl, LrbSource source, SourceKind kind) {
        this(specUrl, source, kind, AutoCloseableSet.empty());
    }

    public SourceHandle(String specUrl, LrbSource source, SourceKind kind,
                        AutoCloseableSet<?> closeables) {
        this.specUrl = specUrl;
        this.source = source;
        this.kind = kind;
        this.closeables = closeables;
    }

    @Override public void close() { ExceptionCondenser.closeAll(closeables); }

    @SuppressWarnings("unused") public String     specUrl() { return specUrl; }
    @SuppressWarnings("unused") public LrbSource  source()  { return source; }
    @SuppressWarnings("unused") public SourceKind kind()    { return kind; }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SourceHandle) obj;
        return Objects.equals(this.specUrl, that.specUrl) &&
                Objects.equals(this.source, that.source) &&
                Objects.equals(this.kind, that.kind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(specUrl, source, kind);
    }

    @Override
    public String toString() {
        return "SourceHandle[" +
                "specUrl=" + specUrl + ", " +
                "source=" + source + ", " +
                "kind=" + kind + ']';
    }

}
