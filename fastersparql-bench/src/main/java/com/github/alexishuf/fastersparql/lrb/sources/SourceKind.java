package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlServer;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;

import java.io.File;
import java.io.IOException;

import static com.github.alexishuf.fastersparql.client.model.SparqlEndpoint.parse;

public enum SourceKind {
    HDT_FILE,
    HDT_TSV,
    HDT_JSON,
    HDT_WS;

    private String augScheme() {
        return switch (this) {
            case HDT_FILE -> "file://";
            case HDT_TSV -> "post,tsv@http://";
            case HDT_JSON -> "post,json@http://";
            case HDT_WS -> "ws://";
        };
    }

    public SourceHandle createHandle(LrbSource source, File dataDir) throws IOException {
        if (dataDir == null)
            throw new IOException("Cannot open files with null dataDir.");
        File file = new File(dataDir, source.filename(this));
        if (!file.exists())
            throw new IOException("File "+file+" not found");
        if (file.isFile() && file.length() == 0)
            throw new IOException("File "+file+" is empty");
        var handle = switch (this) {
            case HDT_FILE, HDT_TSV, HDT_JSON, HDT_WS -> //noinspection resource
                    new SourceHandle("file://" + file, source, HDT_FILE);
        };
        return switch (this) {
            case HDT_WS, HDT_TSV, HDT_JSON -> {
                var hdt = FS.clientFor(parse(handle.specUrl));
                var server = new NettySparqlServer(hdt, "0.0.0.0", 0);
                String url = augScheme()+"127.0.0.1:"+server.port()+"/sparql";
                yield new SourceHandle(url, source, this, AutoCloseableSet.of(server));
            }
            default -> handle;
        };
    }
}
