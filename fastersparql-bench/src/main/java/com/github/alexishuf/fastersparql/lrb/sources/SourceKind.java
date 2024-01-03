package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.netty.NettyEmitSparqlServer;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlServer;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;

import java.io.File;
import java.io.IOException;

import static com.github.alexishuf.fastersparql.client.model.SparqlEndpoint.parse;

public enum SourceKind {
    HDT_FILE,
    HDT_TSV,
    HDT_JSON,
    HDT_WS,
    HDT_TSV_EMIT,
    HDT_JSON_EMIT,
    HDT_WS_EMIT,
    FS_STORE,
    FS_TSV,
    FS_JSON,
    FS_WS,
    FS_TSV_EMIT,
    FS_JSON_EMIT,
    FS_WS_EMIT;

    public boolean isHdt() {
        return switch (this) {
            case HDT_FILE, HDT_TSV, HDT_JSON, HDT_WS,
                    HDT_TSV_EMIT, HDT_JSON_EMIT, HDT_WS_EMIT -> true;
            default -> false;
        };
    }

    public boolean isFsStore() {
        return switch (this) {
            case FS_STORE,FS_TSV,FS_JSON,FS_WS,
                    FS_TSV_EMIT, FS_JSON_EMIT, FS_WS_EMIT -> true;
            default -> false;
        };
    }

    public boolean isNettyItSparqlServer() {
        return switch (this) {
            case HDT_TSV,HDT_JSON,HDT_WS,FS_TSV,FS_JSON,FS_WS -> true;
            default -> false;
        };
    }

    public boolean isNettyEmitSparqlServer() {
        return switch (this) {
            case HDT_TSV_EMIT,HDT_JSON_EMIT,HDT_WS_EMIT,
                    FS_TSV_EMIT,FS_JSON_EMIT,FS_WS_EMIT -> true;
            default -> false;
        };
    }

    private String augScheme() {
        return switch (this) {
            case HDT_FILE,FS_STORE -> "file://";
            case HDT_TSV,FS_TSV,HDT_TSV_EMIT,FS_TSV_EMIT -> "post,tsv@http://";
            case HDT_JSON,FS_JSON,HDT_JSON_EMIT,FS_JSON_EMIT -> "post,json@http://";
            case HDT_WS,FS_WS,HDT_WS_EMIT,FS_WS_EMIT -> "ws://";
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
            case HDT_FILE, HDT_TSV, HDT_JSON, HDT_WS,HDT_TSV_EMIT,HDT_JSON_EMIT,HDT_WS_EMIT
                    -> //noinspection resource
                    new SourceHandle("file://" + file, source, HDT_FILE);
            case FS_STORE,FS_TSV,FS_JSON,FS_WS,FS_TSV_EMIT,FS_JSON_EMIT,FS_WS_EMIT
                    -> //noinspection resource
                    new SourceHandle("file://"+file, source, FS_STORE);
        };
        if (isNettyItSparqlServer()) {
            var inner = FS.clientFor(parse(handle.specUrl));
            var server = new NettySparqlServer(inner, false, "0.0.0.0", 0);
            String url = augScheme()+"127.0.0.1:"+server.port()+"/sparql";
            return new SourceHandle(url, source, this, AutoCloseableSet.of(server));
        } else if (isNettyEmitSparqlServer()) {
            var inner = FS.clientFor(parse(handle.specUrl));
            var server = new NettyEmitSparqlServer(inner, false, "0.0.0.0", 0);
            String url = augScheme()+"127.0.0.1:"+server.port()+"/sparql";
            return new SourceHandle(url, source, this, AutoCloseableSet.of(server));
        }
        return handle;
    }
}
