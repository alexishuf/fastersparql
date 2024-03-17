package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlServer;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.github.alexishuf.fastersparql.client.model.SparqlEndpoint.parse;

public enum SourceKind {
    HDT_FILE,
    HDT_TSV_IT,
    HDT_JSON_IT,
    HDT_WS_IT,
    HDT_TSV_EMIT,
    HDT_JSON_EMIT,
    HDT_WS_EMIT,
    FS_STORE,
    FS_TSV_IT,
    FS_JSON_IT,
    FS_WS_IT,
    FS_TSV_EMIT,
    FS_JSON_EMIT,
    FS_WS_EMIT;

    private static final long IS_HDT;
    private static final long IS_FS;
    private static final long IS_EMIT_SERVER;
    private static final long IS_IT_SERVER;
    private static final long IS_TSV;
    private static final long IS_JSON;
    private static final long IS_WS;
    private static final long IS_SERVER;

    static {
        long hdt = 0, fs = 0, emit = 0, it = 0, tsv = 0, json = 0, ws = 0;
        for (SourceKind kind : values()) {
            String name = kind.name();
            int ordinal = kind.ordinal();
            if (name.startsWith("HDT_")) hdt  |= 1 << ordinal;
            if (name.startsWith("FS_"))  fs   |= 1 << ordinal;
            if (name.endsWith("_IT"))    it   |= 1 << ordinal;
            if (name.endsWith("_EMIT"))  emit |= 1 << ordinal;
            if (name.contains("_TSV"))   tsv  |= 1 << ordinal;
            if (name.contains("_JSON"))  json |= 1 << ordinal;
            if (name.contains("_WS"))    ws   |= 1 << ordinal;
        }
        IS_HDT         = hdt;
        IS_FS          = fs;
        IS_EMIT_SERVER = emit;
        IS_IT_SERVER   = it;
        IS_SERVER      = emit|it;
        IS_TSV         = tsv;
        IS_JSON        = json;
        IS_WS          = ws;
    }

    public boolean isHdt() { return (IS_HDT&(1<<ordinal())) != 0; }
    public boolean isFsStore() { return (IS_FS&(1<<ordinal())) != 0; }
    public boolean isServer() { return (IS_SERVER&(1<<ordinal())) != 0; }
    public boolean isTsv() { return (IS_TSV&(1<<ordinal())) != 0; }
    public boolean isJson() { return (IS_JSON&(1<<ordinal())) != 0; }
    public boolean isWs() { return (IS_WS&(1<<ordinal())) != 0; }

    public Optional<FlowModel> serverFlowModel() {
        int mask = 1 << ordinal();
        if      ((mask&IS_IT_SERVER)   != 0) return Optional.of(FlowModel.ITERATE);
        else if ((mask&IS_EMIT_SERVER) != 0) return Optional.of(FlowModel.EMIT);
        else return Optional.empty();
    }

    public SourceKind fileKind() {
        if      (isHdt())     return HDT_FILE;
        else if (isFsStore()) return FS_STORE;
        else                  throw new UnsupportedOperationException();
    }

    private String augScheme() {
        if (isServer()) {
            if      (isTsv())  return "post,tsv@http://";
            else if (isJson()) return "post,json@http://";
            else if (isWs())   return "ws://";
            else               throw new UnsupportedOperationException();
        } else {
            return "file://";
        }
    }

    public SourceHandle createHandle(LrbSource source, File dataDir) throws IOException {
        if (dataDir == null)
            throw new IOException("Cannot open files with null dataDir.");
        File file = new File(dataDir, source.filename(this));
        if (!file.exists())
            throw new IOException("File "+file+" not found");
        if (file.isFile() && file.length() == 0)
            throw new IOException("File "+file+" is empty");
        var handle = new SourceHandle("file://"+file, source, fileKind());
        if (isServer()) {
            var inner = FS.clientFor(parse(handle.specUrl));
            var server = new NettySparqlServer(serverFlowModel().orElseThrow(), inner,
                    false, "0.0.0.0", 0);
            String url = augScheme()+"127.0.0.1:"+server.port()+"/sparql";
            return new SourceHandle(url, source, this, AutoCloseableSet.of(server));
        }
        return handle;
    }
}
