package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlServer;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import org.tukaani.xz.UnsupportedOptionsException;

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
    FS_WS_EMIT,
    TDB2,
    TDB2_TSV_IT,
    TDB2_JSON_IT,
    TDB2_TSV_EMIT,
    TDB2_JSON_EMIT,
    TDB2_WS_EMIT,
    FUSEKI_TDB2_JSON,
    FUSEKI_TDB2_TSV,
    COMUNICA_FED_TSV,
    COMUNICA_FED_JSON,
    COMUNICA_HDT_TSV,
    COMUNICA_HDT_JSON,
    VIRTUOSO_JSON, // virtuoso TSV outputs invalid RDF
    FEDX_FS_JSON_EMIT,
    FEDX_FS_JSON_IT,
    FEDX_HDT_JSON_EMIT,
    FEDX_HDT_JSON_IT,
    FEDX_TDB2_JSON_EMIT,
    FEDX_TDB2_JSON_IT,
    FEDX_FUSEKI_TDB2_JSON,
    FEDX_VIRTUOSO_JSON;

    private static final long IS_HDT;
    private static final long IS_FS;
    private static final long IS_TDB2;
    private static final long IS_EMIT_SERVER;
    private static final long IS_IT_SERVER;
    private static final long IS_TSV;
    private static final long IS_JSON;
    private static final long IS_WS;
    private static final long IS_PROCESS;
    private static final long IS_FEDX;
    private static final long IS_FS_SERVER;
    private static final long IS_SERVER;

    static {
        long hdt = 0, fs = 0, tdb = 0, fedx = 0;
        long emit = 0, it = 0, tsv = 0, json = 0, ws = 0, proc = 0;
        for (SourceKind kind : values()) {
            String name = kind.name();
            int ordinal = kind.ordinal();
            if (name.startsWith("COMUNICA_"))   proc |= 1L << ordinal;
            if (name.startsWith("VIRTUOSO_"))   proc |= 1L << ordinal;
            if (name.startsWith("FUSEKI_TDB2")) proc |= 1L << ordinal;
            if (name.startsWith("FEDX"))        fedx |= 1L << ordinal;
            if (name.contains("HDT_"))          hdt  |= 1L << ordinal;
            if (name.startsWith("FS_"))         fs   |= 1L << ordinal;
            if (name.contains("TDB2"))          tdb  |= 1L << ordinal;
            if (name.endsWith("_IT"))           it   |= 1L << ordinal;
            if (name.endsWith("_EMIT"))         emit |= 1L << ordinal;
            if (name.contains("_TSV"))          tsv  |= 1L << ordinal;
            if (name.contains("_JSON"))         json |= 1L << ordinal;
            if (name.contains("_WS"))           ws   |= 1L << ordinal;
        }
        IS_PROCESS     = proc;
        IS_FEDX        = fedx;
        IS_HDT         = hdt;
        IS_FS          = fs;
        IS_TDB2        = tdb;
        IS_EMIT_SERVER = emit;
        IS_IT_SERVER   = it;
        IS_TSV         = tsv;
        IS_JSON        = json;
        IS_WS          = ws;
        IS_FS_SERVER   = emit|it;
        IS_SERVER      = emit|it|proc;
    }

    public boolean      isHdt() { return (IS_HDT      &(1L<<ordinal())) != 0; }
    public boolean  isFsStore() { return (IS_FS       &(1L<<ordinal())) != 0; }
    public boolean     isTdb2() { return (IS_TDB2     &(1L<<ordinal())) != 0; }
    public boolean   isServer() { return (IS_SERVER   &(1L<<ordinal())) != 0; }
    public boolean isFsServer() { return (IS_FS_SERVER&(1L<<ordinal())) != 0; }
    public boolean      isTsv() { return (IS_TSV      &(1L<<ordinal())) != 0; }
    public boolean     isJson() { return (IS_JSON     &(1L<<ordinal())) != 0; }
    public boolean       isWs() { return (IS_WS       &(1L<<ordinal())) != 0; }
    public boolean  isProcess() { return (IS_PROCESS  &(1L<<ordinal())) != 0; }
    public boolean     isFedX() { return (IS_FEDX     &(1L<<ordinal())) != 0; }

    public Optional<FlowModel> serverFlowModel() {
        int mask = 1 << ordinal();
        if      ((mask&IS_IT_SERVER)   != 0) return Optional.of(FlowModel.ITERATE);
        else if ((mask&IS_EMIT_SERVER) != 0) return Optional.of(FlowModel.EMIT);
        else return Optional.empty();
    }

    public SourceKind fileKind() {
        if      (isProcess()) return this;
        else if (isHdt())     return HDT_FILE;
        else if (isFsStore()) return FS_STORE;
        else if (isTdb2())    return TDB2;
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
        if (isFedX()) {
            if (source != LrbSource.LargeRDFBench_all)
                throw new UnsupportedOptionsException("FedX sources only allow the union LrbSource.LargeRDFBench_all");
            String uri = "process://fedx/%s/?dir=%s".formatted(
                    name().replace("FEDX_", ""),
                    dataDir.getAbsolutePath()
            );
            return new SourceHandle(uri, source, this);
        }
        File file = new File(dataDir, source.filename(this));
        if (!file.exists())
            throw new IOException("File "+file+" not found");
        if (file.isFile() && file.length() == 0)
            throw new IOException("File "+file+" is empty");
        String fileUri;
        if (isProcess()) {
            int split = name().indexOf("_");
            if (split < 0)
                throw new UnsupportedOptionsException("process source "+this+" has no protocol");
            fileUri = "process://"+name().substring(0, split).toLowerCase()+"/?file="
                    + file.getAbsolutePath();
        } else {
            fileUri = "file://"+file;
        }
        var handle = new SourceHandle(fileUri, source, fileKind());
        if (isFsServer()) {
            var inner = FS.clientFor(parse(handle.specUrl));
            var server = new NettySparqlServer(serverFlowModel().orElseThrow(), inner,
                    false, "0.0.0.0", 0);
            var url = augScheme()+"127.0.0.1:"+server.port()+server.sparqlPath();
            return new SourceHandle(url, source, this, AutoCloseableSet.of(server));
        }
        return handle;
    }
}
