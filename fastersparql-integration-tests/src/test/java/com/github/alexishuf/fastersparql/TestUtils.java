package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.SparqlClientFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class TestUtils {
    public static File extract(Class<?> refClass, String resourcePath) {
        try (InputStream is = refClass.getResourceAsStream(resourcePath)) {
            if (is == null)
                throw new RuntimeException(resourcePath+"relative to "+refClass+" not found");
            String filename = resourcePath.replaceAll("^.*/([^/]+)$", "$1");
            File file = Files.createTempFile("fastersparql-", filename).toFile();
            try (FileOutputStream os = new FileOutputStream(file)) {
                byte[] buf = new byte[4096];
                for (int len = is.read(buf); len >= 0; len = is.read(buf))
                    os.write(buf, 0, len);
            }
            return file;
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        }
    }

    public static String decodeOrToString(Object o) {
        if (o instanceof byte[]) {
            return StandardCharsets.UTF_8.decode(ByteBuffer.wrap((byte[]) o)).toString();
        } else {
            return o == null ? null : o.toString();
        }
    }

    public static List<SparqlClientFactory> allClientFactories() {
        ArrayList<SparqlClientFactory> list = new ArrayList<>();
        for (SparqlClientFactory f : ServiceLoader.load(SparqlClientFactory.class)) {
            list.add(f);
        }
        return list;
    }
}
