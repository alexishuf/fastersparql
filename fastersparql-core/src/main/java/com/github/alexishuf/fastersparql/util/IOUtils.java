package com.github.alexishuf.fastersparql.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class IOUtils {
    public static void writeWithTmp(File destination,
                             ThrowingConsumer<FileOutputStream, IOException> writer)
            throws IOException {
        File parent = destination.getParentFile();
        if (parent == null) parent = new File("");
        if (!parent.exists() && !parent.mkdirs())
            throw new IOException("Could not mkdir "+parent);
        else if (!parent.isDirectory())
            throw new IOException(parent+" exists but is not a directory");
        File tmp = new File(parent, destination.getName() + ".tmp");
        if (tmp.exists() && !tmp.delete())
            throw new IOException("Could not remove stale"+tmp);
        try (var os = new FileOutputStream(tmp)) {
            writer.accept(os);
        }
        Files.move(tmp.toPath(), destination.toPath(), REPLACE_EXISTING);
    }

    public static String readAll(File file) throws IOException {
        try (var is = new FileInputStream(file)) {
            return new String(is.readAllBytes(), UTF_8);
        }
    }

}
