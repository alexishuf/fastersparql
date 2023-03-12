package com.github.alexishuf.fastersparql.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class IOUtilsTest {

    @Test void testMkdir() throws IOException {
        File dir = Files.createTempDirectory("fastersparql").toFile();
        assertTrue(dir.delete());
        assertFalse(dir.exists());
        File subdir = new File(dir, "sub");
        File dest = new File(subdir, "file.txt");
        File tmp1 = new File(subdir, "file.txt.tmp");
        File tmp2 = new File(subdir, ".file.txt.tmp");

        IOUtils.writeWithTmp(dest, os -> os.write("hello".getBytes(UTF_8)));
        assertTrue(dir.isDirectory());
        assertTrue(subdir.isDirectory());
        assertTrue(dest.isFile());
        assertEquals("hello", IOUtils.readAll(dest));
        assertFalse(tmp1.exists());
        assertFalse(tmp2.exists());
    }

    @Test void testOverwrite() throws IOException {
        File dir = Files.createTempDirectory("fastersparql").toFile();
        File dest = new File(dir, "file.txt"), tmp = new File(dir, "file.txt.tmp");
        try (var w = new FileWriter(tmp)) { w.write("tmp"); }
        try (var w = new FileWriter(dest)) { w.write("dest"); }
        IOUtils.writeWithTmp(dest, os -> os.write("overwritten".getBytes(UTF_8)));
        assertEquals("overwritten", IOUtils.readAll(dest));
        assertFalse(tmp.exists());
    }

}