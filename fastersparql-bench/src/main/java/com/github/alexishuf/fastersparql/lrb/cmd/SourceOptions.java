package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

@Command
public class SourceOptions {
    @Option(names = "--lrb-source", split = ",", arity = "0..*",
            description = "Limit the sources in the federation to the ones that match at least " +
                    "one of the given regular expressions. Expressions are evaluated case " +
                    "insensitive against names of the LrbSources enum, spelled with '_' and " +
                    "with '-'.  The default is to include all.")
    List<String> lrbSourceFilters;

    public Set<LrbSource> lrbSources() {
        if (lrbSourceFilters ==  null || lrbSourceFilters.isEmpty())
            return LrbSource.all();
        Set<LrbSource> set = new LinkedHashSet<>();
        var patterns = lrbSourceFilters.stream()
                .map(r -> Pattern.compile(r, CASE_INSENSITIVE)).toList();
        for (LrbSource src : LrbSource.values()) {
            for (Pattern p : patterns) {
                boolean match = p.matcher(src.name()).matches()
                        ||  p.matcher(src.name().replace("_", "-")).matches();
                if (match) {
                    set.add(src);
                    break;
                }
            }
        }
        if (set.isEmpty())
            throw new IllegalArgumentException("No sources match any of the given patterns");
        return set;
    }

    @Option(names = "--selector", description = "What selector shall be used for sources")
    public SelectorKind selKind = SelectorKind.DICT;

    @Option(names = "--source", description = "Implementation of sources in the federation")
    public SourceKind srcKind = SourceKind.HDT_FILE;


    public @Nullable File dataDir;
    @Option(names = "--data-dir",
            description = "Dir containing the data files backing the source triple stores")
    public void setDataDir(File dataDir) throws IOException {
        if (!dataDir.isDirectory()) throw new IOException("No such dir: "+dataDir);
        this.dataDir = dataDir;
    }
}
