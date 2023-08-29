package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.exceptions.BadSerializationException.TomlError;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@SuppressWarnings("unused")
public final class Spec {
    private final Map<String, Object> map;

    public static final String PATHS_RELATIVE_TO = "paths-relative-to";
    public static final Spec EMPTY = new Spec(Map.of());

    public Spec(Map<String, Object> map) {
        this.map = map;
    }

    public Spec(Spec other) { this(other.map); }

    @SuppressWarnings("unchecked")
    public static <T> T coerce(Object o, Class<T> cls) {
        if (o == null) {
            return null;
        } else if (cls.isInstance(o)) {
            return (T) o;
        } else if (cls.isEnum()) {
            String name = o.toString().trim();
            for (Enum<?> val : ((Class<? extends Enum<?>>)cls).getEnumConstants()) {
                if (name.equalsIgnoreCase(val.name()))
                    return (T) val;
            }
            if (SparqlResultFormat.class.equals(cls)) {
                MediaType mt = MediaType.tryParse(name);
                if (mt != null)
                    return (T) SparqlResultFormat.fromMediaType(mt);
            }
        } else if (String.class.equals(cls)) {
            return (T) o.toString();
        } else if (File.class.equals(cls)) {
            return (T)(o instanceof File f ? f : new File(o.toString()));
        } else if (Path.class.equals(cls)) {
            return (T)(o instanceof File f ? f.toPath() : Path.of(o.toString()));
        } else if (MediaType.class.equals(cls)) {
            return (T) MediaType.parse(o.toString());
        } else if (Spec.class.equals(cls)) {
            if (o instanceof Map<?,?> m) return (T) new Spec((Map<String, Object>)m);
            if (o instanceof TomlTable t) return (T) new Spec(t);
            try {
                return (T) Spec.parseToml(o);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else if (o instanceof Collection<?> coll) {
            if (List.class.equals(cls)) return (T) new ArrayList<>(coll);
            if (Set.class.equals(cls)) return (T) new HashSet<>(coll);
        } else if (List.class.equals(cls)) {
            return (T) List.of(o);
        } else if ( Set.class.equals(cls)) {
            return (T) Set.of(o);
        }
        throw typeError(cls, o);
    }

    private static Object fromToml(Object o) {
        return switch (o) {
            case TomlTable t -> new Spec(t);
            case TomlArray a -> {
                List<Object> list = new ArrayList<>(a.size());
                for (int i = 0, n = a.size(); i < n; i++)
                    list.add(fromToml(a.get(i)));
                yield list;
            }
            default -> o;
        };
    }

    public Spec(TomlTable table) {
        this.map = new HashMap<>();
        for (var e : table.entrySet())
            map.put(e.getKey(), fromToml(e.getValue()));
    }

    public static Spec of(Object... keyAndValues) {
        if ((keyAndValues.length&1) == 1)
            throw new IllegalArgumentException("Missing value for last key");
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyAndValues.length; i++) {
            String key = keyAndValues[i++].toString();
            Object value = keyAndValues[i];
            if (value != null)
                map.put(key, value);
        }
        return new Spec(map);
    }

    public static Spec parseToml(Object toml) throws IOException, TomlError {
        Path parent = null;
        TomlParseResult res = switch (toml) {
            case File f -> {
                Path path = f.toPath();
                parent = path.getParent();
                yield Toml.parse(path);
            }
            case Path p -> {
                parent = p.getParent();
                yield Toml.parse(p);
            }
            case InputStream is -> Toml.parse(is);
            case Reader reader -> Toml.parse(reader);
            case String s -> Toml.parse(s);
            default -> throw new TomlError("Cannot parse "+toml);
        };
        if (res.hasErrors())
            throw new TomlError(res.errors());
        Spec spec = new Spec(res);
        Path refDir = spec.get(PATHS_RELATIVE_TO, Path.class);
        if (refDir == null)
            spec.set(PATHS_RELATIVE_TO, parent);
        else if (!refDir.isAbsolute() && parent != null)
            spec.set(PATHS_RELATIVE_TO, parent.resolve(refDir));
        return spec;
    }

    private static void writeValue(Appendable out, Object o) throws IOException {
        if (o == null) {
            out.append("null");
        } else if (o instanceof Collection<?> coll) {
            out.append('[');
            int i = 0;
            for (Object item : coll)
                writeValue(i++ == 0 ? out : out.append(", "), item);
            out.append(']');
        } else if (o instanceof Number || o instanceof Boolean || o instanceof LocalDate || o instanceof LocalDateTime) {
            out.append(o.toString());
        } else if (o instanceof Spec) {
            throw new IllegalArgumentException("Spec must no be written as a value");
        } else {
            out.append('"').append(Toml.tomlEscape(o.toString())).append('"');
        }
    }

    private enum ValueKind {
        PRIMITIVE, SPEC, PRIMITIVE_ARRAY, SPEC_ARRAY, MIXED_ARRAY;

        @SuppressWarnings("UnusedAssignment")
        public static ValueKind of(Object o) {
            return switch (o) {
                case Spec ignored -> SPEC;
                case Map<?, ?> ignored -> SPEC;
                case Collection<?> coll -> {
                    ValueKind kind = null;
                    for (Object item : coll) {
                        var itemKind = switch (of(item)) {
                            case PRIMITIVE, PRIMITIVE_ARRAY -> PRIMITIVE_ARRAY;
                            case MIXED_ARRAY -> MIXED_ARRAY;
                            case SPEC, SPEC_ARRAY -> SPEC_ARRAY;
                        };
                        if      (kind ==     null) kind = itemKind;
                        else if (kind != itemKind) yield MIXED_ARRAY;
                    }
                    yield kind == null ? PRIMITIVE_ARRAY : kind;
                }
                default -> PRIMITIVE;
            };
        }
    }

    private void toToml(StringBuilder path, int pathVisibleFrom,
                        Appendable out) throws IOException {
        // write all members that are not arrays of tables (else they would be parsed
        // as members of the last sub-table of the last array of tables).
        for (var e : map.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            switch (ValueKind.of(value)) {
                case SPEC -> {
                    int oldLen = path.length();
                    path.append(key).append('.');
                    ((Spec) value).toToml(path, pathVisibleFrom, out);
                    path.setLength(oldLen);
                }
                case PRIMITIVE, PRIMITIVE_ARRAY -> {
                    out.append(path, pathVisibleFrom, path.length()).append(key).append(" = ");
                    writeValue(out, value);
                    out.append('\n');
                }
                case MIXED_ARRAY -> throw new UnsupportedOperationException("Cannot serialize arrays with TOML primitive and tables");
            }
        }
        // write arrays of tables
        for (var e : map.entrySet()) {
            Object v = e.getValue();
            if (ValueKind.of(v) != ValueKind.SPEC_ARRAY) continue;
            String key = e.getKey();
            for (Object specObj : ((Collection<?>) e.getValue())) {
                out.append("[[").append(path).append(key).append("]]\n");
                int oldLen = path.length();
                path.append(key).append('.');
                coerce(specObj, Spec.class).toToml(path, path.length(), out);
                path.setLength(oldLen);
            }
        }
    }

    public void toToml(Appendable out) throws IOException {
        toToml(new StringBuilder(), 0, out);
    }

    public String toToml() {
        var buf = new StringBuffer();
        try {
            toToml(new StringBuilder(), 0, buf);
            return buf.toString();
        } catch (IOException e) {
            throw new RuntimeException(e); // never throws
        }
    }

    public void set(String key, @Nullable Object value) {
        if (value == null) map.remove(key);
        map.put(key, value);
    }

    public Set<String> keys() { return map.keySet(); }

    public boolean has(String key) { return map.containsKey(key); }

    public Object get(String key) { return map.get(key); }

    public <T> T get(String key, Class<T> cls) { return coerce(map.get(key), cls); }

    public String getString(String key) { return get(key, String.class); }
    public boolean getBool(String key) { return Boolean.TRUE.equals(get(key, Boolean.class)); }

    public <T> List<T> getListOf(String key, Class<T> itemClass) {
        return toList(get(key), itemClass);
    }

    public <T> List<T> getListOf(String key, Class<T> itemClass, String singletonKey) {
        List<T> list = getListOf(key, itemClass);
        if (list.isEmpty()) {
            T value = get(singletonKey, itemClass);
            if (value != null)
                list = List.of(value);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> toList(Object listObject, Class<T> itemClass) {
        List<?> list = switch (listObject) {
            case null -> List.of();
            case List<?> l -> l;
            case Collection<?> c -> new ArrayList<>(c);
            case Object[] a -> Arrays.asList(a);
            default -> List.of(listObject);
        };
        for (Object o : list) {
            if (o != null && !itemClass.isInstance(o)) {
                ArrayList<T> coerced = new ArrayList<>(list.size());
                for (Object item : list)
                    coerced.add(coerce(item, itemClass));
                return coerced;
            }
        }
        return (List<T>) list;
    }

    public @PolyNull File getFile(String key, @PolyNull String fallback) {
        return getFile(key, get(PATHS_RELATIVE_TO, Path.class), fallback);
    }

    public @PolyNull File getFile(String key, Path refDir, @PolyNull String fallback) {
        Object v = map.get(key);
        if (v instanceof Spec) throw new IllegalArgumentException("Expected file path, got"+v);
        if (v == null) v = fallback;
        if (v == null) return null;

        File file = new File(v.toString());
        if (!file.isAbsolute() && refDir != null)
            file = refDir.resolve(v.toString()).toFile();
        return file;
    }

    public <T> T getOr(String key, T fallback) {
        //noinspection unchecked
        Class<T> cls = fallback == null ? (Class<T>) Object.class : (Class<T>) fallback.getClass();
        T v = get(key, cls);
        return v == null ? fallback : v;
    }

    private static IllegalArgumentException typeError(Class<?> cls, Object v) {
        throw new IllegalArgumentException("Expected a "+ cls.getSimpleName()+", got a "+ v.getClass().getSimpleName()+" "+ v);
    }

    private Spec terminal(List<String> path) {
        Spec spec = this;
        int last = path.size()-1;
        for (int i = 0; i < last; i++) {
            switch (spec.get(path.get(i))) {
                case null -> { spec = EMPTY; i = last; }
                case Spec s -> spec = s;
                default     -> throw new IllegalArgumentException("Cannot get "+path+" since intermediary key "+path.get(i)+" is a terminal value");
            }
        }
        return spec;
    }

    public boolean has(List<String> path) {
        return terminal(path).has(path.get(path.size()-1));
    }

    public <T> T get(List<String> path, Class<T> cls) {
        return terminal(path).get(path.get(path.size()-1), cls);
    }

    public <T> T getOr(List<String> path, T fallback) {
        return terminal(path).getOr(path.get(path.size()-1), fallback);
    }

    public boolean getBool(List<String> path) { return Boolean.TRUE.equals(getOr(path, false)); }

    public <T> List<T> getListOf(List<String> path, Class<T> cls) {
        return toList(get(path, Object.class), cls);
    }

    public <T> List<T> getListOf(List<String> path, Class<T> cls, List<String> singletonPath) {
        List<T> list = getListOf(path, cls);
        if (list.isEmpty()) {
            T value = get(singletonPath, cls);
            if (value != null)
                list = List.of(value);
        }
        return list;
    }

    public File getFile(List<String> path, String fallback) {
        return getFile(path, get(PATHS_RELATIVE_TO, Path.class), fallback);
    }

    public File getFile(List<String> path, Path refDir, String fallback) {
        File file = get(path, File.class);
        if (file == null) {
            if      (fallback == null) return null;
            else if (refDir ==   null) return new File(fallback);
            else                       return refDir.resolve(fallback).toFile();
        }
        return file.isAbsolute() || refDir == null ? file
                : refDir.resolve(file.toPath()).toFile();
    }

    public boolean equals(Object object) {
        return object == this || (object instanceof Spec r && map.equals(r.map));
    }

    @Override public int hashCode() { return map.hashCode(); }

    @Override public String toString() { return toToml(); }

}
