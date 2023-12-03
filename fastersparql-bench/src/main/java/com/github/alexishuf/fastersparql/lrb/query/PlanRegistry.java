package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Empty;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.google.gson.*;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PlanRegistry {
    private final Map<String, Node> name2node;

    private PlanRegistry(Map<String, Node> name2node) {
        this.name2node = name2node;
    }

    public static PlanRegistry parse(File file) throws IOException {
        try (FileInputStream is = new FileInputStream(file)) {
            return parse(is);
        }
    }

    public static PlanRegistry parseBuiltin() {
        return parse(PlanRegistry.class.getResourceAsStream("plans.json"));
    }

    public static PlanRegistry parse(InputStream is) {
        return parse(new InputStreamReader(is, UTF_8));
    }

    public static PlanRegistry parse(Reader reader) {
        Map<String, Node> map = new HashMap<>();
        Gson gson = new Gson();
        JsonElement root = JsonParser.parseReader(reader);
        if (!root.isJsonArray()) {
            JsonObject obj = root.getAsJsonObject();
            if (!obj.has("operator")) {
                for (var e : obj.entrySet())
                    map.put(e.getKey(), gson.fromJson(e.getValue(), Node.class));
            } else {
                map.put("", gson.fromJson(obj, Node.class));
            }
        } else {
            JsonArray a = root.getAsJsonArray();
            if (a.size() > 1) {
                int i = 0;
                for (JsonElement e : a)
                    map.put(String.valueOf(i++), gson.fromJson(e, Node.class));
            }
        }
        return new PlanRegistry(map);
    }

    public void resolve(Map<LrbSource, SparqlClient> source2client) {
        for (var e : name2node.entrySet())
            e.getValue().resolve(source2client);
    }
    public void resolve(Federation federation) {
        Map<LrbSource, SparqlClient> host2client = new HashMap<>();
        federation.<Void>forEachSource((src, handleReady) -> {
            String name = src.spec().getString("lrb-name");
            if (name == null) name = src.spec().getString("name");
            LrbSource lrb = LrbSource.tolerantValueOf(name);
            host2client.put(lrb, src.client);
            handleReady.apply(null, null);
        });
        resolve(host2client);
    }

    public Plan createPlan(Object name) {
        String nameString = name.toString();
        Node node = name2node.getOrDefault(nameString, null);
        if (node == null) {
            for (var e : name2node.entrySet()) {
                if (nameString.equalsIgnoreCase(e.getKey())) node = e.getValue();
            }
        }
        return node == null ? null : node.createPlan();
    }

    /* --- --- --- implementation --- --- --- */

    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "unused"})
    private static class Node {
        String operator;
        Map<Object, Object> params;
        List<Node> operands;

        private static final Pattern HOST_RX = Pattern.compile("(?i)https?://([^/:]+)");

        public void resolve(Map<LrbSource, SparqlClient> src2client) {
            for (Node o : operands) o.resolve(src2client);
            var url = params.getOrDefault("endpoint", "").toString();
            Matcher matcher = HOST_RX.matcher(url);
            if (matcher.find()) {
                LrbSource lrb = LrbSource.tolerantValueOf(matcher.group(1));
                params.put("sparqlClient", src2client.get(lrb));
            }
        }

        private Plan  left() { return operands.get(0).createPlan(); }
        private Plan right() { return operands.get(1).createPlan(); }

        private Plan[] operandsArr() {
            return operands.stream().map(Node::createPlan).toArray(Plan[]::new);
        }

        private Vars vars(String name) {
            return Vars.from((Collection<?>) params.getOrDefault(name, List.of()));
        }

        private long longParam(String name, long fallback) {
            return switch (params.getOrDefault(name, null)) {
                case Long l -> l;
                case Integer i -> i;
                case String s -> Long.parseLong(s);
                case Double d -> {
                    if (Math.floor(d) != d)
                        throw new IllegalArgumentException("expected long, got non-integer double");
                    yield d.longValue();
                }
                case null -> fallback;
                default -> throw new IllegalStateException("Unexpected value: " + params.get(name));
            };
        }

        public Plan createPlan() {
           return switch (operator.toUpperCase())  {
               case "JOIN" -> FS.join(operandsArr());
               case "UNION" -> FS.union(operandsArr());
               case "MERGE" -> FS.crossDedupUnion(operandsArr());
               case "LEFT_JOIN" -> FS.leftJoin(left(), right());
               case "FILTER"
                       -> FS.filter(left(), (Collection<?>)params.getOrDefault("filters", List.of()));
               case "FILTER_EXISTS" -> {
                   boolean negate = parseBoolean(params.getOrDefault("negate", "false").toString());
                   yield FS.exists(left(), negate, right());
               }
               case "FILTER_NOT_EXISTS" -> FS.exists(left(), right());
               case "MINUS" -> FS.minus(left(), right());
               case "PROJECT" -> FS.project(left(), vars("vars"));
               case "SLICE" -> FS.slice(left(), longParam("offset", 0),
                                                longParam("limit", Long.MAX_VALUE));
               case "DISTINCT" -> FS.distinct(left());
               case "PRUNED" -> FS.dedup(left());
               case "NOP" -> left();
               case "EMPTY" -> new Empty(vars("publicVars"), vars("allVars"));
               case "QUERY" -> {
                   var client = (SparqlClient)params.get("sparqlClient");
                   if (client == null)
                       throw new IllegalArgumentException("Unresolved sparql client for "+params.get("endpoint"));
                   Object queryObj = (params.get("query"));
                   if (queryObj == null)
                       throw new IllegalArgumentException("No query param");
                   var qry = new OpaqueSparqlQuery(queryObj.toString());
                   yield new Query(qry, client);
               }
               default -> throw new UnsupportedOperationException("Unknown operator "+operator);
           };
        }
    }
}
