package com.github.alexishuf.fastersparql.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.function.Function;

public class StreamNodeDOT {
    private record NodeRef(StreamNode node, int index) { }

    public static StringBuilder appendRequested(StringBuilder sb, long n) {
        return n > 999_999 ? sb.append("0x").append(Long.toHexString(n)) : sb.append(n);
    }

    public static StringBuilder minimalLabel(StringBuilder sb, StreamNode n) {
        String cls = n.getClass().getSimpleName();
        int trim;
        if      (cls.endsWith("Emitter")) trim = 7;
        else if (cls.endsWith("Stage"  )) trim = 5;
        else                              trim = 0;
        return sb.append(cls, 0, cls.length()-trim)
                .append('@').append(Integer.toHexString(System.identityHashCode(n)));
    }

    public enum Label {
        MINIMAL,
        SIMPLE,
        WITH_STATE,
        WITH_STATE_AND_STATS;

        public boolean showState() {
            return switch (this) {
                case WITH_STATE,WITH_STATE_AND_STATS -> true;
                case SIMPLE,MINIMAL -> false;
            };
        }

        public boolean showStats() {
            return switch (this) {
                case MINIMAL,SIMPLE,WITH_STATE -> false;
                case WITH_STATE_AND_STATS -> true;
            };
        }
    }

    public static String toDOT(StreamNode root, Label type) {
        var sb = new StringBuilder();
        IdentityHashMap<StreamNode, Boolean> visited = new IdentityHashMap<>();
        IdentityHashMap<StreamNode, String> node2id = new IdentityHashMap<>();
        Function<StreamNode, String> node2idMapper = key -> "ref" + node2id.size() + 1;
        ArrayDeque<NodeRef> stack = new ArrayDeque<>();
        ArrayList<StreamNode> upstreamList = new ArrayList<>();
        stack.add(new NodeRef(root, -1));
        sb.append("digraph {\nbgcolor=\"#101010\";");
        while (!stack.isEmpty()) {
            var ref = stack.pop();
            if (visited.put(ref.node, Boolean.TRUE) == Boolean.TRUE)
                continue; // already visited
            var nId = node2id.computeIfAbsent(ref.node, node2idMapper);
            sb.append("  ").append(nId).append("[label=\"");
            if (ref.index >= 0)
                sb.append('[').append(ref.index).append("] ");
            sb.append(ref.node.label(type).replace("\n", "\\n").replace("\"", "'"));
            sb.append("\", color=\"#f0f0f0\", fontcolor=\"#f0f0f0\"];\n");
            upstreamList.clear();
            ref.node.upstream().forEachOrdered(upstreamList::add);
            for (int i = 0, n = upstreamList.size(); i < n; i++) {
                StreamNode u = upstreamList.get(i);
                var uId = node2id.computeIfAbsent(u, node2idMapper);
                sb.append("  ").append(uId).append(" -> ").append(nId)
                        .append(" [color=\"#f0f0f0\"];\n");
                stack.push(new NodeRef(u, n == 1 ? -1 : i));
            }
        }
        return sb.append("}\n").toString();
    }
}
