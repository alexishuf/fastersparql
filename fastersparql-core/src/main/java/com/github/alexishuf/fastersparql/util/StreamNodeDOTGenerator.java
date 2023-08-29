package com.github.alexishuf.fastersparql.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.function.Function;

class StreamNodeDOTGenerator {
    private record NodeRef(StreamNode node, int index) { }

    public static String toDOT(StreamNode root) {
        var sb = new StringBuilder();
        IdentityHashMap<StreamNode, String> node2id = new IdentityHashMap<>();
        Function<StreamNode, String> node2idMapper = key -> "ref" + node2id.size() + 1;
        ArrayDeque<NodeRef> stack = new ArrayDeque<>();
        ArrayList<StreamNode> upstreamList = new ArrayList<>();
        stack.add(new NodeRef(root, -1));
        sb.append("digraph {\nbgcolor=\"#101010\";");
        while (!stack.isEmpty()) {
            var ref = stack.pop();
            var nId = node2id.computeIfAbsent(ref.node, node2idMapper);
            sb.append("  ").append(nId).append("[label=\"");
            if (ref.index >= 0)
                sb.append('[').append(ref.index).append("] ");
            sb.append(ref.node.nodeLabel().replace("\n", "\\ref").replace("\"", "'"));
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
