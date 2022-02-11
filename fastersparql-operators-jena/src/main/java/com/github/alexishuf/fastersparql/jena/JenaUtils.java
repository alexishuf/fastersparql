package com.github.alexishuf.fastersparql.jena;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.PolyNull;

import static org.apache.jena.graph.NodeFactory.*;

public class JenaUtils {
    private static final Node FALSE = createLiteral("false", XSDDatatype.XSDboolean);
    private static final Node TRUE = createLiteral("true", XSDDatatype.XSDboolean);

    public static @PolyNull Node fromNT(@PolyNull String nt) {
        if (nt == null)
            return null;
        nt = nt.trim();
        int ntLen = nt.length();
        if (ntLen == 0)
            throw new IllegalArgumentException("Empty string is not a valid NT term");
        char first = nt.charAt(0), last = nt.charAt(ntLen-1);
        if (first == '"') {
            return parseLiteral(nt, ntLen, last, '"');
        } else if (first == '\'') {
            return parseLiteral(nt, ntLen, last, '\'');
        } else if (first == '_') {
            if (ntLen < 2 || nt.charAt(1) != ':')
                throw new IllegalArgumentException("Bad blank node "+nt);
            return createBlankNode(nt.substring(2));
        } else if (first == '<') {
            if (last != '>') throw new IllegalArgumentException("Bad NT IRI: "+nt);
            return createURI(nt.substring(1, ntLen-1));
        } else if (first == 'f' && nt.equals("false")) {
            return FALSE;
        } else if (first == 't' && nt.equals("true")) {
            return TRUE;
        } else if (first > '0' && first < '9' || first == '-' || first == '+') {
            if (nt.indexOf('e') >= 0 || nt.indexOf('E') >= 0) {
                return createLiteral(nt, XSDDatatype.XSDdouble);
            } else if (nt.indexOf('.') >= 0) {
                return createLiteral(nt, XSDDatatype.XSDdecimal);
            } else {
                return createLiteral(nt, XSDDatatype.XSDinteger);
            }
        } else if (first == '[' && last == ']') {
            if (CSUtils.skipSpaceAnd(nt, 1, ntLen-1, '\0') < last-1)
                throw new IllegalArgumentException("Non-empty blank node "+nt);
            return createBlankNode();
        } else {
            throw new IllegalArgumentException("Bad NT: "+nt);
        }
    }

    @NonNull private static Node parseLiteral(String nt, int ntLen, char last, char quote) {
        StringBuilder lex = new StringBuilder(ntLen + 8);
        int consumed = 1;
        for (int i; consumed < ntLen; consumed = i+2) {
            i = CSUtils.skipUntil(nt, consumed, '\\', quote);
            char c = i < ntLen ? nt.charAt(i) : '\0';
            if (c == '\\') {
                lex.append(nt, consumed, i).append(i+1 < ntLen ? nt.charAt(i+1) : '\\');
            } else {
                lex.append(nt, consumed, Math.min(ntLen, i));
                consumed = i+1;
                break;
            }
        }
        if (consumed+1 < ntLen && nt.charAt(consumed) == '@') {
            String lang = nt.substring(consumed + 1).replace('_', '-');
            return createLiteral(lex.toString(), lang);
        } else if (last == '>') {
            if (!nt.startsWith("^^<", consumed))
                throw new IllegalArgumentException("Bad typed literal "+ nt);
            String uri = nt.substring(consumed + 3, ntLen - 1);
            RDFDatatype dt = TypeMapper.getInstance().getSafeTypeByName(uri);
            return createLiteral(lex.toString(), dt);
        } else if (consumed >= ntLen) {
            return createLiteral(lex.toString());
        } else {
            throw new IllegalArgumentException("Bad literal: "+ nt);
        }
    }
}
