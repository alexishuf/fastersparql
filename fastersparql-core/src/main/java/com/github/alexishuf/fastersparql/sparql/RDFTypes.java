package com.github.alexishuf.fastersparql.sparql;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.client.util.Skip.ALPHANUMERIC;
import static com.github.alexishuf.fastersparql.client.util.Skip.skip;

@SuppressWarnings("unused")
public class RDFTypes {
    public static final String XSD = "http://www.w3.org/2001/XMLSchema#";
    public static final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    public static final String duration = XSD+"duration";
    public static final String dateTime = XSD+"dateTime";
    public static final String time = XSD+"time";
    public static final String date = XSD+"date";
    public static final String gYearMonth = XSD+"gYearMonth";
    public static final String gYear = XSD+"gYear";
    public static final String gMonthDay = XSD+"gMonthDay";
    public static final String gDay = XSD+"gDay";
    public static final String gMonth = XSD+"gMonth";
    public static final String BOOLEAN = XSD+"boolean";
    public static final String base64Binary = XSD+"base64Binary";
    public static final String hexBinary = XSD+"hexBinary";
    public static final String FLOAT = XSD+"float";
    public static final String decimal = XSD+"decimal";
    public static final String DOUBLE = XSD+"double";
    public static final String anyURI = XSD+"anyURI";
    public static final String string = XSD+"string";
    public static final String integer = XSD+"integer";
    public static final String nonPositiveInteger = XSD+"nonPositiveInteger";
    public static final String LONG = XSD+"long";
    public static final String nonNegativeInteger = XSD+"nonNegativeInteger";
    public static final String negativeInteger = XSD+"negativeInteger";
    public static final String INT = XSD+"int";
    public static final String unsignedLong = XSD+"unsignedLong";
    public static final String positiveInteger = XSD+"positiveInteger";
    public static final String SHORT = XSD+"short";
    public static final String unsignedInt = XSD+"unsignedInt";
    public static final String BYTE = XSD+"byte";
    public static final String unsignedShort = XSD+"unsignedShort";
    public static final String unsignedByte = XSD+"unsignedByte";
    public static final String normalizedString = XSD+"normalizedString";
    public static final String token = XSD+"token";
    public static final String language = XSD+"language";
    public static final String langString = RDF+"langString";
    public static final String HTML = RDF+"HTML";
    public static final String XMLLiteral = RDF+"XMLLiteral";

    public static boolean isXsd(String iri, String xsdIri) {
        if (iri.isEmpty()) return false;
        //noinspection StringEquality
        if (iri == xsdIri) return true;
        int hashIdx = XSD.length()-1;
        int offset = hashIdx + (iri.charAt(0) == '<' ? 1 : 0);
        int len = xsdIri.length() - hashIdx;
        return xsdIri.regionMatches(hashIdx, iri, offset, len);
    }

    static final String[] XSD_IRIS, RDF_IRIS; // exposed for tests

    static {
        String[] xsd = {duration, dateTime, time, date, gYearMonth, gYear, gMonthDay, gDay,
                gMonth, BOOLEAN, base64Binary, hexBinary, FLOAT, decimal, DOUBLE, anyURI,
                string, integer, nonPositiveInteger, LONG, nonNegativeInteger, negativeInteger,
                INT, unsignedLong, positiveInteger, SHORT, unsignedInt, BYTE,
                unsignedShort, unsignedByte, normalizedString, token, language};
        String[] rdf = {langString, HTML, XMLLiteral};
        Arrays.sort(xsd);
        Arrays.sort(rdf);
        XSD_IRIS = xsd;
        RDF_IRIS = rdf;
    }

    /**
     * If {@code in.substring(begin, end)} is a {@code "xsd:NAME"} or {@code "rdf:NAME"} where
     * NAME is known to this class, return the complete datatype IRI.
     *
     * @param in String containing the {@code xsd:NAME}/{@code rdf:NAME}
     * @param begin index into {@code in} where the prefixed IRI starts
     * @param end end of {@code xsd:NAME}/{@code rdf:NAME} in {@code in}. Can be {@code in.length()}.
     * @return The full IRI (not enclosed by {@code <>}) equivalent to the prefixed short form in
     *         {@code in.substring(begin, end)} or {@code null} if the local name is not known or
     *         the prefix is not {@code xsd:} nor {@code rdf:}.
     */
    public static String expandPrefixed(String in, int begin, int end) {
        if (begin+6 < end && in.charAt(begin+3) == ':') {
            char c0 = in.charAt(begin), c4 = in.charAt(begin+4);
            if (    (c0=='x' && in.charAt(begin+1)=='s' && in.charAt(begin+2)=='d') ||
                    (c0=='r' && in.charAt(begin+1)=='d' && in.charAt(begin+2)=='f')) {
                String[] candidates;
                int candidatesBegin = 0, candidatesEnd, lnIdx;
                if (c0 == 'x') {
                    candidatesEnd = (candidates = XSD_IRIS).length;
                    lnIdx = XSD.length();
                    // split search space in half
                    int mid = candidatesEnd >> 1;
                    char fst = candidates[mid].charAt(lnIdx);
                    if (c4 < fst) candidatesEnd = mid; else if (c4 > fst) candidatesBegin = mid+1;
                    // save regionMatches calls with a linear search on first char after #
                    for (int i = candidatesBegin; i < candidatesEnd; i++) {
                        char c = candidates[i].charAt(lnIdx);
                        if (c < c4) candidatesBegin = i+1;
                        if (c > c4)   candidatesEnd = i;
                    }
                } else {
                    candidatesEnd = (candidates = RDF_IRIS).length;
                    lnIdx = RDF.length();
                }
                // linear search for #localName
                int lnLen = skip(in, begin+4, end, ALPHANUMERIC)-(begin+4);
                for (int p4 = begin+4, i = candidatesBegin; i < candidatesEnd; i++) {
                    String iri = candidates[i];
                    if (in.regionMatches(p4, iri, lnIdx, lnLen))
                        return iri;
                }
            }
        }
        return null;
    }
}
