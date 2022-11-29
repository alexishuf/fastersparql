package com.github.alexishuf.fastersparql.sparql;

import java.util.Arrays;

@SuppressWarnings("unused")
public class RDFTypes {
    public static final String XSD = "http://www.w3.org/2001/XMLSchema#";

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
    public static final String langString = RDF.NS+"langString";
    public static final String HTML = RDF.NS+"HTML";
    public static final String XMLLiteral = RDF.NS+"XMLLiteral";
    public static final String JSON = RDF.NS+"JSON";
    public static final String PlainLiteral = RDF.NS+"PlainLiteral";

    static final String[] XSD_IRIS; // exposed for tests

    static {
        String[] xsd = {duration, dateTime, time, date, gYearMonth, gYear, gMonthDay, gDay,
                gMonth, BOOLEAN, base64Binary, hexBinary, FLOAT, decimal, DOUBLE, anyURI,
                string, integer, nonPositiveInteger, LONG, nonNegativeInteger, negativeInteger,
                INT, unsignedLong, positiveInteger, SHORT, unsignedInt, BYTE,
                unsignedShort, unsignedByte, normalizedString, token, language};
        Arrays.sort(xsd);
        XSD_IRIS = xsd;
    }

    public static String fromXsdLocal(String str, int localBegin, int localEnd) {
        if (localEnd == localBegin)
            return XSD;
        char localChar = str.charAt(localBegin);
        int prefixLen = XSD.length();
        int first = 0, last = XSD_IRIS.length-1, localLen = localEnd-localBegin;
        while (last - first > 4) {
            int mid = (first+last) >>> 1;
            char midVal = XSD_IRIS[mid].charAt(prefixLen);
            if (midVal < localChar)
                first = mid+1;
            else if (midVal > localChar)
                last = mid-1;
            else break;
        }
        if (last >= first && first >= 0) {
            // walk left while URI shares the first char of the localName
            while (first > 0 && XSD_IRIS[first-1].charAt(prefixLen) == localChar)
                --first;
            while (first <= last) {
                String candidate = XSD_IRIS[first++];
                if (candidate.length()-prefixLen == localLen
                        && candidate.regionMatches(prefixLen, str, localBegin, localLen))
                    return candidate;
            }
        }
        return XSD + str.substring(localBegin, localEnd);
    }
}
