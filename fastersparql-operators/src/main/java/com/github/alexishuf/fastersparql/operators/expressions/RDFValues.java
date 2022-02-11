package com.github.alexishuf.fastersparql.operators.expressions;

@SuppressWarnings("unused")
public class RDFValues {
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


    public static final String TRUE = "\"true\"^^<"+BOOLEAN+">";
    public static final String FALSE = "\"false\"^^<"+BOOLEAN+">";
}
