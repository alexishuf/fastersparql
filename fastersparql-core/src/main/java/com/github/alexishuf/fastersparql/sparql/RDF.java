package com.github.alexishuf.fastersparql.sparql;

@SuppressWarnings("unused")
public class RDF {
    public static final String NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String type     = NS+"type";
    public static final String first    = NS+"first";
    public static final String rest     = NS+"rest";
    public static final String nil      = NS+"nil";
    public static final String value    = NS+"value";
    public static final String Property = NS+"Property";

    public static final String type_nt     = "<"+type+">";
    public static final String first_nt    = "<"+first+">";
    public static final String rest_nt     = "<"+rest+">";
    public static final String nil_nt      = "<"+nil+">";
    public static final String value_nt    = "<"+value+">";
    public static final String Property_nt = "<"+Property+">";

    static final String[] RDF_IRIS = { //exposed for tests only
            type, first, rest, nil, value, Property,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#List",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt",
            RDFTypes.JSON,
            RDFTypes.PlainLiteral,
            RDFTypes.HTML,
            RDFTypes.XMLLiteral,
            RDFTypes.langString,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#object",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#language",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#direction"
    };

    public static String fromRdfLocal(String str, int localBegin, int localEnd) {
        int mid = NS.length(), localLen = localEnd-localBegin;
        for (String candidate : RDF_IRIS) { //sorted by frequency
            if (candidate.length() - mid == localLen
                    && candidate.regionMatches(mid, str, localBegin, localLen))
                return candidate;
        }
        return NS + str.substring(localBegin, localEnd);
    }
}
