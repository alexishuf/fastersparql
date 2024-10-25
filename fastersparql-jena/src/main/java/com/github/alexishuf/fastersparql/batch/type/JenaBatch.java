package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.org.apache.jena.datatypes.xsd.XSDDatatype;
import com.github.alexishuf.fastersparql.org.apache.jena.datatypes.xsd.impl.RDFLangString;
import com.github.alexishuf.fastersparql.org.apache.jena.datatypes.xsd.impl.RDFhtml;
import com.github.alexishuf.fastersparql.org.apache.jena.datatypes.xsd.impl.RDFjson;
import com.github.alexishuf.fastersparql.org.apache.jena.datatypes.xsd.impl.XMLLiteralType;
import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;


public class JenaBatch extends ObjBatch<JenaBatch, Node> {

    /* --- --- --- lifecycle --- --- --- */

    public JenaBatch(Node[] arr, int rows, int cols) {super(arr, rows, cols);}

    static final class Concrete extends JenaBatch implements Orphan<JenaBatch> {
        public Concrete(Node[] arr, int rows, int cols) {super(arr, rows, cols);}
        @Override public JenaBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected void schedNodeCleanup(JenaBatch node, Object nodeOwner) {
        JenaBatchCleaner.INSTANCE.sched(node, nodeOwner);
    }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public JenaBatchType type() { return JenaBatchType.JENA; }

    /* --- --- --- term-level accessors --- --- --- */

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        Node node = obj(row, col);
        if (node == null)
            return null;
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            return new FinalTerm(p.shared(), p.localCopy(), p.suffixShared());
        } finally { p.recycle(this); }
    }

    @Override public TermInfo.Type get(@NonNegative int row, @NonNegative int col, TermInfo info) {
        Node node = obj(row, col);
        if (node == null)
            return info.setEmpty();
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            return info.setSharedAndSegment(false, p.shared(),
                    p.localSegment(), p.localUtf8(),
                    p.localOff(), p.localLen(), p.sharedKind());
        } finally { p.recycle(this); }
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        Node node = obj(row, col);
        if (node == null)
            return false;
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            FinalSegmentRope sh = p.shared(), local = p.localCopy();
            boolean suffixSh = p.suffixShared();
            dest. wrapFirst(suffixSh ? local :    sh);
            dest.wrapSecond(suffixSh ? sh    : local);
            return true;
        } finally {p.recycle(this);}
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        Node node = obj(row, col);
        if (node == null)
            return false;
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            dest.wrap(p.shared(), p.localCopy(), p.suffixShared());
            return true;
        } finally {p.recycle(this);}
    }

    @Override
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRopeView dest) {
        Node node = obj(row, col);
        if (node == null)
            return false;
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            dest.wrap(p.localCopy());
            return true;
        } finally { p.recycle(this); }
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        Node node = obj(row, col);
        if (node == null)
            return 0;
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            return p.localLen();
        } finally {p.recycle(this);}
    }

    @Override public Term. @Nullable Type termType(int row, int col) {
        var n = obj(row, col);
        if      (n == null)      return null;
        else if (n.isLiteral())  return Term.Type.LIT;
        else if (n.isURI())      return Term.Type.IRI;
        else if (n.isBlank())    return Term.Type.BLANK;
        else if (n.isVariable()) return Term.Type.VAR;
        else                     throw new UnsupportedOperationException("Unsupported node type");
    }

    @Override public @Nullable Term datatypeTerm(int row, int col) {
        var n = obj(row, col);
        if (n == null || !n.isLiteral())
            return null;
        var jenaDT = n.getLiteralDatatype();
        return switch (jenaDT) {
            case RDFLangString ignored -> Term.RDF_LANGSTRING;
            case RDFjson ignored -> Term.RDF_JSON;
            case RDFhtml ignored -> Term.RDF_HTML;
            case XMLLiteralType ignored -> Term.RDF_XMLLITERAL;
            case XSDDatatype xsd -> {
                if (xsd == XSDDatatype.XSDstring)             yield Term.XSD_STRING;
                if (xsd == XSDDatatype.XSDinteger)            yield Term.XSD_INTEGER;
                if (xsd == XSDDatatype.XSDdecimal)            yield Term.XSD_DECIMAL;
                if (xsd == XSDDatatype.XSDdouble)             yield Term.XSD_DOUBLE;
                if (xsd == XSDDatatype.XSDfloat)              yield Term.XSD_FLOAT;
                if (xsd == XSDDatatype.XSDboolean)            yield Term.XSD_BOOLEAN;
                if (xsd == XSDDatatype.XSDanyURI)             yield Term.XSD_ANYURI;
                if (xsd == XSDDatatype.XSDdate)               yield Term.XSD_DATE;
                if (xsd == XSDDatatype.XSDtime)               yield Term.XSD_TIME;
                if (xsd == XSDDatatype.XSDdateTime)           yield Term.XSD_DATETIME;
                if (xsd == XSDDatatype.XSDduration)           yield Term.XSD_DURATION;
                if (xsd == XSDDatatype.XSDint)                yield Term.XSD_INT;
                if (xsd == XSDDatatype.XSDlong)               yield Term.XSD_LONG;
                if (xsd == XSDDatatype.XSDshort)              yield Term.XSD_SHORT;
                if (xsd == XSDDatatype.XSDbyte)               yield Term.XSD_BYTE;
                if (xsd == XSDDatatype.XSDunsignedByte)       yield Term.XSD_UNSIGNEDBYTE;
                if (xsd == XSDDatatype.XSDunsignedShort)      yield Term.XSD_UNSIGNEDSHORT;
                if (xsd == XSDDatatype.XSDunsignedInt)        yield Term.XSD_UNSIGNEDINT;
                if (xsd == XSDDatatype.XSDunsignedLong)       yield Term.XSD_UNSIGNEDLONG;
                if (xsd == XSDDatatype.XSDnonPositiveInteger) yield Term.XSD_NONPOSITIVEINTEGER;
                if (xsd == XSDDatatype.XSDnonNegativeInteger) yield Term.XSD_NONNEGATIVEINTEGER;
                if (xsd == XSDDatatype.XSDpositiveInteger)    yield Term.XSD_POSITIVEINTEGER;
                if (xsd == XSDDatatype.XSDnegativeInteger)    yield Term.XSD_NEGATIVEINTEGER;
                if (xsd == XSDDatatype.XSDnormalizedString)   yield Term.XSD_NORMALIZEDSTRING;
                if (xsd == XSDDatatype.XSDtoken)              yield Term.XSD_TOKEN;
                if (xsd == XSDDatatype.XSDlanguage)           yield Term.XSD_LANGUAGE;
                if (xsd == XSDDatatype.XSDhexBinary)          yield Term.XSD_HEXBINARY;
                if (xsd == XSDDatatype.XSDbase64Binary)       yield Term.XSD_BASE64BINARY;
                if (xsd == XSDDatatype.XSDgDay)               yield Term.XSD_GDAY;
                if (xsd == XSDDatatype.XSDgMonth)             yield Term.XSD_GMONTH;
                if (xsd == XSDDatatype.XSDgYear)              yield Term.XSD_GYEAR;
                if (xsd == XSDDatatype.XSDgYearMonth)         yield Term.XSD_GYEARMONTH;
                if (xsd == XSDDatatype.XSDgMonthDay)          yield Term.XSD_GMONTHDAY;
                yield Term.valueOf(jenaDT.getURI());
            }
            case null -> null;
            default -> Term.valueOf(jenaDT.getURI());
        };
    }

    @Override public @NonNull FinalSegmentRope shared(@NonNegative int row, @NonNegative int col) {
        Node node = obj(row, col);
        if (node == null)
            return FinalSegmentRope.EMPTY;
        var p = JenaTermParser.create().takeOwnership(this);
        try {
            p.parse(node);
            return p.shared();
        } finally { p.recycle(this); }
    }

    /* --- --- --- mutators --- --- --- */

    @Override protected void putTermConverting(int dstCol, Batch<?> other, int row, int col) {
        putTerm(dstCol, JenaNodeParser.asNode(other, row, col));
    }

    @Override public void putRowConverting(Batch<?> other, int row) {
        short cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        other.requireAlive();
        var parser = JenaNodeParser.create().takeOwnership(this);
        try {
            beginPut();
            for (int c = 0; c < cols; c++)
                putTerm(c, parser.makeNode(other, row, c));
            commitPut();
        } finally { parser.recycle(this); }
    }

    @Override public void putConverting(Batch<?> other) {
        short cols = this.cols, oRows;
        if (other.cols != cols) throw new IllegalArgumentException("cols mismatch");
        var parser = JenaNodeParser.create().takeOwnership(this);
        try {
            for (; other != null; other = other.next) {
                other.requireAlive();
                oRows = other.rows;
                for (int r = 0; r < oRows; r++) {
                    beginPut();
                    for (int c = 0; c < cols; c++)
                        putTerm(c, parser.makeNode(other, r, c));
                    commitPut();
                }
            }
        } finally { parser.recycle(this); }
    }

    @Override public void putTerm(int col, Term t) {
        putTerm(col, JenaNodeParser.asNode(t));
    }
}
