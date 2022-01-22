package com.github.alexishuf.fastersparql.client.parser.results;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static com.github.alexishuf.fastersparql.client.parser.results.JsonParser.Field.*;
import static com.github.alexishuf.fastersparql.client.util.CSUtils.*;
import static java.util.Collections.emptyList;

@Slf4j
public class JsonParser implements ResultsParser {
    private final ResultsParserConsumer consumer;
    private boolean sentError = false, sentEnd = false;
    private boolean varsDone, empty = true;
    private CharSequence input;
    private @MonotonicNonNull StringBuilder inputBuilder;
    private @NonNegative int cursor;
    private @NonNegative long charsBeforeInput;
    private final List<Token> stack = new ArrayList<>(16);
    private boolean pushed = false, isReturning = false;
    private final List<Field> fields = new ArrayList<>(8);
    private final List<String> fieldStrings = new ArrayList<>(8);
    private final List<String> vars = new ArrayList<>();
    private final List<@Nullable String> row = new ArrayList<>();
    private @Nullable String type, datatype, value, lang;

    public JsonParser(ResultsParserConsumer consumer) {
        this.input = "";
        this.consumer = consumer;
        this.stack.add(Token.VALUE);
    }

    @Override public void feed(CharSequence input) {
        if (sentEnd)
            return; // do nothing
        addInput(input);
        try {
            while (hasInput() && !stack.isEmpty()) {
                boolean callResume = isReturning;
                pushed = isReturning = false; // reset stack change flags
                Token token = stack.get(stack.size() - 1);
                if (callResume) {
                    token.resume(this);
                } else {
                    int lastCursor = cursor;
                    token.parse(this);
                    if (!isReturning && !pushed && cursor == lastCursor)
                        break; // if no progress, wait for more input
                }
            }
        } catch (SyntaxException e) {
            notifyError(e);
        }
    }

    @Override public void end() {
        if (sentEnd) return;
        try {
            if (!varsDone) {
                if (empty) {
                    notifyVars(emptyList());
                } else {
                    String msg = "Non-empty JSON without \"head\" and without \"boolean\"";
                    throw new SyntaxException(pos(), msg);
                }
            }
            if (hasInput())
                throw new SyntaxException(pos()+input.length(), "Unexpected end of JSON stream");
        }  catch (SyntaxException e) {
            notifyError(e);
        }
        if (!sentEnd) {
            consumer.end();
            sentEnd = true;
        }
    }

    static class SyntaxException extends Exception {
        public final long pos;
        public SyntaxException(@Positive long pos, String message) {
            super(message);
            this.pos = pos;
        }
        public SyntaxException(@Positive long pos, char got, char... expected) {
            this(pos, "At "+pos+", expected " + (expected.length > 1 ? "one of " : "") +
                    charNames(expected) + ", got " + charName(got) + ".");
        }
        public SyntaxException(@Positive long pos, CharSequence actual, CharSequence expected) {
            this(pos, "At "+pos+", expected "+expected+", got "+actual);
        }
    }

    enum Field {
        HEAD,
        VARS,
        RESULTS,
        BINDINGS,
        TYPE,
        DATATYPE,
        VALUE,
        XMLLANG,
        BOOLEAN,
        UNKNOWN;

        private static final String[] SORTED_JSON_NAMES;
        private static final Field[] SORTED_FIELDS;
        static {
            Field[] fields = values();
            String[] sortedJsonNames = new String[fields.length];
            for (int i = 0; i < sortedJsonNames.length; i++)
                sortedJsonNames[i] = fields[i].name().toLowerCase().replace("xmllang", "xml:lang");
            Arrays.sort(sortedJsonNames);
            Field[] sortedFields = Arrays.copyOf(fields, fields.length);
            for (int i = 0; i < sortedJsonNames.length; i++)
                sortedFields[i] = valueOf(sortedJsonNames[i].toUpperCase().replace(":", ""));
            SORTED_JSON_NAMES = sortedJsonNames;
            SORTED_FIELDS = sortedFields;
        }

        public static @Nullable Field fromJson(CharSequence name) {
            int i = Arrays.binarySearch(SORTED_JSON_NAMES, name.toString());
            return i >= 0 ? SORTED_FIELDS[i] : UNKNOWN;
        }
    }

    enum Token {
        /* --- --- --- JSON interstitial non-terminals --- --- --- */
        VALUE {
            @Override void parse(JsonParser s) throws SyntaxException {
                Token.parseByFirst(s, '\0');
            }
        },
        R_VALUE {
            @Override void parse(JsonParser s) throws SyntaxException {
                Token.parseByFirst(s, ':');
            }
            @Override void resume(JsonParser s) throws SyntaxException {
                if (s.atField(Field.HEAD, Field.VARS)) {
                    s.notifyVars(s.vars);
                } else if (s.atField(RESULTS, BINDINGS, UNKNOWN)) {
                    String var = s.fieldStrings.get(s.fieldStrings.size() - 1);
                    int i = s.vars.indexOf(var);
                    if (i < 0)
                        throw new SyntaxException(s.pos(), var+" not declared in head.vars array");
                    else
                        s.row.set(i, s.takeNT());
                } else if (s.atField(RESULTS, BINDINGS)) {
                    if (!s.sentEnd)
                        s.consumer.end();
                    s.sentEnd = true;
                }
                s.leaveField().pop();
            }
        },
        /* --- --- --- JSON non-terminals with open/close symbols --- --- --- */
        OBJ {
            @Override String first() { return "{"; }
            @Override boolean keepFirst() { return false; }
            @Override void parse(JsonParser s) throws SyntaxException {
                s.cursor = skipSpaceAnd(s.input, s.cursor, ',');
                if (s.readIf('}')) {
                    s.pop();
                } else {
                    CharSequence name = s.readString();
                    if (name != null) {
                        s.empty = false;
                        s.enterField(name).push(R_VALUE);
                    }
                }
            }
        },
        ARRAY {
            @Override String first() { return "["; }
            @Override boolean keepFirst() {return false;}
            @Override void parse(JsonParser s) throws SyntaxException {
                s.cursor = skipSpaceAnd(s.input, s.cursor, ',');
                if (s.readIf(']')) s.pop();
                else                        Token.parseByFirst(s, '\0');
            }

            @Override void resume(JsonParser s) {
                if (s.atField(RESULTS, BINDINGS))
                    s.notifyRow();
            }
        },
        /* --- --- --- JSON primitive values --- --- --- */
        CONSTANT {
            @Override String first() { return "tfn"; }
            @Override void parse(JsonParser s) throws SyntaxException {
                char f = Character.toLowerCase(s.input.charAt(s.cursor));
                if (s.read(f == 't' ? "true" : (f == 'f' ? "false" : "null"))) {
                    if (s.atField(Field.BOOLEAN)) {
                        if (!s.varsDone)
                            s.notifyVars(emptyList());
                        if (f == 't') s.notifyRow();
                    }
                    s.pop();
                }
            }
        },
        NUMBER {
            private final char[] END_CHARS = {',', ']', '}'};
            @Override String first() { return "-0123456789"; }
            @Override void parse(JsonParser s) throws SyntaxException {
                int end = skipUntil(s.input, s.cursor, END_CHARS);
                if (end < s.input.length()) {
                    if (s.atField(Field.BOOLEAN)) {
                        int value = end - s.cursor  == 1 ? s.input.charAt(s.cursor) - '0' : -1;
                        if ((value & ~0x1) != 0) {
                            String msg = "Read \"boolean\": " + s.input.subSequence(s.cursor, end)
                                       + ", which cannot be coerced to a boolean value";
                            throw new SyntaxException(s.pos(), msg);
                        }
                        if (!s.varsDone) s.notifyVars(emptyList());
                        if (value == 1) s.notifyRow();
                    }
                    s.cursor = end;
                    s.pop();
                }
            }
        },
        STRING {
            @Override String first() { return "\""; }
            @Override void parse(JsonParser s) throws SyntaxException {
                if (s.atKnownField()) {
                    CharSequence charSequence = s.readString();
                    if (charSequence != null) {
                        String str = charSequence.toString();
                        if (s.atField(Field.HEAD, Field.VARS)) {
                            s.vars.add(str);
                        } else if (s.atField(RESULTS, BINDINGS, UNKNOWN, Field.TYPE)) {
                            s.type = str;
                        } else if (s.atField(RESULTS, BINDINGS, UNKNOWN, Field.DATATYPE)) {
                            s.datatype = str;
                        } else if (s.atField(RESULTS, BINDINGS, UNKNOWN, Field.VALUE)) {
                            s.value = str;
                        } else if (s.atField(RESULTS, BINDINGS, UNKNOWN, Field.XMLLANG)) {
                            s.lang = str;
                        } else if (s.atField(Field.BOOLEAN)) {
                            if (str.equalsIgnoreCase("true") || str.equals("1")) {
                                if (!s.varsDone) s.notifyVars(emptyList());
                                s.notifyRow();
                            } else if (str.equalsIgnoreCase("false") || str.equals("0")) {
                                if (!s.varsDone) s.notifyVars(emptyList());
                            } else {
                                String msg = "Cannot coerce \"boolean\":"+str+" to boolean";
                                throw new SyntaxException(s.pos(), msg);
                            }
                        }
                        s.pop();
                    }
                } else {
                    int quote = findNotEscaped(s.input, s.cursor+1, '"');
                    if (quote < s.input.length()) {
                        s.cursor = quote + 1;
                        s.pop();
                    }
                }
            }
        };

        abstract void parse(JsonParser s) throws SyntaxException;
        void resume(JsonParser s) throws SyntaxException { }
        boolean keepFirst() { return true; }
        String first() { return ""; }
        private static void parseByFirst(JsonParser s, char skip) throws SyntaxException {
            s.cursor = skipSpaceAnd(s.input, s.cursor, skip);
            if (s.hasInput()) {
                Token nextToken = first2Token.get(s.input.charAt(s.cursor));
                if (nextToken != null)  {
                    if (!nextToken.keepFirst()) ++s.cursor;
                    s.push(nextToken);
                } else {
                    throw new SyntaxException(s.pos(), s.input.charAt(s.cursor), firstChars);
                }
            }
        }

        static final Map<Character, Token> first2Token;
        static final char[] firstChars;

        static {
            Map<Character, Token> char2token = new HashMap<>();
            for (Token token : values()) {
                String first = token.first();
                for (int i = 0, len = first.length(); i < len; i++)
                    char2token.put(first.charAt(i), token);
            }
            first2Token = char2token;
            char[] array = new char[char2token.size()];
            int i = 0;
            for (Character c : char2token.keySet())
                array[i++] = c;
            firstChars = array;
        }
    }

    /* --- ---- ---- consumer notification helpers --- --- --- */

    private void notifyVars(List<String> vars) throws SyntaxException {
        if (varsDone)
            throw new SyntaxException(pos(), "Second head.vars property found");
        consumer.vars(vars);
        for (int i = 0, size = vars.size(); i < size; i++)
            row.add(null);
        varsDone = true;
    }

    private void notifyRow() {
        consumer.row(row.toArray(new String[0]));
        for (int i = 0, size = row.size(); i < size; i++) row.set(i, null);
    }

    private void notifyError(SyntaxException e) {
        if (sentError)
            log.error("Suppressing {} to avoid double {}.onError()", e, consumer, e);
        consumer.onError(e.getMessage());
        sentError = true;
        consumer.end();
        sentEnd = true;
    }

    /* --- ---- ---- parsing utility functions --- --- --- */

    private void addInput(CharSequence input) {
        if (hasInput()) {
            int capacity = this.input.length()-cursor + input.length();
            if (inputBuilder == null) {
                inputBuilder = new StringBuilder(capacity);
            } else {
                if (this.input == inputBuilder) {
                    this.input = this.inputBuilder.toString();
                }
                inputBuilder.setLength(0);
                inputBuilder.ensureCapacity(capacity);
            }
            inputBuilder.append(this.input, cursor, this.input.length());
            this.input = inputBuilder.append(input);
        } else {
            this.input = input;
        }
        this.charsBeforeInput += this.cursor;
        this.cursor = 0;
    }

    private long pos() {
        return charsBeforeInput + cursor;
    }

    private boolean hasInput() { return cursor < input.length(); }

    private boolean readIf(char expected) {
        if (hasInput() && input.charAt(cursor) == expected) {
            ++cursor;
            return true;
        }
        return false;
    }

    private boolean read(String expected) throws SyntaxException {
        int end = cursor + expected.length();
        if (input.length() < end) return false;
        CharSequence actual = input.subSequence(cursor, end);
        if (expected.contentEquals(actual)) {
            cursor = end;
            return true;
        }
        throw new SyntaxException(pos(), actual, expected);
    }

    @Nullable CharSequence readString() throws SyntaxException {
        int open = skipSpaceAnd(input, cursor, ',');
        if (open == input.length()) return null;
        if (input.charAt(open) != '"')
            throw new SyntaxException(charsBeforeInput+open, '"', input.charAt(open));
        int end = findNotEscaped(input, open + 1, '"');
        if (end == input.length())
            return null; // needs more input
        cursor = end + 1;
        return input.subSequence(open + 1, end);
    }

    boolean atKnownField() {
        return !fields.isEmpty() && fields.get(fields.size()-1) != UNKNOWN;
    }

    boolean atField(Field... path) {
        int depth = fields.size();
        if (depth != path.length) return false;
        for (int i = 0; i < depth; i++) {
            if (fields.get(i) != path[i]) return false;
        }
        return true;
    }

    String takeNT() throws SyntaxException {
        if (value == null && !"bnode".equalsIgnoreCase(type) && !"blank".equalsIgnoreCase(type))
            throw new SyntaxException(pos(), "No value set, cannot build NT representation");
        String nt;
        if ("uri".equalsIgnoreCase(type) || "iri".equalsIgnoreCase(type)) {
            nt = "<"+value+">";
        } else if ("bnode".equalsIgnoreCase(type) || "blank".equalsIgnoreCase(type)) {
            if (value == null || value.isEmpty() || value.equals("_:")) {
                value = UUID.randomUUID().toString();
                log.debug("Generated UUID "+value+" for null/empty bnode at "+pos());
            }
            assert value != null;
            return value.startsWith("_:") ? value : "_:"+value;
        } else if ("literal".equalsIgnoreCase(type)
                || (type == null && (datatype != null || lang != null))) {
            if (lang != null && !lang.isEmpty()) {
                nt = "\""+value+"\"@"+lang.replace('_', '-');
            } else if (datatype != null && !datatype.isEmpty()) {
                if (datatype.charAt(0) == '<' && datatype.charAt(datatype.length() - 1) == '>')
                    datatype = datatype.substring(1, datatype.length() - 1);
                if (datatype.startsWith("xsd:"))
                    datatype = "http://www.w3.org/2001/XMLSchema#"+datatype.substring(4);
                else if (datatype.startsWith("rdf:"))
                    datatype = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"+datatype.substring(4);
                nt = "\""+value+"\"^^<"+datatype+">";
            } else {
                nt = "\""+value+"\"";
            }
        } else if (type == null) {
            if (value.startsWith("http://") || value.startsWith("https://"))
                return "<"+value+">";
            else if (value.startsWith("_:"))
                return value;
            else
                return "\""+value+"\"";
        } else {
            throw new SyntaxException(pos(), "Unsupported type=\""+type+"\" for JSON values");
        }
        value = type = datatype = lang = null;
        return nt;
    }

    private void push(Token token) {
        assert !pushed : "after push() control should return to feed()";
        assert !isReturning : "after pop() control should return to feed()";
        stack.add(token);
        pushed = true;
    }

    private void pop() {
        assert !pushed : "after push() control should return to feed()";
        assert !isReturning : "after pop() control should return to feed()";
        stack.remove(stack.size() - 1);
        isReturning = true;
    }

    private JsonParser enterField(CharSequence name) {
        String string = name.toString();
        fields.add(Field.fromJson(string));
        fieldStrings.add(string);
        return this;
    }
    private JsonParser leaveField() {
        int lastIdx = fields.size()-1;
        fields.remove(lastIdx);
        fieldStrings.remove(lastIdx);
        return this;
    }

    void setupForTest(CharSequence input, int cursor, @Nullable String value, @Nullable String type,
                      @Nullable String datatype, @Nullable String lang, List<String> fields) {
        this.input = input;
        this.cursor = cursor;
        this.value = value;
        this.type = type;
        this.datatype = datatype;
        this.lang = lang;
        for (String field : fields)
            enterField(field);
    }
}
