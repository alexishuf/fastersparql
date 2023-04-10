package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.exceptions.InvalidMediaType;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.regex.Pattern.compile;

@SuppressWarnings("unused")
public final class MediaType {
    private final String type;
    private final String subtype;
    private final Map<String, String> params;
    private final String normalized;

    /* --- --- --- constants --- --- --- */


    /**
     * See <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-2.2">token
     * token in RFC 2616</a>.
     */
    public static final String TOKEN_STR = "[^()<>@,;:\\\\\"/\\[\\]?={}\\t\\n\\r ]+";
    public static final Pattern TOKEN = compile(TOKEN_STR);
    private static final Pattern TYPE = compile("^\\s*("+ TOKEN_STR+")");
    private static final Pattern SUBTYPE = compile("^\\s*/?\\s*("+TOKEN_STR+")");
    private static final String QUOTED_VAL = "[^\"\\\\]|\\\\[\"\\\\]";
    private static final Pattern QUOTED = compile("\"("+QUOTED_VAL+")*\"");
    private static final Pattern MEDIA_TYPE_PARAM = compile(
            "\\s*;\\s*("+ TOKEN_STR+")\\s*=\\s*("+TOKEN_STR+"|\"(?:"+QUOTED_VAL+")*\")"
    );
    private static final Pattern RESIDUAL = compile("\\s*[;,]?\\s*");

    /* --- --- --- builder --- --- --- */

    @SuppressWarnings("unused")
    public static final class Builder {
        private String type;
        private String subtype;
        private Map<String, String> params = null;

        public Builder(String type, String subtype) {
            this.type = type;
            this.subtype = subtype;
        }

        public Builder    type(String value)              {    type = value; return this; }
        public Builder subtype(String value)              { subtype = value; return this; }
        public Builder  params(Map<String, String> value) {  params = value; return this; }

        public Builder param(String name, String value) {
            (params == null ? params = new HashMap<>() : params).put(name, value);
            return this;
        }

        public MediaType build() {
            return new MediaType(type, subtype, params);
        }
    }

    public static Builder builder(String type, String subtype) {return new Builder(type, subtype);}

    public Builder toBuilder() { return new Builder(type, subtype); }

    public MediaType withoutParams(Collection<String> forbidden) {
        if (params.isEmpty())
            return this;
        HashMap<String, String> copy = new HashMap<>(params);
        for (String name : forbidden) copy.remove(name);
        return new MediaType(type, subtype, copy);
    }

    public MediaType withoutParams() {
        return params.isEmpty() ? this : new MediaType(type, subtype, emptyMap());
    }

    /* --- --- --- constructors --- --- --- */

    private MediaType(String type, String subtype, Map<String, String> params,
                      String normalized) {
        assert TOKEN.matcher(type).matches();
        assert TOKEN.matcher(subtype).matches();
        assert params.keySet().stream().allMatch(Objects::nonNull);
        assert params.keySet().stream().noneMatch(String::isEmpty);
        assert params.values().stream().allMatch(Objects::nonNull);
        assert params.values().stream().noneMatch(String::isEmpty);
        assert params.keySet().stream().map(TOKEN::matcher).allMatch(Matcher::matches);

        this.type = type;
        this.subtype = subtype;
        this.params = params.isEmpty() ? emptyMap() : unmodifiableMap(params);
        this.normalized = normalized;
    }

    public MediaType(String type, String subtype) throws FSInvalidArgument {
        this(type, subtype, null);
    }

    public MediaType(String type, String subtype, @Nullable Map<String, String> params)
            throws FSInvalidArgument {
        this.type = checkToken(type.trim().toLowerCase(), "Type");
        this.subtype = checkToken(subtype.trim().toLowerCase(), "Subtype");
        if (params != null && !params.isEmpty()) {
            Map<String, String> sanitized = new HashMap<>(params);
            for (Map.Entry<String, String> e : params.entrySet()) {
                String rawK = e.getKey(), rawV = e.getValue();
                String k = checkToken(e.getKey().trim().toLowerCase(), "Parameter name");
                String v = e.getValue();
                String trimmedV = v.trim();
                if (TOKEN.matcher(trimmedV).matches()) {
                    v = trimmedV.toLowerCase();
                } else {
                    Matcher quotedM = QUOTED.matcher(v);
                    v = quotedM.matches() ? unquote(quotedM.group(1)) : v;
                }
                //noinspection StringEquality
                boolean changeK = k != rawK, changeV = v != rawV;
                if (changeK)            sanitized.remove(rawK);
                if (changeK || changeV) sanitized.put(k, v);
            }
            this.params = unmodifiableMap(sanitized);
        } else {
            this.params = emptyMap();
        }
        this.normalized = normalize(this.type, this.subtype, this.params);
    }

    /**
     * Sanitizes and parses a media type according to the {@code media-type} production in
     * <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-3.7">RFC2616</a>
     *
     * <p>Sanitization:</p>
     * <ul>
     *     <li>Whitespace at the begin and end of the string is removed</li>
     *     <li>Type, subtype and parameter names are converted to lower-case</li>
     *     <li>Unquoted parameter values are converted to lower-case</li>
     * </ul>
     *
     * The parse operation discard the specific whitespace used, so that two media-types
     * with different whitespace (except for quoted parameter values) will compare
     * as {@link MediaType#equals(Object)} and will have the same {@link MediaType#toString()}
     * and {@link MediaType#normalized()} string.
     *
     * @param mediaType the media-type to parse. Can be null or empty.
     * @return A new valid {@link MediaType} or null if {@code mediaType} is null or an empty string.
     * @throws InvalidMediaType If {@code mediaType} is non-empty and does not represent
     *                          a single valid media-type.
     */
    public static MediaType
    parse(CharSequence mediaType) throws InvalidMediaType {
        if (mediaType == null)
            throw new InvalidMediaType("null mediaType");
        int begin, length = mediaType.length();
        if (length == 0)
            throw new InvalidMediaType("Empty media type");
        Matcher typeM = TYPE.matcher(mediaType);
        if (!typeM.find())
            throw new InvalidMediaType("No type in \""+mediaType+"\"");
        begin = typeM.end();

        Matcher subtypeM = SUBTYPE.matcher(mediaType.subSequence(begin, length));
        if (!subtypeM.find())
            throw new InvalidMediaType("No subtype in \""+mediaType+"\"");
        begin += subtypeM.end();

        Matcher m = MEDIA_TYPE_PARAM.matcher(mediaType).region(begin, length);
        boolean ok = m.find();
        Map<String, String> params = ok ? new HashMap<>() : emptyMap();
        try {
            for (; ok; ok = m.find()) {
                begin += m.end();
                String name = checkToken(m.group(1), "Parameter name");
                params.put(name, paramValue(mediaType, m.start(2), m.end(2)));
            }
        } catch (FSInvalidArgument e) {
            throw new InvalidMediaType(e.getMessage());
        }

        if (begin < length) {
            CharSequence tail = mediaType.subSequence(begin, length);
            if (!RESIDUAL.matcher(tail).matches())
                throw new InvalidMediaType("Unexpected leftover after media type: \""+tail+"\"");

        }

        String type = typeM.group(1).toLowerCase(), subtype = subtypeM.group(1).toLowerCase();
        String normalized = normalize(type, subtype, params);
        return new MediaType(type, subtype, params, normalized);
    }

    /**
     * Calls {@link MediaType#parse(CharSequence)} but returns null on error instead of throwing.
     *
     * @param string the media type string to parse
     * @return A valid {@link MediaType} or null if string is null or invalid.
     */
    public static @Nullable MediaType tryParse(@Nullable CharSequence string) {
        try {
            return parse(string);
        } catch (InvalidMediaType e) {
            return null;
        }
    }

    /* --- --- --- trivial getters --- --- --- */

    /**
     * The non-null and non-empty type in {@code type/subtype; p1=v1; ...}.
     *
     * <p>The constructor ensures this conforms to the
     * <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-2.2">token production
     * in RFC2616</a>.</p>
     */
    public String type() { return type; }


    /**
     * The non-null and non-empty subtype in {@code type/subtype; p1=v1; ...}.
     *
     * <p>The constructor ensures this conforms to the
     * <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-2.2">token production
     * in RFC2616</a>.</p>
     */
    public String subtype() { return subtype; }


    /**
     * A map from non-null and non-empty parameter names (e.g., {@code p1}) to non-null and
     * non-empty parameter values (e.g., {@code} v1) in the media type
     * (e.g., {@code type/subtype; p1=v1; ...}).
     *
     * <p>The constructor ensures parameter names conform to the {@code token} production  and values
     * conform either to the {@code token} or {@code quoted-string} productions in
     * <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-2.2">RFC2616</a></p>
     */
    public Map<String, String> params() { return params; }

    /** Equivalent to {@code params().isEmpty()} */
    public boolean noParams() { return params.isEmpty(); }

    /**
     * A normalized view of the media type:
     *
     * <ul>
     *     <li>All whitespace sequences are replaced with a single space character</li>
     *     <li>The param separator string is {@code "; "}</li>
     *     <li>The string is lower-case</li>
     *     <li>The string is trimmed</li>
     * </ul>
     */
    public String normalized() { return normalized; }

    /* --- --- --- methods --- --- --- */

    /**
     * This {@link MediaType} accepts offer iff all the following conditions hold:
     *
     * <ul>
     *     <li>{@code offer} is not {@code null}.</li>
     *     <li>{@code this} and {@code offer} have the same type</li>
     *     <li>{@code this} and {@code offer} have the same subtype</li>
     *     <li>For each parameter in {@code this}, {@code offer} has
     *         the same parameter with same value.</li>
     * </ul>
     *
     * @param offer The offered {@link MediaType}. Can be null
     * @return true iff {@code offer != null} and offer describes a media-type that is also
     *         described by this {@link MediaType}.
     */
    public boolean accepts(MediaType offer) {
        if (offer == null)
            return false;
        if (!type.equals("*") && !type.equals(offer.type))
            return false;
        if (!subtype.equals("*") && !subtype.equals(offer.subtype))
            return false;
        for (Map.Entry<String, String> e : params.entrySet()) {
            String offerValue = offer.params.getOrDefault(e.getKey(), null);
            if (offerValue != null && !offerValue.equals(e.getValue()))
                return false;
        }
        return true;
    }

    public record AcceptedBy(@NonNegative int q, MediaType withParams) { }

    /**
     * If {@code acceptedBy(tryParse(str.substring(begin, end)))} returns a q-str
     * {@code >= 0} and {@code <= 1_000}. Else {@code -1}.
     *
     * @param str a string containing a media type
     * @param begin index where a media type spec starts within {@code str}
     * @param end index where the media type spec ends ({@code str.length()} or first
     *            index after the spec.
     * @return {@code 1_000*q} where {@code q} is the str in {@code [0, 1]} set as a parameter
     *         in the media type spec, if the spec accepts {@code this}. Else return {@code -1} to
     *         signal the spec does not accept {@code this} as an offer.
     */
    public AcceptedBy acceptedBy(String str, int begin, int end) {
        Matcher m = acceptedRx().matcher(str).region(begin, end);
        if (!m.find())
            return null; // type/subtype did not match
        if (m.end() >= end)
            return new AcceptedBy(1_000, this); // matched, no params, q=1 is the default
        int q = 1_000;
        m = MEDIA_TYPE_PARAM.matcher(str).region(m.end()-1, end);
        Map<String, String> params = null;
        while (m.find()) {
            int b = m.start(1), e = m.end(1);
            char c;
            if (e-b == 1 && ((c = str.charAt(b)) == 'q' || c == 'Q')) {
                q = qValue(str, m.start(2), m.end(2));
            } else {
                if (params == null) params = new HashMap<>(this.params);
                String key = str.substring(b, e);
                String value = paramValue(str, m.start(2), m.end(2));
                String offer = this.params.getOrDefault(key, null);
                if (offer != null && !value.equals(offer))
                    return null; // conflicting parameter
                params.put(key, value);
            }
        }
        if (params == null)
            return new AcceptedBy(q, this);
        String normalized = normalize(type, subtype, params);
        return new AcceptedBy(q, new MediaType(type, subtype, params, normalized));
    }

    /**
     * Equivalent to {@code request.accepts(this)}.
     *
     * @param request the request {@link MediaType}. Can be null
     * @return true iff {@code request} is null or if {@code request.accepts(this)}.
     */
    public boolean acceptedBy(MediaType request) {
        return request == null || request.accepts(this);
    }

    public Charset charset(Charset fallback) {
        String name = params.getOrDefault("charset", fallback == null ? null : fallback.name());
        if (name == null)
            return null;
        return Charset.forName(name);
    }

    /* --- --- --- java.lang.Object methods --- --- --- */

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MediaType mediaType)) return false;
        return type.equals(mediaType.type) && subtype.equals(mediaType.subtype)
                                           && params.equals(mediaType.params);
    }

    @Override public int hashCode() {
        return Objects.hash(type, subtype, params);
    }

    @Override public String toString() { return normalized; }


    /* --- --- --- implementation details --- --- --- */

    private void addComponentToRx(String component, StringBuilder rx) {
        if (component.charAt(0) == '*')
            rx.append("\\*");
        else
            rx.append("(?:\\*|").append(component.replace("+", "\\+")).append(')');
    }


    private @MonotonicNonNull Pattern acceptedRx;
    private Pattern acceptedRx() {
        Pattern rx = acceptedRx;
        if (rx == null) {
            var sb = new StringBuilder(40+type.length()+subtype.length());
            sb.append("^\\s*");
            addComponentToRx(type, sb);
            sb.append('/');
            addComponentToRx(subtype, sb);
            sb.append("(?:\\s|[,;]|$)");
            acceptedRx = rx = compile(sb.toString());
        }
        return rx;
    }

    private static String checkToken(@Nullable String token, String role) {
        token = (token == null ? "" : token.trim().toLowerCase());
        if (token.isEmpty())
            throw new FSInvalidArgument(role+" cannot be null/empty");
        else if (!TOKEN.matcher(token).matches())
            throw new FSInvalidArgument("Invalid "+role+": "+token);
        return token;
    }

    private static String normalize(String type, String subtype, Map<String, String> params) {
        if (params == null || params.isEmpty())
            return type+"/"+subtype;
        int capacity = type.length() + 1 + subtype.length() + params.size() * 16;
        StringBuilder b = new StringBuilder(capacity);
        b.append(type).append('/').append(subtype);
        for (Map.Entry<String, String> e : params.entrySet()) {
            b.append("; ").append(e.getKey()).append('=').append(quote(e.getValue()));
        }
        return b.toString();
    }

    static int qValue(String s, int b, int e) {
        if (e <= b) return -1;
        char c = s.charAt(b);
        if (c == '"') {
            if  (++b >= e) return -1;
            else                 c = s.charAt(b);
        }
        int q = 0;
        switch (c) {
            case '0' -> {
                if (++b >= e) return 0;
                if      ((c = s.charAt(b++)) == '"' || b >= e) return 0;
                else if ( c                  != '.'          ) return -1;
                c = s.charAt(b);
            }
            case '1' -> { return 1_000; }
            default  -> { return -1;    }
        }
        if (          c              >= '0' && c <= '9') q += 100 * (s.charAt(b++)-'0');
        if (b < e && (c=s.charAt(b)) >= '0' && c <= '9') q +=  10 * (s.charAt(b++)-'0');
        if (b < e && (c=s.charAt(b)) >= '0' && c <= '9') q +=        s.charAt(b  )-'0' ;
        return q;
    }

    static String paramValue(CharSequence string, int begin, int end) {
        if (string.charAt(begin) == '"') {
            return unquote(string.subSequence(begin+1, end-1).toString());
        } else {
            var token = string.subSequence(begin, end).toString().toLowerCase();
            return checkToken(token, "Parameter value");
        }
    }

    static String quote(CharSequence cs) {
        if (TOKEN.matcher(cs).matches())
            return cs.toString();
        StringBuilder b = new StringBuilder(cs.length() + 16);
        b.append('"');
        for (int i = 0, c, len = cs.length(); i < len; ++i) {
            c = cs.charAt(i);
            if (c == '"' || c == '\\') b.append('\\');
            b.append((char)c);
        }
        return b.append('"').toString();
    }

    static String unquote(String string) {
        int i = string.indexOf('\\');
        if (i < 0)
            return string;
        StringBuilder b = new StringBuilder(string.length() + 16);
        b.append(string, 0, i).append(string.charAt(i+1));
        for (int begin = i += 2, len = string.length(); i < len; begin = ++i) {
            if ((i = string.indexOf('\\', i)) < 0)
                i = len;
            b.append(string, begin, i);
            if (++i < len)
                b.append(string.charAt(i));
        }
        return b.toString();
    }
}