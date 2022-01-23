package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.HeaderUtils;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.checkerframework.common.value.qual.MinLen;

import java.util.*;

import static java.lang.String.format;

/**
 * Configuration that can be applied to a set or to one SPARQL query but is orthogonal
 * to the query itself.
 *
 * To improve interaction with other code and avoid needless object creation, fields in
 * this class carry mostly validated Strings in Lists and Maps.
 *
 * At construction time, inputs are sanitized and validated. The sanitizations and
 * constraints are documented on each field accessor. Users of this class should keep in
 * mind the sanitizations, which avoid the need to handle all styles allowed by HTTP and URIs.
 *
 * <strong>Overlay semantics:</strong>
 * {@link SparqlConfiguration}s are meant to overlay one another. The specific overlay semantic
 * is documented on each field accessor. The
 * {@link SparqlConfiguration#overlayWith(SparqlConfiguration)} method provides an
 * implementation of such semantics.
 */
@Builder(toBuilder = true)
@Value @Accessors(fluent = true)
public class SparqlConfiguration {
    public static final SparqlConfiguration EMPTY = SparqlConfiguration.builder().build();

    /**
     * Non-null, non-empty, immutable distinct list of preferred SPARQL protocol methods.
     *
     * <ul>
     *     <li>Deduplication (only the earliest item remain)</li>
     *     <li>Nulls items will be removed, so long the resulting list remains non-empty</li>
     *     <li>Null lists are replaced with empty lists</li>
     *     <li>Empty lists are replaced with {@link SparqlMethod#VALUES}</li>
     * </ul>
     *
     * <strong>Overlay semantics</strong>: If {@link SparqlConfiguration#hasMethods()} the overlay
     * result will be the intersection between this list and the lower-precedence list, with the
     * order of elements in this list being preserved. Else, the lower-precedence list
     * remains effective.
     */
    @Singular @MinLen(1) List<SparqlMethod> methods;

    /**
     * Non-null, non-empty, immutable, distinct list of non-null SPARQL result formats
     * ordered from most to least-preferred.
     *
     * These result formats will be applied to {@code SELECT} and {@code ASK} queries.
     *
     * <strong>Pre-validation sanitization at construction time</strong>:
     * <ul>
     *     <li>Deduplication (only the earliest item remain)</li>
     *     <li>Nulls items will be removed, so long the resulting list remains non-empty</li>
     *     <li>Null lists are converted to empty lists</li>
     *     <li>Empty lists are converted to a {@link SparqlResultFormat#VALUES}</li>
     * </ul>
     *
     * <strong>Overlay semantics</strong>: If {@link SparqlConfiguration#hasResultsAccepts()} the
     * overlay result will be the intersection between this list and the lower-precedence list,
     * with the order of elements in this list being preserved. Else, the lower-precedence list
     * remains effective.
     */
    @Singular @MinLen(1) List<SparqlResultFormat> resultsAccepts;

    /**
     * Non-null, non-empty, distinct immutable list of accepted RDF media types
     * ordered from most preferred to least preferred and not including q-values.
     *
     * <strong>Sanitization at construction time</strong>:
     * <ul>
     *     <li>{@code null} is converted to an empty list.</li>
     *     <li>An empty list is converted to {@link RDFMediaTypes#DEFAULT_ACCEPTS}</li>
     * </ul>
     *
     * <strong>Overlay semantics</strong>: If {@link SparqlConfiguration#hasRdfAccepts()} the
     * overlay result will be the intersection between this list and the lower-precedence list,
     * with the order of elements in this list being preserved. Else, the lower-precedence list
     * remains effective.
     */
    @Singular @MinLen(1) List<MediaType> rdfAccepts;

    /**
     * An immutable non-null map from non-null (but possibly empty) param names to non-null
     * possibly empty immutable Lists of non-null, possibly empty, percent-escaped param values.
     *
     * <strong>Pre-validation sanitizations at construction time</strong>:
     * <ul>
     *     <li>A name will be percent-escaped if it contains any forbidden character. If it
     *         also contains valid percent-escapes, the {@code %} in these escapes will
     *         nevertheless be replaced with {@code %25}.
     *     </li>
     *     <li>A name-list pair will be removed if name is null and the list is null,
     *         empty or contains only nulls
     *     </li>
     *     <li>Values lists:
     *         <ul>
     *             <li>Null lists will be replaced with an empty list</li>
     *             <li>null items will be removed from lists, unless the list would become empty</li>
     *             <li>List items (values) will be percent-escaped if containing forbidden
     *                 characters. The same caveat for names also applies.</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * <strong>Overlay semantics</strong>: A param name mapped to an empty list removes any
     * binding of that param made in lower-precedence {@link SparqlConfiguration}s. Else, values
     * in a list will be sent after the values set in lower-precedence {@link SparqlConfiguration}s.
     * Each name-value pair will generate a name=value query param in the URI or form request body.
     */
    @Singular Map<String, List<String>> params;

    /**
     * An immutable, non-null map from non-null, non-empty, trimmed, lower-case header names to
     * non-null, non-empty and trimmed values.
     *
     * Header names conform to the
     * <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-2.2">{@code token}
     * production in RFC2616</a>
     *
     * <strong>Value constraints:</strong>
     * <ul>
     *     <li>Non-null</li>
     *     <li>Non-empty</li>
     *     <li>Trimmed</li>
     * </ul>
     *
     * <strong>Pre-validation sanitizations at construction time</strong>:
     * <ul>
     *     <li>Names sanitization:
     *         <ul>
     *             <li>Remove nulls, if their value is also null or an empty string</li>
     *             <li>Remove trailing ':'</li>
     *             <li>Trimming</li>
     *             <li>Lower-case conversion</li>
     *         </ul>
     *     </li>
     *     <li>Value sanitization:
     *         <ul>
     *             <li>{@code} null replaced with an empty String</li>
     *             <li>Trimming</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * <strong>Overlay semantics</strong>: Mapping a header to an empty string removes any
     * value set by a lower-precedence {@link SparqlConfiguration}. Setting a header to a
     * non-empty string make the header take that value, discarding previously set values in
     * lower-precedence {@link SparqlConfiguration}. This semantics overrides even values set
     * in {@link SparqlConfiguration#appendHeaders()} for lower-precedence
     * {@link SparqlConfiguration}. Setting a header in both here
     * in {@link SparqlConfiguration#appendHeaders()} for the same {@link SparqlConfiguration}
     * is an error.
     */
    @Singular Map<String, String> headers;

    /**
     * An immutable, non-null map from non-null, non-empty, trimmed, lower-case header names to
     * a non-null, immutable, possibly empty list of additional non-null, non-empty, trimmed
     * header values.
     *
     * <strong>Pre-validation sanitizations at construction time</strong>:
     * <ul>
     *     <li>Names sanitization:
     *         <ul>
     *             <li>Remove nulls, if their value is also null or an empty list or a list of nulls</li>
     *             <li>Remove trailing ':'</li>
     *             <li>Trimming</li>
     *             <li>Lower-case conversion</li>
     *         </ul>
     *     </li>
     *     <li>Value lists sanitization:
     *         <li>{@code} null replaced with an empty List</li>
     *         <li>Item sanitizations:
     *             <ul>
     *                 <li>Trimming</li>
     *                 <li>Null, empty or whitespace items are removed unless the
     *                     resulting list would be empty</li>
     *             </ul>
     *         </li>
     *     </li>
     * </ul>
     *
     * <strong>Overlay semantics</strong>: Mapping a header to an empty list will remove
     * all values mapped by lower precedence {@link SparqlConfiguration}s even if those values
     * where set in {@link SparqlConfiguration#headers()}. After an overlay, header/value pairs
     * of higher precendece {@link SparqlConfiguration} appear before those of lower-precedence.
     * For the same {@link SparqlConfiguration} instance, setting a header here and in
     * {@link SparqlConfiguration#headers()} is an error
     */
    @Singular Map<String, List<String>> appendHeaders;

    private SparqlConfiguration(@Nullable List<@Nullable SparqlMethod> methods,
                                @Nullable List<@Nullable SparqlResultFormat> resultsAccepts,
                                @Nullable List<@Nullable MediaType> rdfAccepts,
                                @Nullable Map<String, @Nullable List<@Nullable String>> params,
                                @Nullable Map<String, @Nullable String> headers,
                                @Nullable Map<String, @Nullable List<@Nullable String>> appendHeaders) {
        this.methods = nonEmptyNonNullDistinct(methods, SparqlMethod.VALUES,
                                               "method");
        this.resultsAccepts = nonEmptyNonNullDistinct(resultsAccepts, SparqlResultFormat.VALUES,
                                                      "accepted results format");
        this.rdfAccepts = nonEmptyNonNullDistinct(rdfAccepts, RDFMediaTypes.DEFAULT_ACCEPTS,
                                                  "accepted RDF media types");
        this.params = sanitizeParams(params);
        this.headers = sanitizeHeaders(headers);
        this.appendHeaders = sanitizeAppendHeaders(appendHeaders);

        List<String> bad = null;
        for (String header : this.headers.keySet()) {
            if (this.appendHeaders.containsKey(header))
                (bad == null ? bad = new ArrayList<>() : bad).add(header);
        }
        if (bad != null) {
            String msg = "These headers appear both in headers and appendHeaders: " + bad;
            throw new SparqlClientInvalidArgument(msg);
        }
    }


    /**
     * Whether {@link SparqlConfiguration#methods()} was set on construction.
     */
    boolean hasMethods() { return methods != SparqlMethod.VALUES; }

    /**
     * Whether {@link SparqlConfiguration#resultsAccepts()} was set on construction.
     */
    boolean hasResultsAccepts() { return resultsAccepts != SparqlResultFormat.VALUES; }

    /**
     * Whether {@link SparqlConfiguration#rdfAccepts()} was set on construction.
     */
    boolean hasRdfAccepts() { return rdfAccepts != RDFMediaTypes.DEFAULT_ACCEPTS; }

    /**
     * Tests if this objects set no configuration (i.e., it is a no-op).
     *
     * @return true iff no configuration is set (all fields are null lists or empty maps).
     */
    public boolean isEmpty() {
        return this == EMPTY || (
                methods.isEmpty() && resultsAccepts.isEmpty() && rdfAccepts.isEmpty() &&
                params.isEmpty() && headers.isEmpty() && appendHeaders.isEmpty()
        );
    }

    /**
     * null-tolerant {@link java.util.function.BiFunction} version of
     * {@link SparqlConfiguration#overlayWith(SparqlConfiguration)}.
     *
     * @param lower the lower-precedence {@link SparqlConfiguration}. Can be null.
     * @param higher the higher-precedence {@link SparqlConfiguration}. Can be null.
     * @return a non-null {@link SparqlConfiguration} representing the overlay of {@code lower}
     *         with {@code higher}. If just one of them is null, return the non-null argument.
     *         If both are null, return an empty {@link SparqlConfiguration}.
     */
    public static SparqlConfiguration overlay(@Nullable SparqlConfiguration lower,
                                              @Nullable SparqlConfiguration higher) {
        if (higher == null) {
            return lower == null ? SparqlConfiguration.builder().build() : lower;
        } else if (lower == null) {
            return higher;
        } else {
            return lower.overlayWith(higher);
        }
    }

    /**
     * Version of {@link SparqlConfiguration#overlayWith(SparqlConfiguration)} but with
     * reversed precedence: the argument is overlaid with values of this
     * {@link SparqlConfiguration}.
     *
     * @param lower lower-precedence {@link SparqlConfiguration}. If null, returns {@code this}.
     * @return A new {@link SparqlConfiguration} with values in {@code lower} overlaid
     *         with values in {@code this}.
     */
    public SparqlConfiguration overlayAbove(@Nullable SparqlConfiguration lower) {
        if (lower == null)
            return this;
        return lower.overlayWith(this);
    }

    /**
     * Creates a new {@link SparqlConfiguration} replacing (or appending, for
     * {@link SparqlConfiguration#appendHeaders()} and {@link SparqlConfiguration#params()}) values
     * of {@code this} with values from {@code higher}. The overlay rules are specific to
     * each {@link SparqlConfiguration} field.
     *
     * @param higher higher-precedence {@link SparqlConfiguration}. If null, will return {@code this}.
     * @return a new {@link SparqlConfiguration} with values in {@code this} overlaid with values
     *         from {@code higher}.
     * @throws UnacceptableSparqlConfiguration if {@code !higher.accepts(this)}, i.e., the
     *         resulting overlay would have an empty {@link SparqlConfiguration#methods()},
     *         {@link SparqlConfiguration#resultsAccepts()} or
     *         {@link SparqlConfiguration#rdfAccepts()}.
     */
    public SparqlConfiguration overlayWith(@Nullable SparqlConfiguration higher)
            throws UnacceptableSparqlConfiguration {
        if (higher == null)
            return this;

        SparqlConfigurationBuilder b = toBuilder();
        if (higher.hasMethods())
            b.clearMethods().methods(overlay(methods, higher.methods, higher));
        if (higher.hasResultsAccepts()) {
            b.clearResultsAccepts();
            b.resultsAccepts(overlay(resultsAccepts, higher.resultsAccepts, higher));
        }
        if (higher.hasRdfAccepts())
            b.clearRdfAccepts().rdfAccepts(overlayTypes(rdfAccepts, higher.rdfAccepts, higher));
        overlayParams(higher, b);
        overlayHeaders(higher, b);
        return b.build();
    }

    /**
     * Tests if this configuration satisfies the requests represented by {@code req}'s
     * {@link SparqlConfiguration#methods()}, {@link SparqlConfiguration#resultsAccepts()} and
     * {@link SparqlConfiguration#rdfAccepts()}.
     *
     * <strong>Headers and parameters are ignored</strong>.
     *
     * Null lists in {@code this} are interpreted as "anything is offered/supported", while null
     * lists in {@code req} are interpreted as "anything is accepted".
     *
     * For {@link SparqlConfiguration#methods()} and
     * {@link SparqlConfiguration#resultsAccepts()} acceptance is defined by a non-empty
     * intersection. For {@link SparqlConfiguration#rdfAccepts()} some value in {@code this}
     * must be {@link MediaType#acceptedBy(MediaType)} some value in {@code req}.
     *
     * @param req The {@link SparqlConfiguration} which will be interpreted to a request,
     *                which may or may not be allowed by {@code this}, which represents what
     *                is supported by a SPARQL endpoint.
     * @return {@code true} iff req is not null and the constraints imposed by {@code this},
     *         if any, are accepted by the contraints imposed by {@code req}, if any.
     */
    public boolean isAcceptedBy(@Nullable SparqlConfiguration req) {
        if (req == null)
            return true;
        if (noIntersection(methods, req.methods))               return false;
        if (noIntersection(resultsAccepts, req.resultsAccepts)) return false;
        if (rdfAccepts.isEmpty() || req.rdfAccepts.isEmpty())   return true;
        for (MediaType reqMT : req.rdfAccepts) {
            for (MediaType offerMT : rdfAccepts) {
                if (reqMT.accepts(offerMT))
                    return true;
            }
        }
        return false;
    }

    /**
     * Null-safe equivalent to {@code offer.isAcceptedBy(this}.
     *
     * @param offer Supported configuration.
     * @return true if {@code offer} is null or
     *         {@link SparqlConfiguration#isAcceptedBy(SparqlConfiguration)} {@code this}.
     */
    public boolean accepts(@Nullable SparqlConfiguration offer) {
        return offer == null || offer.isAcceptedBy(this);
    }


    /* --- --- --- implementation details --- --- --- */

    private static <T> boolean noIntersection(List<T> a, List<T> b) {
        boolean has = false;
        for (T o : b) {
            has = a.contains(o);
            if (has) break;
        }
        return !has;
    }

    private <T> List<T> overlay(List<T> lower, List<T> higher,
                                SparqlConfiguration higherConfiguration) {
        ArrayList<T> intersection = new ArrayList<>(Math.min(lower.size(), higher.size()));
        for (T o : higher) {
            if (lower.contains(o)) intersection.add(o);
        }
        if (intersection.isEmpty())
            throw new UnacceptableSparqlConfiguration(this, higherConfiguration);
        return intersection;
    }

    private List<MediaType> overlayTypes(List<MediaType> lower, List<MediaType> higher,
                                         SparqlConfiguration higherConfiguration) {
        ArrayList<MediaType> intersection = new ArrayList<>(Math.min(lower.size(), higher.size()));
        for (MediaType request : higher) {
            for (MediaType offer : lower) {
                if (request.accepts(offer)) {
                    intersection.add(request);
                    break;
                }
            }
        }
        if (intersection.isEmpty())
            throw new UnacceptableSparqlConfiguration(this, higherConfiguration);
        return intersection;
    }

    private void overlayHeaders(@NonNull SparqlConfiguration higher,
                                SparqlConfigurationBuilder b) {
        Map<String, String> headers = new HashMap<>(this.headers);
        Map<String, List<String>> appendHeaders = new HashMap<>(this.appendHeaders);
        for (Map.Entry<String, String> e : higher.headers.entrySet()) {
            String name = e.getKey();
            appendHeaders.remove(name);
            headers.put(name, e.getValue());
        }
        for (Map.Entry<String, List<String>> e : higher.appendHeaders.entrySet()) {
            String name = e.getKey();
            headers.remove(name);
            List<String> values = e.getValue();
            if (!values.isEmpty()) {
                List<String> mine = this.appendHeaders.getOrDefault(name, null);
                if (mine != null && !mine.isEmpty()) {
                    ArrayList<String> copy = new ArrayList<>(mine.size() + values.size());
                    copy.addAll(values);
                    copy.addAll(mine);
                    values = Collections.unmodifiableList(copy);
                }
            }
            appendHeaders.put(name, values);
        }
        b.clearHeaders().headers(headers);
        b.clearAppendHeaders().appendHeaders(appendHeaders);
    }

    private void overlayParams(@NonNull SparqlConfiguration higher, SparqlConfigurationBuilder b) {
        for (Map.Entry<String, List<String>> e : higher.params.entrySet()) {
            String name = e.getKey();
            List<String> values = e.getValue();
            if (values.isEmpty()) {
                b.param(name, values);
            } else {
                List<String> mine = params.getOrDefault(name, null);
                if (mine != null && !mine.isEmpty()) {
                    ArrayList<String> copy = new ArrayList<>(mine.size() + values.size());
                    copy.addAll(mine);
                    copy.addAll(values);
                    values = copy;
                }
                b.param(name, values);
            }
        }
    }

    static <T> @PolyNull List<T> nonEmptyNonNullDistinct(@PolyNull List<@Nullable T> input,
                                                         List<T> ifEmpty, String itemName) {
        if (input == null || input.isEmpty())
            return ifEmpty;
        ArrayList<T> sanitized = new ArrayList<>(input.size());
        int nullCount = 0;
        for (T item : input) {
            if (item == null)
                nullCount++;
            else  if (!sanitized.contains(item))
                sanitized.add(item);
        }
        if (sanitized.isEmpty()) {
            String msg = itemName.substring(0, 1).toUpperCase() + itemName.substring(1)+
                    " list became empty after removing "+nullCount+" items";
            throw new SparqlClientInvalidArgument(msg);
        }
        return Collections.unmodifiableList(sanitized);
    }

    @SuppressWarnings("StringEquality") static Map<String, String>
    sanitizeHeaders(@Nullable Map<@Nullable String, @Nullable String> headers) {
        if (headers == null || headers.isEmpty())
            return Collections.emptyMap();
        Map<String, String> sanitized = new HashMap<>(headers);
        for (String name : headers.keySet()) {
            String value = headers.get(name);
            if (name == null || name.trim().isEmpty()) {
                if (value != null && !value.trim().isEmpty()) {
                    String msg = "Non-null and non-empty value " + value + " for " +
                                 (name == null ? "null" : "empty") + " header";
                    throw new SparqlClientInvalidArgument(msg);
                }
                sanitized.remove(name);
            } else {
                String sanitizedName = HeaderUtils.sanitizeHeaderName(name);
                if ("accept".equals(sanitizedName)) {
                    String msg = "Accept header cannot be set via headers/appendHeaders, " +
                                 "use resultsAccepts or rdfAccepts";
                    throw new SparqlClientInvalidArgument(msg);
                }
                String sanitizedValue = value == null ? "" : value.trim();
                if (sanitizedName != name) {
                    sanitized.remove(name);
                    sanitized.put(sanitizedName, sanitizedValue);
                } else if (sanitizedValue != value) {
                    sanitized.put(name, sanitizedValue);
                }
            }
        }
        return Collections.unmodifiableMap(sanitized);
    }

    static Map<String, List<String>>
    sanitizeAppendHeaders(@Nullable Map<@Nullable String, @Nullable List<@Nullable String>> headers) {
        if (headers == null || headers.isEmpty())
            return Collections.emptyMap();
        Map<String, List<String>> result = new HashMap<>(headers);
        for (String name : headers.keySet()) {
            @Nullable List<@Nullable String> list = headers.get(name);
            if (name == null || name.trim().isEmpty()) {
                if (list != null && !list.isEmpty() && list.stream().anyMatch(Objects::nonNull)) {
                    String msg = (name == null ? "null" : "empty")
                               + " key maps to non-empty list with non-null values";
                    throw new SparqlClientInvalidArgument(msg);
                }
                result.remove(name);
            } else {
                String sanitizedName = HeaderUtils.sanitizeHeaderName(name);
                if ("accept".equals(sanitizedName)) {
                    String msg = "Accept header cannot be set in headers/appendHeaders, " +
                            "use resultsAccepts/rdfAccepts instead";
                    throw new SparqlClientInvalidArgument(msg);
                }

                List<String> saneList;
                if (list == null || list.isEmpty()) {
                    saneList = Collections.emptyList();
                } else {
                    saneList = new ArrayList<>(list.size());
                    int nullCount = 0, emptyCount = 0, wsCount = 0;
                    for (String value : list) {
                        if (value == null) {
                            ++nullCount;
                        } else if (value.isEmpty()) {
                            ++emptyCount;
                        } else {
                            value = value.trim();
                            if (value.isEmpty()) ++wsCount;
                            else                 saneList.add(value);
                        }
                    }
                    if (saneList.isEmpty()) {
                        String msg = format("Empty value list for header \"%s\". " +
                                            "Removed %d nulls, %d empty and %d whitespace items",
                                            name, nullCount, emptyCount, wsCount);
                        throw new SparqlClientInvalidArgument(msg);
                    }
                }
                //noinspection StringEquality
                if (sanitizedName != name)
                    result.remove(name);
                result.put(sanitizedName, saneList);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    static Map<String, List<String>>
    sanitizeParams(@Nullable Map<@Nullable String, @Nullable List<@Nullable String>> params) {
        if (params == null || params.isEmpty())
            return Collections.emptyMap();
        Map<String, List<String>> sanitizedMap = new HashMap<>(params);
        for (String name : params.keySet()) {
            List<@Nullable String> list = params.get(name);
            if (name == null) {
                if (list != null && list.stream().anyMatch(Objects::nonNull)) {
                    String msg = "null param name maps to non-empty list with non-null values";
                    throw new SparqlClientInvalidArgument(msg);
                }
                sanitizedMap.remove(null);
            } else {
                String sanitizedName = UriUtils.escapeQueryParam(name);
                //noinspection StringEquality
                if (sanitizedName != name)
                    sanitizedMap.remove(name);
                if (list != null && !list.isEmpty()) {
                    List<@NonNull String> values = new ArrayList<>(list.size());
                    for (String value : list) {
                        if (value != null) values.add(UriUtils.escapeQueryParam(value));
                    }
                    if (values.isEmpty()) {
                        String msg = format("Empty values list for param %s after " +
                                            "removing %d nulls from the input list",
                                            name, list.size());
                        throw new SparqlClientInvalidArgument(msg);
                    }
                    sanitizedMap.put(sanitizedName, values);
                } else {
                    sanitizedMap.put(sanitizedName, list == null ? Collections.emptyList() : list);
                }
            }
        }
        return Collections.unmodifiableMap(sanitizedMap);
    }


}
