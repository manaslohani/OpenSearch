/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.codec.ParquetDocValuesFormat;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

/**
 * Segment-level {@link PerFieldDocValuesFormat} that routes a field's doc values to the
 * Parquet-backed {@link ParquetDocValuesFormat} when the field is Parquet-resident, and to
 * Lucene's native {@link Lucene90DocValuesFormat} otherwise. This lets the two formats coexist
 * within a single segment on a per-field basis (Req 13.3).
 *
 * <h2>Routing</h2>
 * A field is routed to Parquet when its {@code docValueFormats} list (supplied per-field by the
 * {@code docValueFormatsResolver}) contains {@code "parquet"}. When the list is
 * {@code ["parquet", "lucene"]} (both present), Parquet is chosen by default; this matches the
 * design's "default to Parquet unless overridden" rule. When the resolver returns {@code null}
 * or an empty/absent list, the field falls through to the Lucene native format.
 *
 * <h2>How the per-field binding is persisted</h2>
 * Lucene's {@link PerFieldDocValuesFormat#fieldsConsumer} stamps the chosen format's SPI name
 * into the segment's per-field attribute ({@link PerFieldDocValuesFormat#PER_FIELD_FORMAT_KEY},
 * i.e. {@code "PerFieldDocValuesFormat.format"}) at write time, and
 * {@link PerFieldDocValuesFormat#fieldsProducer} reads it back and resolves the format by SPI
 * name at read time. We therefore only override {@link #getDocValuesFormatForField(String)} —
 * the attribute stamping and read-time routing come for free.
 */
@ExperimentalApi
public final class CompositeParquetDocValuesFormat extends PerFieldDocValuesFormat {

    /**
     * Logical identifier for this composite routing format, mirroring the {@code Composite912}
     * naming. Note: the SPI name reported to Lucene is fixed by {@link PerFieldDocValuesFormat}
     * to {@link PerFieldDocValuesFormat#PER_FIELD_NAME}; this constant is for diagnostics and
     * callers that want to refer to the composite format by a stable label.
     */
    public static final String FORMAT_NAME = "CompositeParquetDocValues";

    /** Doc-value format name that marks a field as Parquet-resident. */
    public static final String PARQUET_FORMAT = "parquet";

    private final DocValuesFormat parquetFormat;
    private final DocValuesFormat luceneFormat;

    /**
     * Resolves a field name to its {@code docValueFormats} list (e.g. {@code ["parquet"]},
     * {@code ["parquet", "lucene"]}, {@code ["lucene"]}). May return {@code null} for unknown
     * fields, in which case the field routes to the Lucene native format.
     */
    private final Function<String, List<String>> docValueFormatsResolver;

    /**
     * @param mapperService            passed to the Parquet format so its producer can validate
     *                                 OpenSearch mapping types
     * @param docValueFormatsResolver  field name → its {@code docValueFormats} list
     */
    public CompositeParquetDocValuesFormat(MapperService mapperService, Function<String, List<String>> docValueFormatsResolver) {
        super();
        this.parquetFormat = new ParquetDocValuesFormat(mapperService);
        this.luceneFormat = new Lucene90DocValuesFormat();
        this.docValueFormatsResolver = Objects.requireNonNull(docValueFormatsResolver, "docValueFormatsResolver");
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        List<String> formats = docValueFormatsResolver.apply(field);
        if (isParquetResident(formats)) {
            return parquetFormat;
        }
        return luceneFormat;
    }

    /**
     * True when {@code formats} marks the field as Parquet-resident. A list containing
     * {@code "parquet"} routes to Parquet; this includes the {@code ["parquet", "lucene"]} case,
     * where Parquet is the default for queries unless explicitly overridden elsewhere.
     */
    static boolean isParquetResident(List<String> formats) {
        return formats != null && formats.contains(PARQUET_FORMAT);
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "CompositeParquetDocValuesFormat(parquet=%s, lucene=%s)", parquetFormat, luceneFormat);
    }
}
