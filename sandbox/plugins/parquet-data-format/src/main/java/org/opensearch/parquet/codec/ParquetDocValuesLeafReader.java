/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link FilterLeafReader} that serves doc values for Parquet-resident fields from a
 * {@link ParquetDocValuesProducer}, while delegating everything else to the underlying Lucene
 * leaf reader.
 *
 * <p>This is the read-time integration of the Parquet DocValues codec for the case where a field
 * is <b>Parquet-only</b> — i.e. it has no {@link FieldInfo} in the Lucene segment at all (the
 * composite engine's Lucene secondary writes only text/keyword inverted indexes plus the row-id
 * doc values; numeric fields like {@code age} live solely in Parquet). Lucene's
 * {@code PerFieldDocValuesFormat} cannot route to such a field because there is no segment
 * {@code FieldInfo} to carry the format name. This reader closes that gap by:
 *
 * <ol>
 *   <li><b>Synthesizing {@link FieldInfo}s</b> — for every mapped field that the Parquet codec
 *       supports and that is absent (or DV-less) in the delegate's {@link FieldInfos}, a synthetic
 *       {@code FieldInfo} with the appropriate {@link DocValuesType} (from {@link FieldTypeMapping})
 *       is added so OpenSearch's value-source layer believes the doc values exist and asks for
 *       them.</li>
 *   <li><b>Overriding the five DV accessors</b> — for those synthetic fields the iterators come
 *       from a per-segment {@link ParquetDocValuesProducer}; all other fields delegate to the
 *       underlying reader unchanged.</li>
 * </ol>
 *
 * <p>One producer is built lazily per segment and closed when this reader closes. Not shared
 * across segments.
 */
public final class ParquetDocValuesLeafReader extends FilterLeafReader {

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesLeafReader.class);

    private final MapperService mapperService;

    /** Lazily constructed Parquet producer for this segment; null until first DV access. */
    private ParquetDocValuesProducer producer;
    private boolean producerInitialized;

    /** Synthetic + real merged field infos, computed once. */
    private final FieldInfos mergedFieldInfos;

    /** Field name -> synthetic FieldInfo for Parquet-resident DV fields served by this reader. */
    private final Map<String, FieldInfo> parquetFields;

    /** The segment read state used to build the producer (captured at construction). */
    private final SegmentReadState segmentReadState;

    private ParquetDocValuesLeafReader(
        LeafReader in,
        MapperService mapperService,
        SegmentReadState segmentReadState,
        Map<String, FieldInfo> parquetFields,
        FieldInfos mergedFieldInfos
    ) {
        super(in);
        this.mapperService = mapperService;
        this.segmentReadState = segmentReadState;
        this.parquetFields = parquetFields;
        this.mergedFieldInfos = mergedFieldInfos;
    }

    /**
     * Builds a {@link ParquetDocValuesLeafReader} for {@code in} if a Parquet file resolves for the
     * segment and the mapping declares at least one Parquet-codec-supported field that is missing
     * doc values in the Lucene segment. Otherwise returns {@code in} unwrapped.
     */
    public static LeafReader wrapIfApplicable(LeafReader in, MapperService mapperService) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf (e.g. an in-memory test reader) — nothing to wrap.
            return in;
        }

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        // Only proceed if a Parquet file exists for this segment.
        if (ParquetSegmentLayout.resolve(state) == null) {
            return in;
        }

        FieldInfos existing = in.getFieldInfos();
        Map<String, FieldInfo> parquetFields = new LinkedHashMap<>();
        List<FieldInfo> merged = new ArrayList<>();
        int maxNumber = -1;
        for (FieldInfo fi : existing) {
            merged.add(fi);
            maxNumber = Math.max(maxNumber, fi.number);
        }

        // Walk the mapping. For each field the Parquet codec supports whose doc values are NOT
        // already present in the Lucene segment, synthesize a FieldInfo with the mapped DV type.
        for (MappedFieldType mft : mapperService.fieldTypes()) {
            String name = mft.name();
            if (mapperService.isMetadataField(name)) {
                continue;
            }
            if (FieldTypeMapping.isSupported(mft.typeName()) == false) {
                continue;
            }
            FieldInfo realFi = existing.fieldInfo(name);
            if (realFi != null && realFi.getDocValuesType() != DocValuesType.NONE) {
                // Lucene already serves doc values for this field — leave it to the native reader.
                continue;
            }
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mft.typeName());
            DocValuesType dvType = mapping.singleValued();
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType);
            parquetFields.put(name, synthetic);
            // If a DV-less FieldInfo already exists for this field, replace it with the synthetic
            // one carrying the DV type; otherwise append.
            if (realFi != null) {
                merged.removeIf(fi -> fi.name.equals(name));
            }
            merged.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            // Nothing for us to serve — don't wrap.
            return in;
        }

        FieldInfos mergedInfos = new FieldInfos(merged.toArray(new FieldInfo[0]));
        logger.info(
            "[PARQUET_DV_TRACE] ParquetDocValuesLeafReader: wrapping segment '{}' to serve {} Parquet-resident field(s): {}",
            segmentReader.getSegmentInfo().info.name,
            parquetFields.size(),
            parquetFields.keySet()
        );
        return new ParquetDocValuesLeafReader(in, mapperService, state, parquetFields, mergedInfos);
    }

    /** Builds a synthetic doc-values {@link FieldInfo} carrying the given DV type. */
    private static FieldInfo newDocValuesFieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false,                       // storeTermVector
            true,                        // omitNorms
            false,                       // storePayloads
            IndexOptions.NONE,           // not indexed via this reader
            dvType,
            DocValuesSkipIndexType.NONE,
            -1,                          // dvGen
            new HashMap<>(),             // attributes (mutable, per FieldInfo contract)
            0,                           // pointDimensionCount
            0,                           // pointIndexDimensionCount
            0,                           // pointNumBytes
            0,                           // vectorDimension
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,                       // softDeletes
            false                        // isParentField
        );
    }

    private synchronized ParquetDocValuesProducer producer() throws IOException {
        if (producerInitialized == false) {
            producer = new ParquetDocValuesProducer(segmentReadState, mapperService);
            producerInitialized = true;
        }
        return producer;
    }

    /** Returns the synthetic FieldInfo if the given field is served from Parquet, else null. */
    private FieldInfo parquetFieldInfo(String field) {
        return parquetFields.get(field);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return mergedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.NUMERIC) {
            return producer().getNumeric(fi);
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // A numeric mapping's single-valued DV type is NUMERIC, but OpenSearch numeric
            // value sources request SORTED_NUMERIC. The producer validates against both forms.
            FieldInfo asSortedNumeric = fi.getDocValuesType() == DocValuesType.SORTED_NUMERIC
                ? fi
                : newDocValuesFieldInfo(field, fi.number, DocValuesType.SORTED_NUMERIC);
            return producer().getSortedNumeric(asSortedNumeric);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.BINARY) {
            return producer().getBinary(fi);
        }
        return in.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.SORTED) {
            return producer().getSorted(fi);
        }
        return in.getSortedDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            FieldInfo asSortedSet = fi.getDocValuesType() == DocValuesType.SORTED_SET
                ? fi
                : newDocValuesFieldInfo(field, fi.number, DocValuesType.SORTED_SET);
            return producer().getSortedSet(asSortedSet);
        }
        return in.getSortedSetDocValues(field);
    }

    @Override
    protected void doClose() throws IOException {
        IOException first = null;
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (IOException e) {
            first = e;
        }
        try {
            super.doClose();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        if (first != null) {
            throw first;
        }
    }

    // Cache helpers must delegate to the underlying reader so query/segment caches stay coherent.
    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
