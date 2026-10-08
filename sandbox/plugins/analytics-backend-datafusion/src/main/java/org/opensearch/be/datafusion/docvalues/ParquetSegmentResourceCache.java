/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-index cache of one {@link ParquetSegmentResources} per segment core, keyed on the leaf's core cache
 * key.
 *
 * <p>The producer, the synthesized {@link FieldInfo}s and the combined {@link FieldInfos} are built
 * once when a segment core is first seen and reused by every request and every {@code openIfChanged}
 * over that core. File resolve, metadata read and format-version gate run once per core, not per
 * request. A core that serves no Parquet doc values caches the {@link ParquetSegmentResources#ABSENT}
 * sentinel, so the negative resolve is not repeated either. The resources are closed and dropped by
 * the core's closed-listener, so their file-level handles die with the segment core.
 *
 * <p>TODO: generalize the core-keyed map + closed-listener eviction when a second per-core value needs
 * it (sorted-set producer, uninverted ordinals); only getOrCreate(coreHelper, factory) and
 * close-on-evict are required.
 */
public final class ParquetSegmentResourceCache {

    /** {@code ParquetDataFormat.PARQUET_DATA_FORMAT_NAME}; that module is not on this plugin's compile classpath. */
    private static final String PARQUET_FORMAT = "parquet";

    /**
     * Metadata fields allowed through the stored-field carve-out. Exactly {@code _id} today: {@code _source},
     * {@code _routing} and {@code _seq_no} must never be served this way even if {@link StoredFieldMapping}
     * grows to cover their types, so the carve-out is gated on this explicit allowlist in addition to the
     * capability and {@link StoredFieldMapping} checks.
     */
    private static final Set<String> PARQUET_STORED_METADATA_FIELDS = Set.of(IdFieldMapper.CONTENT_TYPE);

    private final MapperService mapperService;
    private final Map<IndexReader.CacheKey, ParquetSegmentResources> resourceByCore = new ConcurrentHashMap<>();
    private final Object buildLock = new Object();

    public ParquetSegmentResourceCache(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    /**
     * The resources for {@code in}'s segment core, building them on first use. Returns
     * {@link ParquetSegmentResources#ABSENT} when the core serves no Parquet doc values.
     */
    ParquetSegmentResources resourcesFor(LeafReader in) throws IOException {
        IndexReader.CacheHelper coreHelper = in.getCoreCacheHelper();
        if (coreHelper != null) {
            ParquetSegmentResources cached = resourceByCore.get(coreHelper.getKey());
            if (cached != null) {
                return cached;
            }
        }
        synchronized (buildLock) {
            if (coreHelper != null) {
                ParquetSegmentResources cached = resourceByCore.get(coreHelper.getKey());
                if (cached != null) {
                    return cached;
                }
            }
            return build(in, coreHelper);
        }
    }

    private ParquetSegmentResources build(LeafReader in, IndexReader.CacheHelper coreHelper) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf; it has no stable segment core to key on and serves no Parquet doc values.
            return ParquetSegmentResources.ABSENT;
        }

        // The core cache key is the resources' lifecycle anchor, so a segment leaf with no core cache
        // helper has no safe scope to attach to and must fail rather than leak.
        if (coreHelper == null) {
            throw new IOException("segment leaf exposes no core cache helper; cannot scope Parquet doc-values producer");
        }
        IndexReader.CacheKey key = coreHelper.getKey();

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        if (ParquetSegmentLayout.resolve(state) == null) {
            return cacheAbsent(coreHelper, key);
        }

        FieldInfos existing = in.getFieldInfos();
        SyntheticFields synthetic = buildSyntheticFields(mapperService, existing);
        if (synthetic.parquetFields.isEmpty()) {
            return cacheAbsent(coreHelper, key);
        }

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(state, mapperService);
        ParquetSegmentResources built = new ParquetSegmentResources(
            producer,
            synthetic.parquetFields,
            synthetic.combinedFieldInfos,
            synthetic.multiValuedFields,
            segmentReader.getSegmentInfo().info,
            synthetic.storedFields
        );
        resourceByCore.put(key, built);
        // Registered once, in the create branch only, so a core carries exactly one listener no matter
        // how many requests wrap its leaf.
        coreHelper.addClosedListener(this::onCoreClosed);
        return built;
    }

    private ParquetSegmentResources cacheAbsent(IndexReader.CacheHelper coreHelper, IndexReader.CacheKey key) {
        resourceByCore.put(key, ParquetSegmentResources.ABSENT);
        coreHelper.addClosedListener(this::onCoreClosed);
        return ParquetSegmentResources.ABSENT;
    }

    /** Drops the resources bound to a segment core when Lucene drops the core, closing their producer. */
    private void onCoreClosed(IndexReader.CacheKey key) throws IOException {
        ParquetSegmentResources removed = resourceByCore.remove(key);
        if (removed != null && removed.producer != null) {
            removed.producer.close();
        }
    }

    /**
     * Test seam: installs {@code resources} for {@code in}'s segment core, or returns the resources
     * already cached for it, registering the core's closed-listener on first install.
     */
    ParquetSegmentResources cacheForTesting(LeafReader in, ParquetSegmentResources resources) {
        IndexReader.CacheHelper coreHelper = in.getCoreCacheHelper();
        IndexReader.CacheKey key = coreHelper.getKey();
        ParquetSegmentResources existing = resourceByCore.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (buildLock) {
            existing = resourceByCore.get(key);
            if (existing != null) {
                return existing;
            }
            resourceByCore.put(key, resources);
            coreHelper.addClosedListener(this::onCoreClosed);
            return resources;
        }
    }

    /** Cached resource count (tests). */
    int size() {
        return resourceByCore.size();
    }

    /**
     * Builds the synthetic {@link FieldInfo}s for the Parquet-only fields of a segment, the combined
     * {@link FieldInfos} presented to readers, which of those fields may hold more than one value, and
     * which are served as Parquet stored fields. Package-private and static so the per-field decision -
     * including the metadata carve-out for {@code _id} - can be unit-tested without a Parquet-backed
     * segment.
     */
    static SyntheticFields buildSyntheticFields(MapperService mapperService, FieldInfos existing) {
        Map<String, FieldInfo> parquetFields = new LinkedHashMap<>();
        Map<String, StoredFieldMapping.Kind> storedFields = new LinkedHashMap<>();
        Set<String> multiValuedFields = new HashSet<>();
        List<FieldInfo> combined = new ArrayList<>();
        int maxNumber = -1;
        for (FieldInfo fi : existing) {
            combined.add(fi);
            maxNumber = Math.max(maxNumber, fi.number);
        }

        // Synthesize a FieldInfo for every codec-served field whose doc values Lucene does not serve:
        // the mapped DV type when the codec serves its doc values, NONE when it serves only the stored
        // field. A keyword or ip field is present in the Lucene segment for its term postings, so its
        // entry is replaced rather than added: FieldInfos rejects two entries under one name, and the
        // synthetic one reports no postings or points.
        for (MappedFieldType mft : mapperService.fieldTypes()) {
            String name = mft.name();
            FieldInfo realFi = existing.fieldInfo(name);
            if (mapperService.isMetadataField(name)) {
                // Metadata fields never get synthetic doc values, so sort and agg on _source, _seq_no,
                // _routing, _id, ... keep whatever behaviour the Lucene secondary already gives them. The
                // one metadata field that passes is _id: it is on the explicit PARQUET_STORED_METADATA_FIELDS
                // allowlist, Parquet is the composite primary so it wins STORED_FIELDS for _id in
                // assignCapabilities (parquetClaimsStoredField is true), and StoredFieldMapping maps "_id" ->
                // BINARY. Serving its Parquet column as a stored field is what makes SearchHit#getId()
                // non-null on a composite index. _source, _routing and _seq_no must never be served this way
                // even if StoredFieldMapping grows to cover their types, which is why the allowlist gates the
                // carve-out in addition to the capability and StoredFieldMapping checks.
                if (PARQUET_STORED_METADATA_FIELDS.contains(name) == false
                    || parquetClaimsStoredField(mft) == false
                    || StoredFieldMapping.forType(mft.typeName()) == null) {
                    continue;
                }
                // The Lucene secondary indexes _id (real FieldInfo, IndexOptions.DOCS, no doc values). Leave
                // that real entry in `combined` untouched so getFieldInfos() still reports _id indexed,
                // matching the postings the FilterLeafReader delegates; we only register _id for stored-field
                // overlay routing and never synthesize doc values for it. The overlay FieldInfo is used only
                // by visitor.needsField/binaryField, which key on name, so the real FieldInfo serves there;
                // fall back to a synthetic stored-only one when the Lucene segment carries no _id entry.
                FieldInfo overlayFi = realFi != null
                    ? realFi
                    : newDocValuesFieldInfo(name, ++maxNumber, DocValuesType.NONE, DocValuesSkipIndexType.NONE);
                if (realFi == null) {
                    combined.add(overlayFi);
                }
                parquetFields.put(name, overlayFi);
                storedFields.put(name, StoredFieldMapping.forType(mft.typeName()));
                if (mft.isMultiValued()) {
                    multiValuedFields.add(name);
                }
                continue;
            }
            if (realFi != null && realFi.getDocValuesType() != DocValuesType.NONE) {
                continue;
            }
            // A non-metadata field keeps its mapped DV type; a type with no FieldTypeMapping is served only
            // as a stored field (dvType NONE).
            DocValuesType dvType = FieldTypeMapping.isSupported(mft.typeName())
                ? FieldTypeMapping.forType(mft.typeName())
                : DocValuesType.NONE;
            StoredFieldMapping.Kind storedKind = parquetClaimsStoredField(mft) ? StoredFieldMapping.forType(mft.typeName()) : null;
            if (dvType == DocValuesType.NONE && storedKind == null) {
                continue;
            }
            DocValuesSkipIndexType skipType = dvType == DocValuesType.NONE ? DocValuesSkipIndexType.NONE : skipIndexTypeFor(mft.typeName());
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType, skipType);
            if (realFi != null) {
                combined.removeIf(fi -> fi.name.equals(name));
            }
            parquetFields.put(name, synthetic);
            combined.add(synthetic);
            if (storedKind != null) {
                storedFields.put(name, storedKind);
            }
            // A mapping flips to LIST permanently the first time an array is ingested, so a field not
            // marked LIST has never stored one. The reverse is only wasteful: a LIST field whose older
            // segments happen to hold single values is still treated as multi-valued.
            if (mft.isMultiValued()) {
                multiValuedFields.add(name);
            }
        }

        return new SyntheticFields(
            parquetFields,
            storedFields,
            Set.copyOf(multiValuedFields),
            new FieldInfos(combined.toArray(new FieldInfo[0]))
        );
    }

    /** The synthetic field metadata derived from the mapping for a Parquet-backed segment. */
    static final class SyntheticFields {
        final Map<String, FieldInfo> parquetFields;
        final Map<String, StoredFieldMapping.Kind> storedFields;
        final Set<String> multiValuedFields;
        final FieldInfos combinedFieldInfos;

        SyntheticFields(
            Map<String, FieldInfo> parquetFields,
            Map<String, StoredFieldMapping.Kind> storedFields,
            Set<String> multiValuedFields,
            FieldInfos combinedFieldInfos
        ) {
            this.parquetFields = parquetFields;
            this.storedFields = storedFields;
            this.multiValuedFields = multiValuedFields;
            this.combinedFieldInfos = combinedFieldInfos;
        }
    }

    static boolean isIntegerShaped(String mappingType) {
        return switch (mappingType) {
            case "long", "integer", "short", "byte", "date", "boolean" -> true;
            default -> false;
        };
    }

    private static DocValuesSkipIndexType skipIndexTypeFor(String mappingType) {
        return isIntegerShaped(mappingType) ? DocValuesSkipIndexType.RANGE : DocValuesSkipIndexType.NONE;
    }

    /** True when the mapping is stored and Parquet claimed {@code STORED_FIELDS} for it, so no other format holds a copy. */
    private static boolean parquetClaimsStoredField(MappedFieldType mft) {
        if (mft.isStored() == false) {
            return false;
        }
        for (Map.Entry<DataFormat, Set<FieldTypeCapabilities.Capability>> claim : mft.getCapabilityMap().entrySet()) {
            boolean parquet = PARQUET_FORMAT.equals(claim.getKey().name());
            if (parquet && claim.getValue().contains(FieldTypeCapabilities.Capability.STORED_FIELDS)) {
                return true;
            }
        }
        return false;
    }

    /** Builds a synthetic {@link FieldInfo}; {@code dvType} is NONE for a field served only as a stored field. */
    private static FieldInfo newDocValuesFieldInfo(String name, int number, DocValuesType dvType, DocValuesSkipIndexType skipType) {
        return new FieldInfo(
            name,
            number,
            false,                       // storeTermVector
            true,                        // omitNorms
            false,                       // storePayloads
            IndexOptions.NONE,           // not indexed via this reader
            dvType,
            skipType,
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
}
