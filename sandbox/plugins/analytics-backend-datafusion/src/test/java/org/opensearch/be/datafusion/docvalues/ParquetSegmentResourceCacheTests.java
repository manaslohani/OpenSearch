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
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ParquetSegmentResourceCache#buildSyntheticFields} - the per-field decision that
 * turns the mapping into synthetic {@link FieldInfo}s, Parquet stored fields and the combined
 * {@link FieldInfos} - driven off a mocked {@link MapperService} so the loop can be exercised without a
 * Parquet-backed segment. The focus is the narrow metadata carve-out that lets {@code _id} - and only
 * {@code _id} - through as a stored field.
 */
public class ParquetSegmentResourceCacheTests extends OpenSearchTestCase {

    private static final DataFormat PARQUET = formatNamed("parquet");
    private static final Set<FieldTypeCapabilities.Capability> STORED = Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS);

    /**
     * {@code _id} is a metadata field, stored, and Parquet (the composite primary) wins its STORED_FIELDS
     * claim, so it alone passes the metadata carve-out: it lands in {@code storedFields} as BINARY, its
     * overlay FieldInfo carries no doc values (we never synthesize doc values for {@code _id}), and the
     * real Lucene {@code _id} entry is kept untouched in the combined FieldInfos (still indexed, no
     * synthetic replacement). {@code _source}, {@code _seq_no} and {@code _routing} are skipped entirely,
     * and a user {@code binary} field still gets a BINARY doc-values entry and a BINARY stored field.
     */
    public void testIdIsTheOnlyMetadataFieldServedAsAStoredField() {
        MappedFieldType id = mockFieldType("_id", IdFieldMapper.CONTENT_TYPE, true, Map.of(PARQUET, STORED));
        // Parquet also writes _source and _routing, so it claims STORED_FIELDS for them too; they are still
        // skipped here because neither type has a StoredFieldMapping entry. This proves the carve-out is
        // keyed on StoredFieldMapping, not merely on the capability claim.
        MappedFieldType source = mockFieldType("_source", "_source", true, Map.of(PARQUET, STORED));
        MappedFieldType seqNo = mockFieldType(
            "_seq_no",
            "_seq_no",
            false,
            Map.of(PARQUET, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE))
        );
        MappedFieldType routing = mockFieldType("_routing", "_routing", true, Map.of(PARQUET, STORED));
        MappedFieldType userBinary = mockFieldType("user_binary", "binary", true, Map.of(PARQUET, STORED));

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldTypes()).thenReturn(List.of(id, source, seqNo, routing, userBinary));
        when(mapperService.isMetadataField("_id")).thenReturn(true);
        when(mapperService.isMetadataField("_source")).thenReturn(true);
        when(mapperService.isMetadataField("_seq_no")).thenReturn(true);
        when(mapperService.isMetadataField("_routing")).thenReturn(true);
        when(mapperService.isMetadataField("user_binary")).thenReturn(false);

        // The Lucene secondary indexes _id as a term (postings, no doc values); the build loop must keep
        // that real entry untouched rather than replacing it with a stored-only synthetic one.
        FieldInfo luceneId = existingFieldInfo(IdFieldMapper.CONTENT_TYPE, 0, IndexOptions.DOCS, DocValuesType.NONE);
        FieldInfos existing = new FieldInfos(new FieldInfo[] { luceneId });

        ParquetSegmentResourceCache.SyntheticFields synthetic = ParquetSegmentResourceCache.buildSyntheticFields(mapperService, existing);

        // _id is served as a stored field, with no doc values.
        assertEquals(StoredFieldMapping.Kind.BINARY, synthetic.storedFields.get(IdFieldMapper.CONTENT_TYPE));
        assertNotNull("_id must have an overlay FieldInfo", synthetic.parquetFields.get(IdFieldMapper.CONTENT_TYPE));
        assertEquals(
            "no doc values are synthesized for _id",
            DocValuesType.NONE,
            synthetic.parquetFields.get(IdFieldMapper.CONTENT_TYPE).getDocValuesType()
        );

        // Every other metadata field is skipped from both maps.
        for (String metadata : List.of("_source", "_seq_no", "_routing")) {
            assertFalse(metadata + " must not be a stored field", synthetic.storedFields.containsKey(metadata));
            assertFalse(metadata + " must not be a Parquet field", synthetic.parquetFields.containsKey(metadata));
        }

        // A user binary field keeps its mapped BINARY doc values and is also served as a stored field.
        assertEquals(DocValuesType.BINARY, synthetic.parquetFields.get("user_binary").getDocValuesType());
        assertEquals(StoredFieldMapping.Kind.BINARY, synthetic.storedFields.get("user_binary"));

        assertEquals(
            "only _id and the user binary field are stored",
            Set.of(IdFieldMapper.CONTENT_TYPE, "user_binary"),
            synthetic.storedFields.keySet()
        );

        // The combined FieldInfos keeps the REAL _id entry untouched: still indexed (IndexOptions.DOCS)
        // with no doc values, so getFieldInfos() reports _id indexed to match the postings the
        // FilterLeafReader delegates. _id is registered for overlay routing (storedFields + parquetFields)
        // without a synthetic entry replacing the real one, and the overlay reuses the real FieldInfo.
        FieldInfo combinedId = synthetic.combinedFieldInfos.fieldInfo(IdFieldMapper.CONTENT_TYPE);
        assertEquals("the real _id keeps its term postings", IndexOptions.DOCS, combinedId.getIndexOptions());
        assertEquals("no doc values are synthesized for _id", DocValuesType.NONE, combinedId.getDocValuesType());
        assertSame("the overlay reuses the real _id FieldInfo", luceneId, synthetic.parquetFields.get(IdFieldMapper.CONTENT_TYPE));
        assertSame("the combined FieldInfos keeps the real _id FieldInfo", luceneId, combinedId);
        int idCount = 0;
        for (FieldInfo fi : synthetic.combinedFieldInfos) {
            if (fi.name.equals(IdFieldMapper.CONTENT_TYPE)) {
                idCount++;
            }
        }
        assertEquals("the real _id entry is kept, not duplicated", 1, idCount);
    }

    /**
     * A metadata field that is stored, wins Parquet's STORED_FIELDS claim AND whose {@code typeName}
     * happens to be {@code "binary"} (so {@link StoredFieldMapping#forType} returns BINARY) is still
     * excluded, because it is not on the explicit {@code PARQUET_STORED_METADATA_FIELDS} allowlist. This
     * is the guard that keeps {@code _source}/{@code _routing}/{@code _seq_no} out even if
     * {@link StoredFieldMapping} grows to cover their types: the carve-out is keyed on the allowlist, not
     * merely on the capability claim plus a StoredFieldMapping entry.
     */
    public void testMetadataFieldNotOnAllowlistIsExcludedEvenWhenBinaryTyped() {
        MappedFieldType fakeMeta = mockFieldType("_fake_meta", "binary", true, Map.of(PARQUET, STORED));

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldTypes()).thenReturn(List.of(fakeMeta));
        when(mapperService.isMetadataField("_fake_meta")).thenReturn(true);

        ParquetSegmentResourceCache.SyntheticFields synthetic = ParquetSegmentResourceCache.buildSyntheticFields(
            mapperService,
            new FieldInfos(new FieldInfo[0])
        );

        assertFalse(
            "a binary-typed metadata field off the allowlist is not a stored field",
            synthetic.storedFields.containsKey("_fake_meta")
        );
        assertFalse(
            "a binary-typed metadata field off the allowlist is not a Parquet field",
            synthetic.parquetFields.containsKey("_fake_meta")
        );
    }

    private static MappedFieldType mockFieldType(
        String name,
        String typeName,
        boolean stored,
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capabilities
    ) {
        MappedFieldType mft = mock(MappedFieldType.class);
        when(mft.name()).thenReturn(name);
        when(mft.typeName()).thenReturn(typeName);
        when(mft.isStored()).thenReturn(stored);
        when(mft.isMultiValued()).thenReturn(false);
        when(mft.getCapabilityMap()).thenReturn(capabilities);
        return mft;
    }

    private static DataFormat formatNamed(String name) {
        DataFormat format = mock(DataFormat.class);
        when(format.name()).thenReturn(name);
        return format;
    }

    private static FieldInfo existingFieldInfo(String name, int number, IndexOptions indexOptions, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false,
            true,
            false,
            indexOptions,
            dvType,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
