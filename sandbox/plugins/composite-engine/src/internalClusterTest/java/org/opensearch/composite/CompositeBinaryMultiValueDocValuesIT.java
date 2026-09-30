/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Reads a multi-valued {@code binary} field on a composite (Parquet-primary) index back through every read
 * surface and checks each answer against a vanilla Lucene index holding the same documents. The two indices
 * differ only in how the values are stored: vanilla frames all of a document's values into one Lucene binary
 * doc value and stores each one as its own stored field; composite writes a single {@code LIST<Binary>} Parquet
 * column and must present it identically. Doc values are sorted unsigned and deduplicated; stored fields and
 * {@code _source} keep ingest order and duplicates; an absent field and an empty array both mean "no value".
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class CompositeBinaryMultiValueDocValuesIT extends AbstractCompositeEngineIT {

    private static final String COMPOSITE = "bin_mv";
    private static final String VANILLA = "bin_mv_vanilla";
    private static final int DOCS = 36;
    private static final int SHAPES = 6;

    /** Auto-generated ids per index, keyed by {@code n}, for get-by-id. */
    private static final Map<String, Map<Integer, String>> IDS = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (indexExists(COMPOSITE) == false) {
            Settings.Builder composite = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", List.of("lucene"));
            Settings.Builder vanilla = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            // multi_value is a composite-only mapping parameter; a vanilla index takes arrays without it.
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(COMPOSITE)
                    .setSettings(composite)
                    .setMapping("n", "type=long", "blob", "type=binary,store=true,doc_values=true,multi_value=true")
            );
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(VANILLA)
                    .setSettings(vanilla)
                    .setMapping("n", "type=long", "blob", "type=binary,store=true,doc_values=true")
            );
            ensureGreen(COMPOSITE, VANILLA);
            indexDocs();
        }
    }

    /** Two refreshes, so each index holds two segments and the read path crosses a segment boundary. */
    private void indexDocs() {
        for (int i = 0; i < DOCS; i++) {
            for (String index : List.of(COMPOSITE, VANILLA)) {
                var request = client().prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                List<byte[]> values = ingested(i);
                if (values == null) {
                    request.setSource("n", i);
                } else {
                    request.setSource("n", i, "blob", encode(values));
                }
                IndexResponse response = request.get();
                IDS.computeIfAbsent(index, k -> new HashMap<>()).put(i, response.getId());
            }
            if (i == DOCS / 2) {
                refresh(COMPOSITE, VANILLA);
            }
        }
        refresh(COMPOSITE, VANILLA);
    }

    /**
     * The values document {@code i} was indexed with, in ingest order; {@code null} means the field was absent.
     * Six shapes cycle: absent, empty array, one value, two values in descending order, duplicates, and a mix
     * of a 300-byte value (two-byte length vInt), an empty value and a short one.
     */
    private static List<byte[]> ingested(int i) {
        byte[] a = bytes(i, i % 5 + 1);
        byte[] b = Arrays.copyOf(a, a.length + 1); // a is a proper prefix of b, so b > a unsigned
        b[a.length] = (byte) 0xF0;
        return switch (i % SHAPES) {
            case 0 -> null;
            case 1 -> List.of();
            case 2 -> List.of(a);
            case 3 -> List.of(b, a);
            case 4 -> List.of(a, a, bytes(i + 7, 3));
            default -> List.of(bytes(i, 300), new byte[0], bytes(i + 1, 2));
        };
    }

    private static byte[] bytes(int seed, int length) {
        byte[] value = new byte[length];
        for (int k = 0; k < length; k++) {
            value[k] = (byte) (seed * 31 + k * 7);
        }
        return value;
    }

    /**
     * What doc values must return: the ingested values sorted unsigned with duplicates removed, rendered the way
     * {@code DocValueFormat.BINARY} renders them (base64 without padding).
     */
    private static List<String> expectedDocValues(int i) {
        List<byte[]> values = ingested(i);
        if (values == null) {
            return List.of();
        }
        TreeSet<byte[]> sorted = new TreeSet<>(Arrays::compareUnsigned);
        sorted.addAll(values);
        List<String> out = new ArrayList<>(sorted.size());
        for (byte[] value : sorted) {
            out.add(Base64.getEncoder().withoutPadding().encodeToString(value));
        }
        return out;
    }

    /** What stored fields and {@code _source} must return: the ingested values in ingest order, duplicates kept. */
    private static List<String> expectedStored(int i) {
        List<byte[]> values = ingested(i);
        return values == null ? List.of() : encode(values);
    }

    private static boolean hasValues(int i) {
        List<byte[]> values = ingested(i);
        return values != null && values.isEmpty() == false;
    }

    private static List<String> encode(List<byte[]> values) {
        List<String> out = new ArrayList<>(values.size());
        for (byte[] value : values) {
            out.add(Base64.getEncoder().encodeToString(value));
        }
        return out;
    }

    public void testAllSurfacesMatchVanillaBeforeAndAfterForceMerge() {
        assertAllSurfacesMatchVanilla("two segments");

        assertEquals(0, client().admin().indices().prepareForceMerge(COMPOSITE, VANILLA).setMaxNumSegments(1).get().getFailedShards());
        refresh(COMPOSITE, VANILLA);

        assertAllSurfacesMatchVanilla("one merged segment");
    }

    public void testExistsQueryMatchesVanilla() {
        int present = 0;
        for (int i = 0; i < DOCS; i++) {
            if (hasValues(i)) {
                present++;
            }
        }
        for (String index : List.of(COMPOSITE, VANILLA)) {
            assertEquals(index + ": exists", present, count(index, QueryBuilders.existsQuery("blob")));
            assertEquals(
                index + ": not exists",
                DOCS - present,
                count(index, QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("blob")))
            );
        }
    }

    public void testGetByIdSourceMatchesVanilla() {
        for (int i = 0; i < DOCS; i++) {
            for (String index : List.of(COMPOSITE, VANILLA)) {
                GetResponse response = client().prepareGet(index, IDS.get(index).get(i)).setRealtime(false).get();
                assertTrue(index + ": doc " + i + " exists", response.isExists());
                assertEquals(
                    index + ": get-by-id _source doc " + i,
                    expectedStored(i),
                    normalizeSource(response.getSourceAsMap().get("blob"))
                );
            }
        }
    }

    private void assertAllSurfacesMatchVanilla(String stage) {
        for (String index : List.of(COMPOSITE, VANILLA)) {
            Map<Long, List<String>> docValues = docValuesByN(index);
            assertEquals(stage + " " + index + ": every doc returned", DOCS, docValues.size());
            for (int i = 0; i < DOCS; i++) {
                assertEquals(stage + " " + index + ": docvalue_fields doc " + i, expectedDocValues(i), docValues.get((long) i));

                SearchHit hit = fetchHit(index, i);
                assertEquals(stage + " " + index + ": stored_fields doc " + i, expectedStored(i), storedBlobs(hit));
                assertEquals(
                    stage + " " + index + ": _source doc " + i,
                    expectedStored(i),
                    normalizeSource(hit.getSourceAsMap().get("blob"))
                );
                assertEquals(stage + " " + index + ": _source n doc " + i, (long) i, ((Number) hit.getSourceAsMap().get("n")).longValue());
            }
        }
    }

    private SearchHit fetchHit(String index, int n) {
        SearchResponse response = client().prepareSearch(index)
            .setQuery(QueryBuilders.termQuery("n", n))
            .setFetchSource(true)
            .addStoredField("blob")
            .get();
        assertEquals(index + ": no shard failures", 0, response.getFailedShards());
        assertEquals(index + ": one hit for n=" + n, 1, response.getHits().getTotalHits().value());
        return response.getHits().getAt(0);
    }

    /** One stored field per value, in ingest order; the transport client renders each as bytes or base64. */
    private static List<String> storedBlobs(SearchHit hit) {
        DocumentField field = hit.getFields().get("blob");
        if (field == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (Object value : field.getValues()) {
            if (value instanceof BytesReference bytes) {
                out.add(Base64.getEncoder().encodeToString(BytesReference.toBytes(bytes)));
            } else if (value instanceof byte[] bytes) {
                out.add(Base64.getEncoder().encodeToString(bytes));
            } else {
                out.add((String) value);
            }
        }
        return out;
    }

    /**
     * Vanilla returns the original JSON (an array, possibly empty); derived source on composite renders one value
     * as a scalar and no values as an absent field. Both are the same document, so compare them as a list.
     */
    @SuppressWarnings("unchecked")
    private static List<String> normalizeSource(Object blob) {
        if (blob == null) {
            return List.of();
        }
        if (blob instanceof List<?> list) {
            return (List<String>) list;
        }
        return List.of((String) blob);
    }

    private Map<Long, List<String>> docValuesByN(String index) {
        SearchResponse response = client().prepareSearch(index)
            .setFetchSource(false)
            .addDocValueField("n")
            .addDocValueField("blob")
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(DOCS)
            .get();
        assertEquals(index + ": no shard failures", 0, response.getFailedShards());
        Map<Long, List<String>> byN = new HashMap<>();
        for (SearchHit hit : response.getHits()) {
            long n = hit.getFields().get("n").<Long>getValue();
            DocumentField blob = hit.getFields().get("blob");
            List<String> values = new ArrayList<>();
            if (blob != null) {
                for (Object value : blob.getValues()) {
                    values.add((String) value);
                }
            }
            byN.put(n, values);
        }
        return byN;
    }

    private long count(String index, QueryBuilder query) {
        SearchResponse response = client().prepareSearch(index).setSize(0).setQuery(query).get();
        assertEquals(index + ": no shard failures", 0, response.getFailedShards());
        return response.getHits().getTotalHits().value();
    }
}
