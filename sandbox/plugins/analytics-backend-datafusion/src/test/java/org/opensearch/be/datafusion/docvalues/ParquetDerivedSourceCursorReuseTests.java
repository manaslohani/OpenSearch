/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.common.settings.Settings;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads real Parquet segments through {@link ParquetDocValuesLeafReader#perDocumentValuesReader() the
 * per-document view} to prove _source derivation reuses one native cursor per column across hits, while
 * the aggregation accessor still opens a fresh cursor per call. The cursor-open count is read from
 * {@link ParquetDocValuesProducer#cursorsOpened()}.
 */
public class ParquetDerivedSourceCursorReuseTests extends DataFusionBackedTestCase {

    private static final String NUM = "n";
    private static final String CITY = "city";

    /** Two hits over the same numeric column through the view open one cursor and read the right values. */
    public void testViewReusesOneNumericCursorAcrossHits() throws Exception {
        int rows = 5;
        Path parquet = createTempDir().resolve("nums.parquet");
        LongColumnFixture.write(parquet, allocator, NUM, rows, 0);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, rows);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader seg = (SegmentReader) reader.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, rows);
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                LeafReader view = numericLeaf(seg, producer, rows, registry, cache).perDocumentValuesReader();

                for (int doc : new int[] { 1, 3 }) {
                    assertNumericValue(view, doc, LongColumnFixture.valueAt(doc));
                }

                assertEquals("one native cursor serves the column across both hits", 1, producer.cursorsOpened());
                assertEquals("one cursor registered on the request", 1, registry.opened().size());
                assertEquals("one live cached cursor", 1, cache.liveCursorCount());
                cache.close();
                registry.close();
            }
        }
    }

    /** Two hits over the same keyword column through the view open one cursor and read the right values. */
    public void testViewReusesOneKeywordCursorAcrossHits() throws Exception {
        List<String> cities = List.of("delhi", "mumbai", "pune");
        Path parquet = createTempDir().resolve("cities.parquet");
        StringColumnFixture.write(parquet, allocator, CITY, cities);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, cities.size());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader seg = (SegmentReader) reader.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, cities.size());
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                LeafReader view = keywordLeaf(seg, producer, cities.size(), registry, cache).perDocumentValuesReader();

                assertKeywordValue(view, 0, "delhi");
                assertKeywordValue(view, 2, "pune");

                assertEquals("one native binary cursor serves the column across both hits", 1, producer.cursorsOpened());
                assertEquals("one live cached cursor", 1, cache.liveCursorCount());
                cache.close();
                registry.close();
            }
        }
    }

    /** A backward target and a repeated target reuse the one cursor and still read the right value. */
    public void testBackwardAndRepeatTargetsReuseTheCursor() throws Exception {
        int rows = 6;
        Path parquet = createTempDir().resolve("nums-bw.parquet");
        LongColumnFixture.write(parquet, allocator, NUM, rows, 0);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, rows);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader seg = (SegmentReader) reader.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, rows);
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                LeafReader view = numericLeaf(seg, producer, rows, registry, cache).perDocumentValuesReader();

                assertNumericValue(view, 4, LongColumnFixture.valueAt(4));
                assertNumericValue(view, 1, LongColumnFixture.valueAt(1)); // backward: cursor resets, no new cursor
                assertNumericValue(view, 1, LongColumnFixture.valueAt(1)); // repeat same target
                assertNumericValue(view, 2, LongColumnFixture.valueAt(2));

                assertEquals("backward and repeat targets reuse the one cursor", 1, producer.cursorsOpened());
                cache.close();
                registry.close();
            }
        }
    }

    /** A backward target and a repeated target reuse the one keyword cursor and still read the right value. */
    public void testKeywordBackwardAndRepeatTargetsReuseTheCursor() throws Exception {
        List<String> cities = List.of("delhi", "mumbai", "pune", "surat");
        Path parquet = createTempDir().resolve("cities-bw.parquet");
        StringColumnFixture.write(parquet, allocator, CITY, cities);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, cities.size());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader seg = (SegmentReader) reader.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, cities.size());
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                LeafReader view = keywordLeaf(seg, producer, cities.size(), registry, cache).perDocumentValuesReader();

                assertKeywordValue(view, 3, "surat");
                assertKeywordValue(view, 1, "mumbai"); // backward: binary cursor resets (loadBatchContaining), no reopen
                assertKeywordValue(view, 1, "mumbai"); // repeat same target
                assertKeywordValue(view, 2, "pune");

                assertEquals("backward and repeat keyword targets reuse the one cursor", 1, producer.cursorsOpened());
                cache.close();
                registry.close();
            }
        }
    }

    /** The aggregation accessor on the leaf itself is unchanged: a fresh cursor per call, never cached. */
    public void testAggregationAccessorStillOpensAFreshCursorAndDoesNotCache() throws Exception {
        int rows = 4;
        Path parquet = createTempDir().resolve("nums-agg.parquet");
        LongColumnFixture.write(parquet, allocator, NUM, rows, 0);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, rows);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader seg = (SegmentReader) reader.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, rows);
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                ParquetDocValuesLeafReader leaf = numericLeaf(seg, producer, rows, registry, cache);

                SortedNumericDocValues first = leaf.getSortedNumericDocValues(NUM);
                assertTrue(first.advanceExact(0));
                assertEquals(LongColumnFixture.valueAt(0), first.nextValue());
                SortedNumericDocValues second = leaf.getSortedNumericDocValues(NUM);
                assertTrue(second.advanceExact(1));
                assertEquals(LongColumnFixture.valueAt(1), second.nextValue());

                assertEquals("the aggregation accessor opens a fresh cursor each call", 2, producer.cursorsOpened());
                assertEquals("the aggregation path does not use the derived-source cache", 0, cache.liveCursorCount());
                cache.close();
                registry.close();
            }
        }
    }

    /**
     * Two real Parquet segments served through the same cache: a column asked on segment B after segment A
     * evicts and closes A's native cursor, so the thread keeps exactly one live cursor per column across the
     * switch while both segments read the right values.
     */
    public void testCrossSegmentEvictionKeepsOneLiveCursorPerColumn() throws Exception {
        int rowsA = 5;
        int rowsB = 4;
        Path parquetA = createTempDir().resolve("seg-a.parquet");
        Path parquetB = createTempDir().resolve("seg-b.parquet");
        LongColumnFixture.write(parquetA, allocator, NUM, rowsA, 0);
        LongColumnFixture.write(parquetB, allocator, NUM, rowsB, 0);

        try (Directory dirA = new ByteBuffersDirectory(); Directory dirB = new ByteBuffersDirectory()) {
            writeDocs(dirA, rowsA);
            writeDocs(dirB, rowsB);
            try (DirectoryReader readerA = DirectoryReader.open(dirA); DirectoryReader readerB = DirectoryReader.open(dirB)) {
                SegmentReader segA = (SegmentReader) readerA.leaves().get(0).reader();
                SegmentReader segB = (SegmentReader) readerB.leaves().get(0).reader();
                ParquetDocValuesProducer producerA = numericProducer(parquetA, rowsA);
                ParquetDocValuesProducer producerB = numericProducer(parquetB, rowsB);
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                // Two leaves, distinct segment keys, sharing one request-scoped cache.
                LeafReader viewA = numericLeaf(segA, producerA, rowsA, registry, cache).perDocumentValuesReader();
                LeafReader viewB = numericLeaf(segB, producerB, rowsB, registry, cache).perDocumentValuesReader();

                assertNumericValue(viewA, 2, LongColumnFixture.valueAt(2));
                assertEquals("segment A holds the single live cursor", 1, cache.liveCursorCount());

                // Segment B asks for the same column: A's cursor is evicted and closed, B opens its own.
                assertNumericValue(viewB, 1, LongColumnFixture.valueAt(1));
                assertEquals("after the segment switch exactly one cursor stays live", 1, cache.liveCursorCount());
                assertEquals(
                    "one native cursor opened per segment (A evicted, B opened)",
                    2,
                    producerA.cursorsOpened() + producerB.cursorsOpened()
                );

                // Re-reading segment B keeps reusing B's one cursor; no reopen.
                assertNumericValue(viewB, 3, LongColumnFixture.valueAt(3));
                assertEquals("segment B keeps its one live cursor", 1, cache.liveCursorCount());
                assertEquals(
                    "no further native cursor opened for the reused segment",
                    2,
                    producerA.cursorsOpened() + producerB.cursorsOpened()
                );

                cache.close();
                registry.close();
            }
        }
    }

    private static void assertNumericValue(LeafReader view, int doc, long expected) throws Exception {
        SortedNumericDocValues dv = view.getSortedNumericDocValues(NUM);
        assertTrue("doc " + doc + " must have a value", dv.advanceExact(doc));
        assertEquals(1, dv.docValueCount());
        assertEquals(expected, dv.nextValue());
    }

    private static void assertKeywordValue(LeafReader view, int doc, String expected) throws Exception {
        SortedSetDocValues ss = view.getSortedSetDocValues(CITY);
        assertTrue("doc " + doc + " must have a value", ss.advanceExact(doc));
        assertEquals(1, ss.docValueCount());
        assertEquals(expected, ss.lookupOrd(ss.nextOrd()).utf8ToString());
    }

    private ParquetDocValuesProducer numericProducer(Path parquetFile, int maxDoc) {
        // Test-seam constructor: null mapper service skips mapping-type validation, so a synthetic
        // SORTED_NUMERIC/SORTED_SET FieldInfo is served directly from the fixture file.
        return new ParquetDocValuesProducer(parquetFile, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, maxDoc, null);
    }

    private ParquetDocValuesLeafReader numericLeaf(
        SegmentReader seg,
        ParquetDocValuesProducer producer,
        int maxDoc,
        CursorRegistry registry,
        DerivedSourceCursorCache cache
    ) {
        FieldInfo fi = syntheticField(NUM, DocValuesType.SORTED_NUMERIC);
        ParquetSegmentResources resources = new ParquetSegmentResources(
            producer,
            Map.of(NUM, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            Set.of(),
            seg.getSegmentInfo().info
        );
        return new ParquetDocValuesLeafReader(seg, resources, registry, cache);
    }

    private ParquetDocValuesLeafReader keywordLeaf(
        SegmentReader seg,
        ParquetDocValuesProducer producer,
        int maxDoc,
        CursorRegistry registry,
        DerivedSourceCursorCache cache
    ) {
        FieldInfo fi = syntheticField(CITY, DocValuesType.SORTED_SET);
        ParquetSegmentResources resources = new ParquetSegmentResources(
            producer,
            Map.of(CITY, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            Set.of(),
            seg.getSegmentInfo().info
        );
        return new ParquetDocValuesLeafReader(seg, resources, registry, cache);
    }

    /** One trivial document per Parquet row, so the Lucene segment's maxDoc matches the column length. */
    private static void writeDocs(Directory dir, int count) throws Exception {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < count; i++) {
                Document document = new Document();
                document.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            writer.commit();
        }
    }

    private static FieldInfo syntheticField(String name, DocValuesType dvType) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
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
