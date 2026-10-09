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
import org.apache.lucene.index.DocValues;
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
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
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

    /**
     * End-to-end through the production wiring: wrap a real {@link DirectoryReader} with
     * {@link ParquetDocValuesDirectoryReader} (the factory the reader wrapper installs), read a keyword
     * value through the leaf's {@link ParquetDocValuesLeafReader#perDocumentValuesReader() per-document
     * view} so the lazy native binary cursor actually opens, then close the directory reader.
     *
     * <p>Pins that {@link ParquetDocValuesDirectoryReader#doClose()} closes the derived-source cache, which
     * frees that keyword cursor. The binary cursor is not recorded on the {@link CursorRegistry} and the
     * production cache is private, so only the cache's close frees it - remove that close and the native
     * handle outlives the reader.
     *
     * <p>The single open is proven by {@link ParquetDocValuesProducer#cursorsOpened()}, which counts only
     * this view's cursors. The close is then checked against the native live-handle registry
     * ({@link NativeHandle#liveHandleCount()}): once the reader closes, the live-handle count is back at or
     * below the pre-open baseline. The assertion is {@code <=}, not {@code ==}, on purpose - a Cleaner
     * freeing an unrelated older handle can only decrement the JVM-global count, so {@code <=} never fails
     * spuriously, yet it still fails deterministically if the cache stops closing the cursor (the handle then
     * outlives the reader and the count stays above the baseline). Exact-count assertions on the global
     * registry would be flaky for exactly that reason, so this test avoids them.
     */
    public void testClosingDirectoryReaderFreesTheDerivedSourceKeywordCursor() throws Exception {
        List<String> cities = List.of("delhi", "mumbai", "pune");
        Path parquet = createTempDir().resolve("cities-e2e.parquet");
        StringColumnFixture.write(parquet, allocator, CITY, cities);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, cities.size());
            DirectoryReader in = DirectoryReader.open(dir);
            DirectoryReader wrapped = null;
            try {
                SegmentReader seg = (SegmentReader) in.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, cities.size());
                // Install the Parquet resources for this segment core so the production wrap resolves them,
                // then wrap the reader exactly as the index reader wrapper does in production.
                ParquetSegmentResourceCache resourceCache = new ParquetSegmentResourceCache(null);
                resourceCache.cacheForTesting(seg, keywordResources(seg, producer));
                wrapped = ParquetDocValuesDirectoryReader.wrap(in, resourceCache);

                ParquetDocValuesLeafReader leaf = (ParquetDocValuesLeafReader) wrapped.leaves().get(0).reader();
                LeafReader view = leaf.perDocumentValuesReader();

                int handlesBefore = NativeHandle.liveHandleCount();
                assertKeywordValue(view, 0, "delhi"); // advanceExact opens the lazy native binary cursor

                // Prove the one open through the producer's own counter, not the JVM-global live-handle
                // count: a Cleaner freeing an unrelated older handle mid-test can only shift that global
                // count down, so asserting handlesBefore+1 here would be flaky. cursorsOpened() counts
                // exactly this view's cursors.
                assertEquals("the derived-source view opens exactly one native keyword cursor", 1, producer.cursorsOpened());

                wrapped.close(); // request end: doClose() closes the derived-source cache, then the registry

                // doClose() closed the derived-source cache, which freed this view's one keyword cursor, so
                // the live-handle count is back at or below the pre-open baseline. Cleaners only decrement
                // the global count, so <= never fails spuriously; it still fails deterministically if the
                // cache stops closing the cursor (the handle then outlives the reader and the count stays
                // above handlesBefore), which is what pins ParquetDocValuesDirectoryReader.doClose()'s close.
                assertTrue(
                    "closing the directory reader freed the derived-source keyword cursor (live handles back to baseline)",
                    NativeHandle.liveHandleCount() <= handlesBefore
                );
                assertEquals("no further native cursor opened after the single derived-source read", 1, producer.cursorsOpened());
            } finally {
                // On an assertion failure before wrapped.close(), close the wrapper so its cursor cache and
                // registry free the native cursor; closing it also closes the inner reader.
                if (wrapped != null && wrapped.getRefCount() > 0) {
                    wrapped.close();
                } else if (in.getRefCount() > 0) {
                    in.close();
                }
            }
        }
    }

    /**
     * The keyword aggregation accessor on the leaf itself ({@code buildOrdinals=true}) is unaffected by the
     * reuse cache: it opens its own cursor and caches nothing in the request's {@link DerivedSourceCursorCache}.
     */
    public void testKeywordAggregationAccessorDoesNotUseTheDerivedSourceCache() throws Exception {
        List<String> cities = List.of("delhi", "mumbai", "pune");
        Path parquet = createTempDir().resolve("cities-agg.parquet");
        StringColumnFixture.write(parquet, allocator, CITY, cities);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, cities.size());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader seg = (SegmentReader) reader.leaves().get(0).reader();
                ParquetDocValuesProducer producer = numericProducer(parquet, cities.size());
                CursorRegistry registry = new CursorRegistry();
                DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
                ParquetDocValuesLeafReader leaf = keywordLeaf(seg, producer, cities.size(), registry, cache);

                // Aggregation accessor (buildOrdinals=true): no CITY postings in the Lucene segment, so the
                // ordinals build is skipped and the streaming reader serves the value directly. Grab the
                // inner streaming iterator before advancing - DocValues.unwrapSingleton rejects a singleton
                // whose inner has already been used - so it can be closed once the read is done.
                SortedSetDocValues agg = leaf.getSortedSetDocValues(CITY);
                ParquetSortedDocValues aggInner = (ParquetSortedDocValues) DocValues.unwrapSingleton(agg);
                assertTrue(agg.advanceExact(0));
                assertEquals("delhi", agg.lookupOrd(agg.nextOrd()).utf8ToString());

                assertEquals("the keyword aggregation path does not use the derived-source cache", 0, cache.liveCursorCount());

                // The aggregation path opens its own streaming keyword cursor, recorded on neither the
                // registry nor the cache. Close it explicitly rather than leaving it to the iterator's
                // Cleaner: a Cleaner-only handle freed mid-run would shift the JVM-global live-handle count
                // that testClosingDirectoryReaderFreesTheDerivedSourceKeywordCursor reads.
                aggInner.close();
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

    /** The keyword SORTED_SET resources for {@code seg}, installed on the resource cache for a production wrap. */
    private ParquetSegmentResources keywordResources(SegmentReader seg, ParquetDocValuesProducer producer) {
        FieldInfo fi = syntheticField(CITY, DocValuesType.SORTED_SET);
        return new ParquetSegmentResources(
            producer,
            Map.of(CITY, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            Set.of(),
            seg.getSegmentInfo().info
        );
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
