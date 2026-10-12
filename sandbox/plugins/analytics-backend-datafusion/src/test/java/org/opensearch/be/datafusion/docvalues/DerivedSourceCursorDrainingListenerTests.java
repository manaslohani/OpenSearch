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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the fetch-phase hook: both {@link DerivedSourceCursorDrainingListener#onFetchPhase} (success) and
 * {@link DerivedSourceCursorDrainingListener#onFailedFetchPhase} (failure) free the calling thread's reused
 * derived-source cursors through the {@link SearchContext}'s searcher while leaving the reader open; and the
 * hook is a safe no-op when there is no searcher or no Parquet reader in the chain. Reader fixtures mirror
 * {@link ParquetDerivedSourceCursorReuseTests}.
 */
public class DerivedSourceCursorDrainingListenerTests extends DataFusionBackedTestCase {

    private static final String CITY = "city";

    /** onFetchPhase (success path) frees the fetch thread's cursor while the reader stays open (a). */
    public void testOnFetchPhaseDrainsCallingThreadsCursors() throws Exception {
        assertHookDrains((listener, context) -> listener.onFetchPhase(context, 123L));
    }

    /** onFailedFetchPhase (failure path) frees the fetch thread's cursor just like the success path (b). */
    public void testOnFailedFetchPhaseDrainsCallingThreadsCursors() throws Exception {
        assertHookDrains((listener, context) -> listener.onFailedFetchPhase(context));
    }

    /** A context with no searcher is tolerated: the hook must not throw. */
    public void testHookIsNoOpWhenSearcherIsNull() {
        SearchContext context = mock(SearchContext.class);
        when(context.searcher()).thenReturn(null);
        DerivedSourceCursorDrainingListener listener = new DerivedSourceCursorDrainingListener();
        listener.onFetchPhase(context, 0L); // must not throw
        listener.onFailedFetchPhase(context);
    }

    /** A reader with no Parquet wrapper in its chain is tolerated: the hook is a no-op. */
    public void testHookIsNoOpForNonParquetReader() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, 1);
            try (DirectoryReader plain = DirectoryReader.open(dir)) {
                SearchContext context = mockContextFor(plain);
                DerivedSourceCursorDrainingListener listener = new DerivedSourceCursorDrainingListener();
                listener.onFetchPhase(context, 0L); // no Parquet reader in the chain -> no-op, no throw
                listener.onFailedFetchPhase(context);
            }
        }
    }

    /** Builds a wrapped reader, opens a native cursor on this thread, invokes {@code hook}, then asserts the drain. */
    private void assertHookDrains(ThrowingHook hook) throws Exception {
        List<String> cities = List.of("delhi", "mumbai");
        Path parquet = createTempDir().resolve("listener-cities.parquet");
        StringColumnFixture.write(parquet, allocator, CITY, cities);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeDocs(dir, cities.size());
            DirectoryReader in = DirectoryReader.open(dir);
            DirectoryReader wrapped = null;
            try {
                SegmentReader seg = (SegmentReader) in.leaves().get(0).reader();
                ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
                    parquet,
                    ParquetColumnReader.LOCAL_STORE,
                    Settings.EMPTY,
                    cities.size(),
                    null
                );
                ParquetSegmentResourceCache resourceCache = new ParquetSegmentResourceCache(null);
                resourceCache.cacheForTesting(seg, keywordResources(seg, producer));
                wrapped = ParquetDocValuesDirectoryReader.wrap(in, resourceCache);

                LeafReader view = ((ParquetDocValuesLeafReader) wrapped.leaves().get(0).reader()).perDocumentValuesReader();
                int handlesBefore = NativeHandle.liveHandleCount();
                readKeyword(view, 0, "delhi");
                assertEquals("the page opened one native cursor", 1, producer.cursorsOpened());

                hook.accept(new DerivedSourceCursorDrainingListener(), mockContextFor(wrapped));

                assertTrue(
                    "the hook freed the fetch thread's cursor (live handles back to baseline)",
                    NativeHandle.liveHandleCount() <= handlesBefore
                );
                assertTrue("the reader stays open after the hook", wrapped.getRefCount() > 0);
            } finally {
                if (wrapped != null && wrapped.getRefCount() > 0) {
                    wrapped.close();
                } else if (in.getRefCount() > 0) {
                    in.close();
                }
            }
        }
    }

    private static SearchContext mockContextFor(DirectoryReader reader) {
        ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        when(searcher.getDirectoryReader()).thenReturn(reader);
        SearchContext context = mock(SearchContext.class);
        when(context.searcher()).thenReturn(searcher);
        return context;
    }

    private static void readKeyword(LeafReader view, int doc, String expected) throws Exception {
        SortedSetDocValues ss = view.getSortedSetDocValues(CITY);
        assertTrue("doc " + doc + " must have a value", ss.advanceExact(doc));
        assertEquals(expected, ss.lookupOrd(ss.nextOrd()).utf8ToString());
    }

    private ParquetSegmentResources keywordResources(SegmentReader seg, ParquetDocValuesProducer producer) {
        FieldInfo fi = syntheticKeyword(CITY);
        return new ParquetSegmentResources(
            producer,
            Map.of(CITY, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            Set.of(),
            seg.getSegmentInfo().info
        );
    }

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

    private static FieldInfo syntheticKeyword(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED_SET,
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

    @FunctionalInterface
    private interface ThrowingHook {
        void accept(DerivedSourceCursorDrainingListener listener, SearchContext context) throws Exception;
    }
}
