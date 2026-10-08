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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetUninvertedSortedDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.IdFieldMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link ParquetDocValuesLeafReader}'s doc-values routing. The first group covers the
 * branches that decide a field's fate before any value is read, and uses a null producer deliberately:
 * reaching the producer there would surface as a {@link NullPointerException}. The second group covers
 * which tier serves a healthy field, and needs a real producer over a fixture Parquet file.
 */
public class ParquetDocValuesLeafReaderTests extends DataFusionBackedTestCase {

    private static final String CITY = "city";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Another test in this JVM may have closed a plugin, which sets the ordinals cache's static
        // shuttingDown and makes every later build refuse.
        UninvertedOrdinalsCache.start();
    }

    /** A multi-valued keyword aggregation or sort is a client error, refused before any value is read. */
    public void testMultiValuedKeywordIsRefusedAsAClientError() throws Exception {
        ParquetSegmentResources resources = sortedSetResources("tags", Set.of("tags"));

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());

            // The null producer proves the refusal happens before any value is read: a path that touched
            // the producer would fail with NullPointerException instead of this client error.
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parquetLeaf.getSortedSetDocValues("tags"));
            assertTrue(e.getMessage(), e.getMessage().contains("multi-valued keyword field [tags]"));
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /** Keyword is served as SORTED_SET, so the plain SORTED accessor never matches a Parquet field. */
    public void testSortedAccessorReturnsNullForAParquetField() throws Exception {
        ParquetSegmentResources resources = sortedSetResources("tags", Set.of());

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());

            assertNull("SORTED accessor does not match a SORTED_SET Parquet field", parquetLeaf.getSortedDocValues("tags"));
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /** Source derivation must not fail the whole fetch on a multi-valued field; see the TODO in ParquetDocValuesLeafReader. */
    public void testSourceDerivationViewReturnsNullForAMultiValuedField() throws Exception {
        ParquetSegmentResources resources = sortedSetResources("tags", Set.of("tags"));

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());

            LeafReader sourceView = parquetLeaf.perDocumentValuesReader();
            SortedSetDocValues values = sourceView.getSortedSetDocValues("tags");
            assertNull("source derivation omits a multi-valued field rather than throwing", values);
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /**
     * A stored-only Parquet field such as {@code _id} carries dvType NONE in {@code parquetFields}; a
     * doc-values request for it must delegate to the underlying leaf (which has none) rather than reach
     * the producer. The null producer proves the delegation: a path that touched it would surface as a
     * {@link NullPointerException} instead of the underlying leaf's null.
     */
    public void testStoredOnlyFieldDelegatesDocValuesToUnderlyingLeaf() throws Exception {
        FieldInfo idFi = storedOnlyField(IdFieldMapper.CONTENT_TYPE);
        ParquetSegmentResources resources = new ParquetSegmentResources(
            null,
            Map.of(IdFieldMapper.CONTENT_TYPE, idFi),
            new FieldInfos(new FieldInfo[] { idFi }),
            Set.of(),
            null
        );

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());

            assertNull("_id binary DV delegates to the underlying leaf", parquetLeaf.getBinaryDocValues(IdFieldMapper.CONTENT_TYPE));
            assertNull("_id sorted DV delegates to the underlying leaf", parquetLeaf.getSortedDocValues(IdFieldMapper.CONTENT_TYPE));
            assertNull("_id numeric DV delegates to the underlying leaf", parquetLeaf.getNumericDocValues(IdFieldMapper.CONTENT_TYPE));
            assertNull(
                "_id sorted-numeric DV delegates to the underlying leaf",
                parquetLeaf.getSortedNumericDocValues(IdFieldMapper.CONTENT_TYPE)
            );
            assertNull("_id sorted-set DV delegates to the underlying leaf", parquetLeaf.getSortedSetDocValues(IdFieldMapper.CONTENT_TYPE));
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /**
     * Builds resources with a null producer holding one synthetic SORTED_SET {@link FieldInfo} for
     * {@code field}, the matching {@link FieldInfos}, the given multi-valued set, and a null segment info.
     */
    private static ParquetSegmentResources sortedSetResources(String field, Set<String> multiValuedFields) {
        FieldInfo fi = sortedSetField(field);
        return new ParquetSegmentResources(null, Map.of(field, fi), new FieldInfos(new FieldInfo[] { fi }), multiValuedFields, null);
    }

    // ------------------------------------------------------------------------------------------------
    // Which tier serves a healthy field. These need a real producer, so the Parquet column and the
    // Lucene postings must agree: the ordinals build refuses a field whose coverage does not match.
    // ------------------------------------------------------------------------------------------------

    /** With no value in the whole segment, empty doc values ARE the correct ordinals view; nothing is built. */
    public void testZeroNonNullRowsServesEmptyOrdinals() throws Exception {
        List<String> values = Arrays.asList(null, null, null);
        Path parquetFile = createTempDir().resolve("allnull.parquet");
        StringColumnFixture.write(parquetFile, allocator, CITY, values);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedDocValues dv = cityDocValues(leaf, parquetFile, values.size());
                assertEquals("no row carries a value, so the ordinals view is empty", 0, dv.getValueCount());
            }
        }
    }

    /** A filesystem segment gets an ord file, and the ordinals tier serves the field. */
    public void testLeasedOrdFileProducesTheOrdinalsBackedIterator() throws Exception {
        List<String> values = List.of("delhi", "mumbai", "pune");
        Path parquetFile = createTempDir().resolve("cities.parquet");
        StringColumnFixture.write(parquetFile, allocator, CITY, values);

        Path shardDir = createTempDir();
        try (Directory dir = openFsIndex(shardDir, "delhi", "mumbai", "pune")) {
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedDocValues dv = cityDocValues(leaf, parquetFile, values.size());

                assertTrue("a leased ord file must serve the field", dv instanceof ParquetUninvertedSortedDocValues);
                assertEquals("three distinct cities", 3, dv.getValueCount());
                try (var listing = Files.list(shardDir.resolve("parquet-ords"))) {
                    assertTrue("the built ord file must exist under the shard", listing.anyMatch(f -> f.toString().endsWith(".ord")));
                }
            }
        }
    }

    /** No ord file can exist for a non-filesystem segment, so the streaming tier serves the field unchanged. */
    public void testNoLeaseFallsBackToTheStreamingIterator() throws Exception {
        List<String> values = List.of("delhi", "mumbai", "pune");
        Path parquetFile = createTempDir().resolve("cities-nofs.parquet");
        StringColumnFixture.write(parquetFile, allocator, CITY, values);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedDocValues dv = cityDocValues(leaf, parquetFile, values.size());
                assertTrue("without an ord file the streaming tier serves the field", dv instanceof ParquetSortedDocValues);
            }
        }
    }

    /** The field as an aggregation receives it: the leaf reader's SORTED_SET answer with its singleton unwrapped. */
    private SortedDocValues cityDocValues(SegmentReader leaf, Path parquetFile, int maxDoc) throws Exception {
        FieldInfo fi = sortedSetField(CITY);
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
            parquetFile,
            ParquetColumnReader.LOCAL_STORE,
            Settings.EMPTY,
            maxDoc,
            null
        );
        ParquetSegmentResources resources = new ParquetSegmentResources(
            producer,
            Map.of(CITY, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            Set.of(),
            leaf.getSegmentInfo().info
        );
        ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());
        return DocValues.unwrapSingleton(parquetLeaf.getSortedSetDocValues(CITY));
    }

    /** Opens a filesystem index at {@code <shardDir>/index}, so ord files resolve to {@code <shardDir>/parquet-ords}. */
    private static Directory openFsIndex(Path shardDir, String... cities) throws Exception {
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        Directory directory = FSDirectory.open(storeDir);
        writeCities(directory, cities);
        return directory;
    }

    /** One document per city, carrying postings for the field the ordinals build reads. */
    private static void writeCities(Directory dir, String... cities) throws Exception {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (String city : cities) {
                Document document = new Document();
                document.add(new StringField(CITY, city, Field.Store.NO));
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            writer.commit();
        }
    }

    /** A synthetic SORTED_SET field info, matching what the resources builder synthesizes for keyword. */
    private static FieldInfo sortedSetField(String name) {
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

    /** A stored-only field info: indexed (DOCS), no doc values - the shape of the real _id overlay entry. */
    private static FieldInfo storedOnlyField(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.DOCS,
            DocValuesType.NONE,
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

    private static IndexWriter singleDocWriter(Directory dir) throws Exception {
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        return writer;
    }
}
