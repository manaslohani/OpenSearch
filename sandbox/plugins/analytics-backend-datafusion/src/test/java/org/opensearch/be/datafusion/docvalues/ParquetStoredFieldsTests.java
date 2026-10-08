/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Drives {@link ParquetStoredFields} over a Parquet fixture paired with a Lucene segment of the same
 * size: Parquet stored fields are offered after the segment's own, absent rows yield nothing, the
 * visitor's answers are honoured, and one cursor serves every document of a request.
 */
public class ParquetStoredFieldsTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "blob";
    private static final int ROWS = 40;
    private static final int NULL_EVERY = 5;

    public void testOffersParquetFieldsAfterLuceneFieldsAndSkipsAbsentRows() throws Exception {
        try (Fixture f = new Fixture()) {
            StoredFields fields = f.storedFields.wrap(f.leaf.storedFields());
            for (int doc = 0; doc < ROWS; doc++) {
                Capture visitor = new Capture(Set.of("id", COLUMN));
                fields.document(doc, visitor);
                assertEquals("Lucene's own field at doc " + doc, "doc-" + doc, visitor.values.get("id"));
                if (doc % NULL_EVERY == 0) {
                    assertFalse("absent row yields no stored field at doc " + doc, visitor.values.containsKey(COLUMN));
                } else {
                    assertArrayEquals("value at doc " + doc, BinaryColumnFixture.valueAt(doc), (byte[]) visitor.values.get(COLUMN));
                }
            }
            assertEquals("one cursor serves the whole request", 1, f.request.opened().size());
        }
    }

    public void testHonoursTheVisitorsAnswer() throws Exception {
        try (Fixture f = new Fixture()) {
            StoredFields fields = f.storedFields.wrap(f.leaf.storedFields());

            Capture onlyId = new Capture(Set.of("id"));
            fields.document(1, onlyId);
            assertEquals(Set.of("id"), onlyId.values.keySet());
            assertTrue("a field the visitor does not want opens no cursor", f.request.opened().isEmpty());

            Capture stop = new Capture(Set.of());
            stop.stopOn = COLUMN;
            fields.document(1, stop);
            assertTrue(stop.values.isEmpty());
            assertTrue("STOP opens no cursor", f.request.opened().isEmpty());
        }
    }

    public void testSequentialReaderAndBackwardAccess() throws Exception {
        try (Fixture f = new Fixture()) {
            StoredFieldsReader in = ((CodecReader) f.leaf).getFieldsReader();
            StoredFieldsReader reader = f.storedFields.wrap(in);
            for (int doc : new int[] { 31, 2, 39 }) { // forward, backward (reopens the cursor), forward again
                Capture visitor = new Capture(Set.of(COLUMN));
                reader.document(doc, visitor);
                assertArrayEquals("value at doc " + doc, BinaryColumnFixture.valueAt(doc), (byte[]) visitor.values.get(COLUMN));
            }
            Capture viaClone = new Capture(Set.of(COLUMN));
            reader.clone().document(4, viaClone);
            assertArrayEquals(BinaryColumnFixture.valueAt(4), (byte[]) viaClone.values.get(COLUMN));
            Capture viaMerge = new Capture(Set.of(COLUMN));
            reader.getMergeInstance().document(6, viaMerge);
            assertArrayEquals(BinaryColumnFixture.valueAt(6), (byte[]) viaMerge.values.get(COLUMN));
            assertEquals("the random and sequential forms share the request's cursor", 1, f.request.opened().size());
        }
    }

    /**
     * The {@code _id} metadata field is served from its Parquet column exactly like a user binary field,
     * so a real {@link FieldsVisitor} - the server's fetch-phase visitor - recovers the document id via
     * {@code binaryField -> Uid.decodeId}. An absent row yields a null id, as for a missing stored field.
     */
    public void testServesIdColumnThroughRealFieldsVisitor() throws Exception {
        try (IdFixture f = new IdFixture()) {
            StoredFields fields = f.storedFields.wrap(f.leaf.storedFields());
            for (int doc = 0; doc < ROWS; doc++) {
                FieldsVisitor visitor = new FieldsVisitor(false);
                fields.document(doc, visitor);
                if (doc % NULL_EVERY == 0) {
                    assertNull("absent _id row must yield a null id at doc " + doc, visitor.id());
                } else {
                    assertEquals("decoded _id at doc " + doc, "doc-" + doc, visitor.id());
                }
            }
            assertEquals("one cursor serves the whole request", 1, f.request.opened().size());
        }
    }

    /** The raw {@code Uid.encodeId} bytes stored in the {@code _id} column at a present row. */
    private static byte[] encodedId(int row) {
        BytesRef encoded = Uid.encodeId("doc-" + row);
        return Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length);
    }

    /** A Parquet {@code _id} column of {@link #ROWS} rows beside a one-segment Lucene index that does not store {@code _id}. */
    private final class IdFixture implements AutoCloseable {
        final Directory dir = newDirectory();
        final IndexWriter writer;
        final DirectoryReader reader;
        final LeafReader leaf;
        final ParquetDocValuesProducer producer;
        final CursorRegistry request = new CursorRegistry();
        final ParquetStoredFields storedFields;

        IdFixture() throws Exception {
            Path file = createTempDir().resolve("id.parquet");
            byte[][] values = new byte[ROWS][];
            for (int i = 0; i < ROWS; i++) {
                values[i] = (i % NULL_EVERY == 0) ? null : encodedId(i);
            }
            BinaryColumnFixture.writeValues(file, allocator, IdFieldMapper.CONTENT_TYPE, values, true);
            producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, ROWS, null);

            // The Lucene secondary does not store _id (Store.NO), so the overlay is the only source: a doc
            // per row with no _id stored field mirrors a composite segment.
            writer = new IndexWriter(dir, new IndexWriterConfig());
            for (int i = 0; i < ROWS; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            reader = DirectoryReader.open(dir);
            assertEquals("fixture must be a single segment so docId == row", 1, reader.leaves().size());
            leaf = reader.leaves().get(0).reader();

            // dvType NONE mirrors the stored-only synthetic FieldInfo the build loop makes for _id; the
            // overlay is name-keyed, so the doc-values type is irrelevant to serving the column.
            FieldInfo fi = idField(IdFieldMapper.CONTENT_TYPE, ROWS + 1);
            ParquetSegmentResources resources = new ParquetSegmentResources(
                producer,
                Map.of(IdFieldMapper.CONTENT_TYPE, fi),
                new FieldInfos(new FieldInfo[] { fi }),
                Set.of(),
                null,
                Map.of(IdFieldMapper.CONTENT_TYPE, StoredFieldMapping.Kind.BINARY)
            );
            storedFields = new ParquetStoredFields(resources, request);
        }

        @Override
        public void close() throws IOException {
            request.close();
            producer.close();
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /** A Parquet column of {@link #ROWS} rows beside a one-segment Lucene index of {@link #ROWS} stored ids. */
    private final class Fixture implements AutoCloseable {
        final Directory dir = newDirectory();
        final IndexWriter writer;
        final DirectoryReader reader;
        final LeafReader leaf;
        final ParquetDocValuesProducer producer;
        final CursorRegistry request = new CursorRegistry();
        final ParquetStoredFields storedFields;

        Fixture() throws Exception {
            Path file = createTempDir().resolve("stored.parquet");
            BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);
            producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, ROWS, null);

            writer = new IndexWriter(dir, new IndexWriterConfig());
            for (int i = 0; i < ROWS; i++) {
                Document doc = new Document();
                doc.add(new StoredField("id", "doc-" + i));
                writer.addDocument(doc);
            }
            writer.commit();
            reader = DirectoryReader.open(dir);
            assertEquals("fixture must be a single segment so docId == row", 1, reader.leaves().size());
            leaf = reader.leaves().get(0).reader();

            FieldInfo fi = binaryField(COLUMN, ROWS + 1);
            ParquetSegmentResources resources = new ParquetSegmentResources(
                producer,
                Map.of(COLUMN, fi),
                new FieldInfos(new FieldInfo[] { fi }),
                Set.of(),
                null,
                Map.of(COLUMN, StoredFieldMapping.Kind.BINARY)
            );
            storedFields = new ParquetStoredFields(resources, request);
        }

        @Override
        public void close() throws IOException {
            request.close();
            producer.close();
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /** Records the values offered for the wanted fields; optionally answers STOP for one field. */
    private static final class Capture extends StoredFieldVisitor {
        final Map<String, Object> values = new LinkedHashMap<>();
        private final Set<String> wanted;
        String stopOn;

        Capture(Set<String> wanted) {
            this.wanted = wanted;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if (fieldInfo.name.equals(stopOn)) {
                return Status.STOP;
            }
            return wanted.contains(fieldInfo.name) ? Status.YES : Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            values.put(fieldInfo.name, value);
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) {
            values.put(fieldInfo.name, value);
        }
    }

    private static FieldInfo binaryField(String name, int number) {
        return fieldInfo(name, number, DocValuesType.BINARY);
    }

    /** The stored-only synthetic shape the build loop makes for {@code _id}: no doc values. */
    private static FieldInfo idField(String name, int number) {
        return fieldInfo(name, number, DocValuesType.NONE);
    }

    private static FieldInfo fieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
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
