/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
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
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A repeated binary stored column emits one {@code binaryField} per value in INGEST order (not sorted,
 * not deduped) - the read side of the write path storing each array element as its own
 * {@code StoredField}. Empty, null, and all-null lists emit nothing.
 */
public class ParquetBinaryListStoredFieldsTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "blob";

    public void testRepeatedBinaryEmitsOneFieldPerValueInIngestOrder() throws Exception {
        // doc 3 keeps duplicates and a descending pair to prove stored fields are neither deduped nor sorted.
        byte[][][] rows = { { b("ab"), b("cd") }, {}, null, { b("z"), b("z"), b("a") }, { null, b("x") } };
        try (Fixture f = new Fixture(rows)) {
            StoredFields fields = f.storedFields.wrap(f.leaf.storedFields());

            assertEquals("two values, in ingest order", List.of("ab", "cd"), capture(fields, 0));
            assertEquals("empty list emits nothing", List.of(), capture(fields, 1));
            assertEquals("null list emits nothing", List.of(), capture(fields, 2));
            assertEquals("duplicates kept, order preserved (not sorted/deduped)", List.of("z", "z", "a"), capture(fields, 3));
            assertEquals("null element dropped, present one kept", List.of("x"), capture(fields, 4));

            assertEquals("one list cursor serves the whole request", 1, f.request.listOpened().size());
            assertTrue("a repeated column never opens a scalar cursor", f.request.opened().isEmpty());
        }
    }

    private List<String> capture(StoredFields fields, int doc) throws IOException {
        Capture visitor = new Capture(COLUMN);
        fields.document(doc, visitor);
        List<String> out = new ArrayList<>();
        for (byte[] value : visitor.values) {
            out.add(new String(value, StandardCharsets.UTF_8));
        }
        return out;
    }

    private final class Fixture implements AutoCloseable {
        final Directory dir = newDirectory();
        final IndexWriter writer;
        final DirectoryReader reader;
        final LeafReader leaf;
        final ParquetDocValuesProducer producer;
        final CursorRegistry request = new CursorRegistry();
        final ParquetStoredFields storedFields;

        Fixture(byte[][][] rows) throws Exception {
            Path file = createTempDir().resolve("stored-list.parquet");
            BinaryListColumnFixture.write(file, allocator, COLUMN, rows);
            producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows.length, null);

            writer = new IndexWriter(dir, new IndexWriterConfig());
            for (int i = 0; i < rows.length; i++) {
                Document doc = new Document();
                doc.add(new StoredField("id", "doc-" + i));
                writer.addDocument(doc);
            }
            writer.commit();
            reader = DirectoryReader.open(dir);
            assertEquals("fixture must be a single segment so docId == row", 1, reader.leaves().size());
            leaf = reader.leaves().get(0).reader();

            FieldInfo fi = binaryField(COLUMN, rows.length + 1);
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

    /** Records every binary value offered for the wanted field, in the order the overlay emits them. */
    private static final class Capture extends StoredFieldVisitor {
        final List<byte[]> values = new ArrayList<>();
        private final String wanted;

        Capture(String wanted) {
            this.wanted = wanted;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            return fieldInfo.name.equals(wanted) ? Status.YES : Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            values.add(value);
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static FieldInfo binaryField(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            true,
            false,
            IndexOptions.NONE,
            DocValuesType.BINARY,
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
