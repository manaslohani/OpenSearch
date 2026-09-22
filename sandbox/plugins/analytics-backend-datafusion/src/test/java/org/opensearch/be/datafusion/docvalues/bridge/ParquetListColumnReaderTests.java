/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * End-to-end coverage for the repeated (list) doc-values read path: writes a {@code list<int64>}
 * Parquet fixture, then reads each row's value list back through {@link ParquetListColumnReader}.
 */
public class ParquetListColumnReaderTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";

    public void testReadsPerRowValueLists() throws Exception {
        long[][] rows = { { 7, 2, 5 }, { 4, 1 }, { 9 }, {} };
        Path file = createTempDir().resolve("lists.parquet");
        writeLongListColumn(file, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rows.length; row++) {
                reader.loadListBatchContaining(row);
                DecodedListBatch batch = reader.decodedListBatch();
                assertTrue("row " + row + " must be in the batch", batch.contains(row));
                int start = batch.startOffset(row);
                int end = batch.endOffset(row);
                assertEquals("row " + row + " list width", rows[row].length, end - start);
                DecodedBatch children = batch.childValues();
                for (int i = 0; i < rows[row].length; i++) {
                    assertTrue(children.isPresent(start + i));
                    assertEquals("row " + row + " value " + i, rows[row][i], children.valueAt(start + i));
                }
            }
        }
    }

    /** A backward request after a forward scan across a batch boundary reopens the cursor and still reconstructs values. */
    public void testReopenReReadsEarlierRowAfterCrossingBatchBoundary() throws Exception {
        long[][] rows = new long[10][];
        for (int r = 0; r < rows.length; r++) {
            rows[r] = new long[] { r * 10L, r * 10L + 1 };
        }
        Path file = createTempDir().resolve("reopen.parquet");
        writeLongListColumn(file, rows);

        // Windows of at most 4 rows force several batches over 10 rows, so a step back crosses a boundary.
        Settings settings = Settings.builder()
            .put("index.parquet.docvalues.initial_batch_size", 2)
            .put("index.parquet.docvalues.max_batch_size", 4)
            .build();
        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN, settings)) {
            assertRow(reader, rows, 0);
            assertRow(reader, rows, 7); // forward across at least one boundary
            assertRow(reader, rows, 2); // backward: reopens the cursor
            assertRow(reader, rows, 9); // forward again after the reopen
        }
    }

    /** A null element inside a non-empty list reports absent at its index and present at its neighbours. */
    public void testNullElementInsideListIsAbsentWhileNeighboursPresent() throws Exception {
        Long[][] rows = { { 10L, null, 30L } };
        Path file = createTempDir().resolve("null-element.parquet");
        writeNullableLongListColumn(file, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            reader.loadListBatchContaining(0);
            DecodedListBatch batch = reader.decodedListBatch();
            int start = batch.startOffset(0);
            assertEquals("row 0 list width", 3, batch.endOffset(0) - start);
            DecodedBatch children = batch.childValues();
            assertTrue("first element present", children.isPresent(start));
            assertFalse("middle element null", children.isPresent(start + 1));
            assertTrue("last element present", children.isPresent(start + 2));
            assertEquals(10L, children.valueAt(start));
            assertEquals(30L, children.valueAt(start + 2));
        }
    }

    /** A null list row and an empty list row are indistinguishable: both present as a zero-width offset range. */
    public void testNullListRowAndEmptyListRowBothPresentAsZeroWidth() throws Exception {
        Long[][] rows = { null, {} };
        Path file = createTempDir().resolve("null-vs-empty.parquet");
        writeNullableLongListColumn(file, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            reader.loadListBatchContaining(0);
            DecodedListBatch batch = reader.decodedListBatch();
            assertEquals("null list row is zero-width", 0, batch.endOffset(0) - batch.startOffset(0));
            assertEquals("empty list row is zero-width", 0, batch.endOffset(1) - batch.startOffset(1));
            assertEquals("null and empty rows share the same offset boundary", batch.endOffset(0), batch.startOffset(1));
        }
    }

    /** A batch of only empty lists decodes with zero child values, so every row is a zero-width range. */
    public void testAllEmptyBatchDecodesToEmptyChildView() throws Exception {
        Long[][] rows = { {}, {}, {}, {}, {} };
        Path file = createTempDir().resolve("all-empty.parquet");
        writeNullableLongListColumn(file, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rows.length; row++) {
                reader.loadListBatchContaining(row);
                DecodedListBatch batch = reader.decodedListBatch();
                assertTrue("row " + row + " must be in the batch", batch.contains(row));
                assertEquals("row " + row + " must be empty", 0, batch.endOffset(row) - batch.startOffset(row));
            }
        }
    }

    private void assertRow(ParquetListColumnReader reader, long[][] rows, int row) throws IOException {
        reader.loadListBatchContaining(row);
        DecodedListBatch batch = reader.decodedListBatch();
        assertTrue("row " + row + " must be in the batch", batch.contains(row));
        int start = batch.startOffset(row);
        assertEquals("row " + row + " list width", rows[row].length, batch.endOffset(row) - start);
        DecodedBatch children = batch.childValues();
        for (int i = 0; i < rows[row].length; i++) {
            assertTrue("row " + row + " value " + i + " present", children.isPresent(start + i));
            assertEquals("row " + row + " value " + i, rows[row][i], children.valueAt(start + i));
        }
    }

    private void writeLongListColumn(Path file, long[][] rows) throws Exception {
        try (ListVector listVector = ListVector.empty(COLUMN, allocator)) {
            UnionListWriter writer = listVector.getWriter();
            for (int r = 0; r < rows.length; r++) {
                writer.setPosition(r);
                writer.startList();
                for (long v : rows[r]) {
                    writer.bigInt().writeBigInt(v);
                }
                writer.endList();
            }
            writer.setValueCount(rows.length);
            writeListColumn(file, listVector, rows.length);
        }
    }

    // A null outer row writes a null list; a null Long element writes a null child slot.
    private void writeNullableLongListColumn(Path file, Long[][] rows) throws Exception {
        try (ListVector listVector = ListVector.empty(COLUMN, allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Int(64, true)));
            BigIntVector data = (BigIntVector) listVector.getDataVector();
            int childCapacity = 0;
            for (Long[] row : rows) {
                if (row != null) {
                    childCapacity += row.length;
                }
            }
            listVector.allocateNew();
            data.allocateNew(Math.max(childCapacity, 1));
            int child = 0;
            for (int r = 0; r < rows.length; r++) {
                if (rows[r] == null) {
                    listVector.setNull(r);
                    continue;
                }
                listVector.startNewValue(r);
                for (Long v : rows[r]) {
                    if (v == null) {
                        data.setNull(child);
                    } else {
                        data.set(child, v);
                    }
                    child++;
                }
                listVector.endValue(r, rows[r].length);
            }
            data.setValueCount(child);
            listVector.setValueCount(rows.length);
            writeListColumn(file, listVector, rows.length);
        }
    }

    private void writeListColumn(Path file, ListVector listVector, int rowCount) throws Exception {
        Schema schema = new Schema(List.of(listVector.getField()));
        // Not AutoCloseable: flush() releases the native entry on success, cleanup() on any failure path.
        NativeParquetWriter parquetWriter = new NativeParquetWriter(file.toString());
        try {
            ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
            Data.exportSchema(allocator, schema, null, schemaExport);
            try (ArrowExport export = new ArrowExport(null, schemaExport)) {
                parquetWriter.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
            }

            try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(listVector), rowCount)) {
                ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
                ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
                try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                    parquetWriter.write(export.getArrayAddress(), export.getSchemaAddress());
                }
            }
            parquetWriter.flush();
        } finally {
            parquetWriter.cleanup();
        }
    }
}
