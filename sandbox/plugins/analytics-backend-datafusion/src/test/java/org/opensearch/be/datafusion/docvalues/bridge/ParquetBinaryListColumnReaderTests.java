/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.BinaryListColumnFixture;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * End-to-end coverage for the repeated binary read path at the bridge level: writes a
 * {@code list<binary>} Parquet fixture, then reads each row's byte-slice list back through
 * {@link ParquetListColumnReader}, whose child {@link DecodedBatch} is a {@code KIND_BINARY} view read
 * with {@link DecodedBatch#bytesAt}.
 */
public class ParquetBinaryListColumnReaderTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "blob";

    public void testReadsPerRowBinaryValueLists() throws Exception {
        byte[][][] rows = {
            { bytes("ab"), bytes("cde"), bytes("f") },
            { bytes("gg") },
            {},
            { bytes(""), bytes("hij") } };
        Path file = createTempDir().resolve("binary-lists.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            BytesRef scratch = new BytesRef();
            for (int row = 0; row < rows.length; row++) {
                reader.loadListBatchContaining(row);
                DecodedListBatch batch = reader.decodedListBatch();
                assertTrue("row " + row + " must be in the batch", batch.contains(row));
                int start = batch.startOffset(row);
                int end = batch.endOffset(row);
                assertEquals("row " + row + " list width", rows[row].length, end - start);
                DecodedBatch children = batch.childValues();
                for (int i = 0; i < rows[row].length; i++) {
                    assertTrue("row " + row + " value " + i + " present", children.isPresent(start + i));
                    children.bytesAt(start + i, scratch);
                    assertArrayEquals(
                        "row " + row + " value " + i,
                        rows[row][i],
                        Arrays.copyOfRange(scratch.bytes, scratch.offset, scratch.offset + scratch.length)
                    );
                }
            }
        }
    }

    /** A null element inside a non-empty binary list reports absent at its index, present at its neighbours. */
    public void testNullElementInsideBinaryListIsAbsentWhileNeighboursPresent() throws Exception {
        byte[][][] rows = { { bytes("x"), null, bytes("yz") } };
        Path file = createTempDir().resolve("null-element.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            reader.loadListBatchContaining(0);
            DecodedListBatch batch = reader.decodedListBatch();
            int start = batch.startOffset(0);
            assertEquals("row 0 list width", 3, batch.endOffset(0) - start);
            DecodedBatch children = batch.childValues();
            assertTrue("first element present", children.isPresent(start));
            assertFalse("middle element null", children.isPresent(start + 1));
            assertTrue("last element present", children.isPresent(start + 2));

            BytesRef scratch = new BytesRef();
            children.bytesAt(start, scratch);
            assertArrayEquals(bytes("x"), Arrays.copyOfRange(scratch.bytes, scratch.offset, scratch.offset + scratch.length));
            children.bytesAt(start + 2, scratch);
            assertArrayEquals(bytes("yz"), Arrays.copyOfRange(scratch.bytes, scratch.offset, scratch.offset + scratch.length));
        }
    }

    /** A null list row and an empty list row are both zero-width; a batch of only empties has no child values. */
    public void testNullAndEmptyBinaryListRowsAreZeroWidth() throws Exception {
        byte[][][] rows = { null, {}, {} };
        Path file = createTempDir().resolve("null-vs-empty.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rows.length; row++) {
                reader.loadListBatchContaining(row);
                DecodedListBatch batch = reader.decodedListBatch();
                assertTrue("row " + row + " must be in the batch", batch.contains(row));
                assertEquals("row " + row + " must be zero-width", 0, batch.endOffset(row) - batch.startOffset(row));
            }
        }
    }

    /** A backward request after a forward scan across a batch boundary reopens the cursor and re-reads bytes. */
    public void testReopenReReadsEarlierRowAfterCrossingBatchBoundary() throws Exception {
        byte[][][] rows = new byte[10][][];
        for (int r = 0; r < rows.length; r++) {
            rows[r] = new byte[][] { bytes("v" + r), bytes("w" + r) };
        }
        Path file = createTempDir().resolve("reopen.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

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

    private void assertRow(ParquetListColumnReader reader, byte[][][] rows, int row) throws IOException {
        reader.loadListBatchContaining(row);
        DecodedListBatch batch = reader.decodedListBatch();
        assertTrue("row " + row + " must be in the batch", batch.contains(row));
        int start = batch.startOffset(row);
        assertEquals("row " + row + " list width", rows[row].length, batch.endOffset(row) - start);
        DecodedBatch children = batch.childValues();
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < rows[row].length; i++) {
            assertTrue(children.isPresent(start + i));
            children.bytesAt(start + i, scratch);
            assertArrayEquals(
                "row " + row + " value " + i,
                rows[row][i],
                Arrays.copyOfRange(scratch.bytes, scratch.offset, scratch.offset + scratch.length)
            );
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
