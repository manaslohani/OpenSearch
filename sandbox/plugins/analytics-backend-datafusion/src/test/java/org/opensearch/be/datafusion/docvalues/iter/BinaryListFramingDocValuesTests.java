/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.BinaryListColumnFixture;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetListColumnReader;
import org.opensearch.index.mapper.BinaryFieldMapper;

import java.nio.file.Path;
import java.util.Arrays;

/**
 * {@link BinaryListFramingDocValues} must frame a repeated binary column byte-identically to a vanilla
 * multi-valued {@code binary} field. The decisive assertion builds the expected frame with the server's
 * own {@code BinaryFieldMapper.CustomBinaryDocValuesField} (on the classpath) rather than a
 * re-implementation, so a divergence in the sort, the dedup, or the vInt header is caught.
 */
public class BinaryListFramingDocValuesTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "blob";

    /** The exact bytes a vanilla binary field would store for {@code values} (sorted unsigned, deduped, framed). */
    private static byte[] vanillaFrame(byte[]... values) {
        BinaryFieldMapper.CustomBinaryDocValuesField field = null;
        for (byte[] value : values) {
            if (field == null) {
                field = new BinaryFieldMapper.CustomBinaryDocValuesField(COLUMN, value);
            } else {
                field.add(value);
            }
        }
        BytesRef ref = field.binaryValue();
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }

    private static byte[] frameOf(BinaryListFramingDocValues dv, int doc) throws Exception {
        assertTrue("doc " + doc + " must have a value", dv.advanceExact(doc));
        BytesRef out = dv.binaryValue();
        return Arrays.copyOfRange(out.bytes, out.offset, out.offset + out.length);
    }

    public void testFramesEachRowExactlyLikeTheVanillaBinaryField() throws Exception {
        byte[][][] rows = {
            { b(0x0A, 0x0B), b(0x01) },                 // two values, already ascending
            { b(0x7F, 0x00), b(0x03), b(0x03) },        // a duplicate to dedup
            { b((byte) 0xFF), b(0x00) },                // unsigned order: 0x00 sorts before 0xFF
            { b() } };                                  // a single empty value is still one value
        Path file = createTempDir().resolve("frame.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, rows.length);
            for (int doc = 0; doc < rows.length; doc++) {
                assertArrayEquals("row " + doc + " frame must match the vanilla binary field", vanillaFrame(rows[doc]), frameOf(dv, doc));
            }
        }
    }

    public void testSingleElementListFramesAsCountOne() throws Exception {
        byte[][][] rows = { { b(0x0A, 0x0B, 0x0C) } };
        Path file = createTempDir().resolve("single.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, rows.length);
            byte[] frame = frameOf(dv, 0);
            // vInt(1) = 0x01, vInt(3) = 0x03, then the three value bytes.
            assertArrayEquals(new byte[] { 0x01, 0x03, 0x0A, 0x0B, 0x0C }, frame);
            assertArrayEquals(vanillaFrame(rows[0]), frame);
        }
    }

    public void testEmptyListNullListAndAllNullListCarryNoValue() throws Exception {
        byte[][][] rows = {
            {},                          // empty list
            null,                        // null list
            { null, null },              // list of only null elements
            { b(0x05) } };               // a real value, so the reader is exercised past the empties
        Path file = createTempDir().resolve("absent.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, rows.length);
            assertFalse("empty list carries no value", dv.advanceExact(0));
            assertFalse("null list carries no value", dv.advanceExact(1));
            assertFalse("all-null list carries no value", dv.advanceExact(2));
            assertTrue("the real value is still readable", dv.advanceExact(3));
            BytesRef out = dv.binaryValue();
            assertArrayEquals(vanillaFrame(rows[3]), Arrays.copyOfRange(out.bytes, out.offset, out.offset + out.length));
        }
    }

    public void testNullElementsAreDroppedBeforeFraming() throws Exception {
        // Only the two non-null values reach the frame, sorted+deduped like the vanilla field over them.
        byte[][][] rows = { { b(0x09), null, b(0x02), null } };
        Path file = createTempDir().resolve("null-mixed.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, rows.length);
            assertArrayEquals(vanillaFrame(b(0x09), b(0x02)), frameOf(dv, 0));
        }
    }

    /** advance() must skip rows that carry no value and stop at the first that does. */
    public void testAdvanceSkipsValuelessRows() throws Exception {
        byte[][][] rows = { {}, null, { b(0x01) } };
        Path file = createTempDir().resolve("advance.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, rows.length);
            assertEquals("advance skips the empty and null rows", 2, dv.advance(0));
            assertEquals(2, dv.docID());
        }
    }

    private static byte[] b(int... bytes) {
        byte[] out = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            out[i] = (byte) bytes[i];
        }
        return out;
    }
}
