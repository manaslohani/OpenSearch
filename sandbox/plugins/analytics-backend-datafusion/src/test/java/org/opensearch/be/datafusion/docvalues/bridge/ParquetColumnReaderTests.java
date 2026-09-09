/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.be.datafusion.DatafusionSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

/**
 * End-to-end coverage for the numeric Parquet doc-values read bridge: reads committed Parquet
 * fixtures (written once by the format plugin's writer; see src/test/resources/docvalues/README.md)
 * through the FFM zero-copy borrow path (Java -> native Rust cursor -> Arrow decode -> borrowed
 * buffers read back in Java).
 *
 * <p>Opening a cursor needs the DataFusion runtime manager and the global file-metadata cache the
 * analytics-backend-datafusion plugin owns, so each test starts a runtime rather than the reader
 * falling back to a private pool and cache of its own. Thread-leak detection is off because the
 * Tokio runtime manager is a per-JVM singleton whose threads outlive any one test class.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ParquetColumnReaderTests extends OpenSearchTestCase {

    private static final String COLUMN = "value";

    private long globalRuntimePtr;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Idempotent: the manager is a OnceLock, so another test class may already have started it.
        // Deliberately never shut down - doing so kills the shared executor for the rest of the JVM.
        DataFusionRuntimeFixture.initRuntimeManager(2);
        globalRuntimePtr = DataFusionRuntimeFixture.createGlobalRuntime(createTempDir("datafusion-spill"));
        assertNotEquals("global runtime must start before a cursor can be opened", 0L, globalRuntimePtr);
    }

    @Override
    public void tearDown() throws Exception {
        if (globalRuntimePtr != 0L) {
            DataFusionRuntimeFixture.closeGlobalRuntime(globalRuntimePtr);
        }
        super.tearDown();
    }

    private static long expected(long row) {
        return row * 7 + 1;
    }

    /**
     * Copies a committed fixture from src/test/resources/docvalues into a fresh temp dir, since the
     * native cursor opens files by filesystem path, not classpath. See the README there for what
     * each fixture holds and how to regenerate it.
     */
    private Path fixture(String name) throws Exception {
        Path target = createTempDir().resolve(name);
        try (java.io.InputStream in = getClass().getResourceAsStream("/docvalues/" + name)) {
            assertNotNull("missing test resource docvalues/" + name, in);
            java.nio.file.Files.copy(in, target);
        }
        return target;
    }

    public void testAscendingWalkReloadsBatches() throws Exception {
        int rowCount = 500;
        Path file = fixture("long_dense.parquet");

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (long row = 0; row < rowCount; row++) {
                DecodedBatch batch = reader.decodedBatch();
                if (batch == null || batch.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    batch = reader.decodedBatch();
                }
                assertTrue("row " + row + " should be in the batch", batch.contains(row));
                assertEquals(DecodedBatch.KIND_LONG, batch.valueKind());
                assertTrue("row " + row + " should be present", batch.isPresent(row));
                assertEquals("value at row " + row, expected(row), batch.valueAt(row));
            }
        }
    }

    public void testResidentRowIsServedWithoutMovingTheCursor() throws Exception {
        int rowCount = 500;
        Path file = fixture("long_dense.parquet");

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(100);
            long firstRow = reader.decodedBatch().firstRow();
            long lastRow = reader.decodedBatch().lastRow();

            // The native cursor parks at lastRow + 1, so each of these would be a backward seek if
            // it reached the native side.
            for (long row = firstRow; row <= lastRow; row++) {
                reader.loadBatchContaining(row);
                DecodedBatch batch = reader.decodedBatch();
                assertEquals("resident row must not reload the batch", firstRow, batch.firstRow());
                assertEquals("resident row must not reload the batch", lastRow, batch.lastRow());
                assertEquals("value at row " + row, expected(row), batch.valueAt(row));
            }
        }
    }

    public void testFailedLoadDoesNotRetainTheStaleBatch() throws Exception {
        int rowCount = 500;
        Path file = fixture("long_dense.parquet");

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(0);
            assertNotNull("a batch should be resident after a successful load", reader.decodedBatch());

            // A load past the end fails. The batch it would have replaced must not stay reachable,
            // because a successful native call frees the buffers the old batch borrowed.
            expectThrows(IOException.class, () -> reader.loadBatchContaining(rowCount));
            assertNull("no batch may remain resident after a failed load", reader.decodedBatch());
        }
    }

    public void testPresenceLookupOutsideTheBatchThrows() throws Exception {
        int rowCount = 500;
        Path file = fixture("long_sparse.parquet");

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(64);
            DecodedBatch batch = reader.decodedBatch();

            // The bitmap is byte-granular, so a row just past the batch can still land inside the
            // mapped bytes. It must be rejected rather than answered from a neighbouring bit.
            expectThrows(IndexOutOfBoundsException.class, () -> batch.isPresent(batch.lastRow() + 1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.isPresent(batch.firstRow() - 1));
        }
    }

    public void testForwardJumpAndBackwardReopen() throws Exception {
        int rowCount = 500;
        Path file = fixture("long_dense.parquet");

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(400);
            for (long row = 400; row <= 410; row++) {
                DecodedBatch batch = reader.decodedBatch();
                if (batch.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    batch = reader.decodedBatch();
                }
                assertTrue("row " + row + " should be present after forward jump", batch.isPresent(row));
                assertEquals("value at row " + row, expected(row), batch.valueAt(row));
            }

            reader.loadBatchContaining(10);
            DecodedBatch batch = reader.decodedBatch();
            assertTrue("row 10 should be in the batch after backward reopen", batch.contains(10));
            assertTrue("row 10 should be present", batch.isPresent(10));
            assertEquals("value at row 10", expected(10), batch.valueAt(10));
        }
    }

    public void testNullPresenceBitmap() throws Exception {
        int rowCount = 500;
        int nullEvery = 5;
        Path file = fixture("long_sparse.parquet");

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (long row = 0; row < rowCount; row++) {
                DecodedBatch batch = reader.decodedBatch();
                if (batch == null || batch.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    batch = reader.decodedBatch();
                }
                boolean expectNull = row % nullEvery == 0;
                if (expectNull) {
                    assertFalse("row " + row + " should be null", batch.isPresent(row));
                } else {
                    assertTrue("row " + row + " should be present", batch.isPresent(row));
                    assertEquals("value at row " + row, expected(row), batch.valueAt(row));
                }
            }
        }
    }

    /**
     * Registering a setting as {@code IndexScope} only makes it *settable* per index; this proves
     * the value is *honoured* per reader: two readers over the same file, opened with different
     * index settings (as two indices would open them), get different decode windows.
     */
    public void testIndexScopedBatchSizeSettingsAreHonouredPerReader() throws Exception {
        int rowCount = 500;
        Path file = fixture("long_dense.parquet");

        Settings small = Settings.builder()
            .put(DatafusionSettings.DOCVALUES_INITIAL_BATCH_SIZE.getKey(), 4)
            .put(DatafusionSettings.DOCVALUES_MAX_BATCH_SIZE.getKey(), 8)
            .build();

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN, small)) {
            reader.loadBatchContaining(0);
            DecodedBatch batch = reader.decodedBatch();
            assertTrue(batch.contains(0));
            assertTrue(batch.contains(3));
            assertFalse("initial window of 4 must not include row 4", batch.contains(4));

            // Dense walk: the adaptive window may grow, but never past max_batch_size = 8.
            for (long row = 0; row < rowCount; row++) {
                DecodedBatch current = reader.decodedBatch();
                if (current == null || current.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    current = reader.decodedBatch();
                }
                assertTrue("row " + row + " should be in the batch", current.contains(row));
                assertFalse("window must stay capped at max_batch_size=8 rows", current.contains(row + 8));
            }
        }

        // The same file under default settings (initial 32): row 4 IS resident after the first
        // load — the two readers diverge purely on the Settings they were opened with.
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(0);
            DecodedBatch batch = reader.decodedBatch();
            assertTrue("default initial window (32) must include row 4", batch.contains(4));
        }
    }

    public void testNegativeDoublesUseSortableEncoding() throws Exception {
        double[] values = { -100.5, -0.5, 0.0, 3.25, -2.75, 42.0, -1.0e300, 1.0e300 };
        Path file = fixture("numeric_kinds.parquet");
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, "d_double")) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_DOUBLE, batch.valueKind());
                // valueAt returns the sortable long; sortableLongToDouble must recover the original value.
                assertEquals("value at row " + row, values[row], NumericUtils.sortableLongToDouble(batch.valueAt(row)), 0.0);
            }
            // Negatives must sort below positives in the encoded long space (range/skipper consumers).
            DecodedBatch batch = loadRow(reader, 0);
            assertTrue("negative double must sort below positive", batch.valueAt(0) < batch.valueAt(5));
        }
    }

    public void testNegativeFloatsUseSignExtendedSortableEncoding() throws Exception {
        float[] values = { -100.5f, -0.5f, 0.0f, 3.25f, -2.75f, 42.0f, -3.4e38f, 3.4e38f };
        Path file = fixture("numeric_kinds.parquet");
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, "d_float")) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_FLOAT, batch.valueKind());
                assertEquals("value at row " + row, values[row], NumericUtils.sortableIntToFloat((int) batch.valueAt(row)), 0.0f);
            }
            // Sign-extended, so a negative float's long compares below a positive float's long.
            DecodedBatch batch = loadRow(reader, 0);
            assertTrue("negative float must sort below positive (sign-extended)", batch.valueAt(0) < batch.valueAt(5));
        }
    }

    private static DecodedBatch loadRow(ParquetColumnReader reader, long row) throws java.io.IOException {
        DecodedBatch batch = reader.decodedBatch();
        if (batch == null || batch.contains(row) == false) {
            reader.loadBatchContaining(row);
            batch = reader.decodedBatch();
        }
        return batch;
    }

    // Sign-extension coverage for KIND_INT: a signed 32-bit column must widen each stored int into
    // the identically-signed long. If the codec zero-extended it instead (the u32 path), -1 would
    // surface as 4294967295 and Integer.MIN_VALUE as 2147483648 - this test catches that swap.
    public void testIntColumnSignExtendsNegatives() throws Exception {
        long[] values = { -1L, Integer.MIN_VALUE, -987654L, 0L, 42L, Integer.MAX_VALUE, 7L, -7L };
        assertIntegralColumn("d_int", DecodedBatch.KIND_INT, values);
    }

    // Zero-extension coverage for KIND_UINT_BITS: an unsigned 32-bit column must widen each stored
    // u32 into a non-negative long. If the codec sign-extended it instead (the i32 path), any value
    // above Integer.MAX_VALUE - here 3000000000 and 4294967295 - would surface as a negative long.
    public void testUnsignedIntColumnZeroExtends() throws Exception {
        long[] values = { 0L, 42L, 3000000000L, 4294967295L, 7L, 1L, 2147483648L, 65536L };
        assertIntegralColumn("d_uint", DecodedBatch.KIND_UINT_BITS, values);
        // Explicitly guard the sign: zero-extension must never produce a negative long.
        try (ParquetColumnReader reader = ParquetColumnReader.open(fixture("numeric_kinds.parquet"), "d_uint")) {
            for (int row = 0; row < values.length; row++) {
                assertTrue("u32 value at row " + row + " must be non-negative", loadRow(reader, row).valueAt(row) >= 0L);
            }
        }
    }

    // Sign-extension coverage for KIND_SHORT and KIND_BYTE: a signed 16- or 8-bit column must widen
    // each stored value into the identically-signed long. A zero-extension bug would turn -1 into
    // 65535 (short) / 255 (byte) and the MIN_VALUEs into their positive u16/u8 counterparts.
    public void testShortAndByteColumnsSignExtend() throws Exception {
        long[] shortValues = { -1L, Short.MIN_VALUE, -1234L, 0L, 42L, Short.MAX_VALUE, 7L, -7L };
        assertIntegralColumn("d_short", DecodedBatch.KIND_SHORT, shortValues);

        long[] byteValues = { -1L, Byte.MIN_VALUE, -50L, 0L, 42L, Byte.MAX_VALUE, 7L, -7L };
        assertIntegralColumn("d_byte", DecodedBatch.KIND_BYTE, byteValues);
    }

    // Zero-extension coverage for KIND_USHORT and KIND_UBYTE: an unsigned 16- or 8-bit column must
    // widen each stored value into a non-negative long. A sign-extension bug would turn 50000 into
    // -15536 (u16) and 200 into -56 (u8) once the top bit is set.
    public void testUnsignedShortAndByteZeroExtend() throws Exception {
        long[] ushortValues = { 0L, 42L, 50000L, 65535L, 7L, 1L, 32768L, 256L };
        assertIntegralColumn("d_ushort", DecodedBatch.KIND_USHORT, ushortValues);

        long[] ubyteValues = { 0L, 42L, 200L, 255L, 7L, 1L, 128L, 16L };
        assertIntegralColumn("d_ubyte", DecodedBatch.KIND_UBYTE, ubyteValues);
    }

    /** Reads every row back and asserts both the mapped value kind and the exact widened long. */
    private void assertIntegralColumn(String column, int expectedKind, long[] values) throws Exception {
        try (ParquetColumnReader reader = ParquetColumnReader.open(fixture("numeric_kinds.parquet"), column)) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals("value kind", expectedKind, batch.valueKind());
                assertTrue("row " + row + " should be present", batch.isPresent(row));
                assertEquals("value at row " + row, values[row], batch.valueAt(row));
            }
        }
    }
}
