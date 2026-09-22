/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.DatafusionSettings;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

/**
 * Repeated (list) column reader over the same forward-only cursor as {@link ParquetColumnReader},
 * reading through {@link ParquetCodecBridge#nextListBatch}. The resident {@link DecodedListBatch}
 * holds borrowed Arrow buffers valid only until the next batch call. Scope and lifecycle mirror
 * {@link ParquetColumnReader}; the child buffer is sized by child-value count, not row count.
 */
public final class ParquetListColumnReader extends NativeHandle implements ListValueReader {

    private static final Logger LOGGER = LogManager.getLogger(ParquetListColumnReader.class);

    /** Store pointer meaning "read from the local filesystem" (every hot shard); a warm shard passes its own. */
    public static final long LOCAL_STORE = 0L;

    /** Number of scalar out-parameters {@code nextListBatch} writes back. */
    private static final int OUT_PARAM_COUNT = 9;

    private final Path file;
    private final String column;

    /**
     * Ceiling on the rows the native cursor may return, captured once at open. A value rather than a
     * live setting lookup, so a dynamic setting change cannot reject a batch the cursor was told to produce.
     */
    private final int maxBatchSize;

    private DecodedListBatch decodedListBatch;

    private ParquetListColumnReader(long handle, Path file, String column, int maxBatchSize) {
        super(handle);
        this.file = file;
        this.column = column;
        this.maxBatchSize = maxBatchSize;
    }

    /** Opens a list cursor over a local file using the default batch-size settings. */
    public static ParquetListColumnReader open(Path file, String column) throws IOException {
        return open(file, column, Settings.EMPTY);
    }

    /** Opens a list cursor over a local file, sized from {@code settings}. */
    public static ParquetListColumnReader open(Path file, String column, Settings settings) throws IOException {
        return open(file, column, settings, LOCAL_STORE);
    }

    /**
     * Opens a list cursor sized from {@code index.parquet.docvalues.initial_batch_size} and
     * {@code index.parquet.docvalues.max_batch_size}.
     *
     * @param settings index settings, or {@link Settings#EMPTY} to take the defaults
     * @param storePtr native object store to read through, or {@link #LOCAL_STORE} for a local file
     */
    public static ParquetListColumnReader open(Path file, String column, Settings settings, long storePtr) throws IOException {
        long handle = ParquetCodecBridge.openColumnCursor(
            file.toString(),
            column,
            DatafusionSettings.docValuesInitialBatchSize(settings),
            DatafusionSettings.docValuesMaxBatchSize(settings),
            storePtr
        );
        return new ParquetListColumnReader(handle, file, column, DatafusionSettings.docValuesMaxBatchSize(settings));
    }

    @Override
    public DecodedListBatch decodedListBatch() {
        return decodedListBatch;
    }

    /**
     * Ensures the resident batch contains {@code row}: a resident row is served as-is, a row ahead
     * advances the cursor, and a row behind reopens it first.
     */
    @Override
    public void loadListBatchContaining(long row) throws IOException {
        ensureOpen();
        DecodedListBatch current = decodedListBatch;
        if (current != null) {
            // The native cursor parks past the resident batch, so re-requesting a row it already
            // holds would reach the native side as a backward seek and be rejected.
            if (current.contains(row)) {
                return;
            }
            if (row < current.firstRow()) {
                reopen();
            }
        }
        loadListBatch(row);
    }

    /** Replaces the forward-only cursor with a fresh one at row zero. Only reached on a backward request. */
    private void reopen() throws IOException {
        decodedListBatch = null;
        ParquetCodecBridge.resetColumnCursor(ptr);
    }

    private void loadListBatch(long row) throws IOException {
        long firstRow;
        long lastRow;
        long valuesAddr;
        long validityAddr;
        int kind;
        int bitOffset;
        int valueBitOffset;
        long offsetsAddr;
        long valueCount;

        // Drop the resident batch first: a successful native call frees the buffers it borrowed, so
        // no early exit below may leave a DecodedListBatch whose views address freed memory.
        decodedListBatch = null;

        // The scalar out-parameters are tiny and read out immediately, so a per-call arena suffices;
        // the borrowed buffers live in Rust-owned memory and are reinterpreted separately below.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG, OUT_PARAM_COUNT);
            MemorySegment firstRowOut = out.asSlice(0L, Long.BYTES);
            MemorySegment lastRowOut = out.asSlice(Long.BYTES, Long.BYTES);
            MemorySegment valuesAddrOut = out.asSlice(2L * Long.BYTES, Long.BYTES);
            MemorySegment validityAddrOut = out.asSlice(3L * Long.BYTES, Long.BYTES);
            MemorySegment validityBitOffsetOut = out.asSlice(4L * Long.BYTES, Long.BYTES);
            MemorySegment valueKindOut = out.asSlice(5L * Long.BYTES, Long.BYTES);
            MemorySegment valueBitOffsetOut = out.asSlice(6L * Long.BYTES, Long.BYTES);
            MemorySegment offsetsAddrOut = out.asSlice(7L * Long.BYTES, Long.BYTES);
            MemorySegment valueCountOut = out.asSlice(8L * Long.BYTES, Long.BYTES);

            long rc = ParquetCodecBridge.nextListBatch(
                ptr,
                row,
                firstRowOut,
                lastRowOut,
                valuesAddrOut,
                validityAddrOut,
                validityBitOffsetOut,
                valueKindOut,
                valueBitOffsetOut,
                offsetsAddrOut,
                valueCountOut
            );
            checkStatus(rc, row);

            firstRow = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
            lastRow = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
            valuesAddr = valuesAddrOut.get(ValueLayout.JAVA_LONG, 0);
            validityAddr = validityAddrOut.get(ValueLayout.JAVA_LONG, 0);
            bitOffset = (int) validityBitOffsetOut.get(ValueLayout.JAVA_LONG, 0);
            kind = (int) valueKindOut.get(ValueLayout.JAVA_LONG, 0);
            valueBitOffset = (int) valueBitOffsetOut.get(ValueLayout.JAVA_LONG, 0);
            offsetsAddr = offsetsAddrOut.get(ValueLayout.JAVA_LONG, 0);
            valueCount = valueCountOut.get(ValueLayout.JAVA_LONG, 0);
        }

        // Validate framing before pointing views at the borrowed buffers: reinterpret() is unbounded,
        // so these checks fail fast on a malformed contract instead of reading out of bounds.
        if (firstRow < 0 || lastRow < firstRow || row < firstRow || row > lastRow) {
            throw contractViolation(row, "row range [" + firstRow + ", " + lastRow + "]");
        }
        long batchRowsLong = lastRow - firstRow + 1;
        if (batchRowsLong > maxBatchSize) {
            throw contractViolation(row, batchRowsLong + " rows exceeds cap " + maxBatchSize);
        }
        if (valueCount < 0 || valueCount > Integer.MAX_VALUE || offsetsAddr == 0 || bitOffset < 0 || valueBitOffset < 0) {
            throw contractViolation(row, "value count " + valueCount + ", offsets address " + offsetsAddr + ", bit offset " + bitOffset);
        }
        // A non-empty child buffer must have a values address; an all-empty batch (every list empty)
        // legitimately backs zero child values and may hand over a null values pointer.
        if (valueCount > 0 && valuesAddr == 0) {
            throw contractViolation(row, "value count " + valueCount + " with null values address");
        }
        int batchRows = (int) batchRowsLong;
        int children = (int) valueCount;

        // Borrowed Arrow buffers, valid until the next batch call. Offsets hold batchRows + 1 i32
        // entries; the child buffers are sized by child-value count, since one row backs a list.
        MemorySegment offsets = MemorySegment.ofAddress(offsetsAddr).reinterpret((long) (batchRows + 1) * Integer.BYTES);
        // The final offset bounds the child values any row reads; the native cursor sizes the child
        // buffer to valueCount, so an offset past it would read beyond the borrowed buffer.
        int lastOffset = offsets.getAtIndex(ValueLayout.JAVA_INT, batchRows);
        if (lastOffset < 0 || lastOffset > children) {
            throw contractViolation(row, "final offset " + lastOffset + " exceeds value count " + children);
        }
        DecodedBatch childValues = decodeChildValues(valuesAddr, validityAddr, kind, bitOffset, valueBitOffset, children, row);
        decodedListBatch = new DecodedListBatch(firstRow, lastRow, offsets, childValues);
    }

    /**
     * Builds a {@link DecodedBatch} over the flattened child values, addressed from child index 0 so
     * the list offsets index it directly. An empty batch yields an empty view no offset range reads from.
     */
    private DecodedBatch decodeChildValues(
        long valuesAddr,
        long validityAddr,
        int kind,
        int bitOffset,
        int valueBitOffset,
        int children,
        long row
    ) throws IOException {
        if (children == 0) {
            // No offsets view: the child buffers are exported as a fixed-width kind; a variable-width
            // (binary) child would need the child's own i32 offsets exported alongside the list offsets.
            return new DecodedBatch(0, -1, MemorySegment.NULL, kind, valueBitOffset, null, null, 0);
        }
        MemorySegment values = MemorySegment.ofAddress(valuesAddr).reinterpret(valuesByteLength(kind, children, valueBitOffset, row));
        MemorySegment presenceBits;
        int presenceBitOffset;
        if (validityAddr == 0) {
            presenceBits = null;
            presenceBitOffset = 0;
        } else {
            // Size to the bitmap's significant bytes, not up to a word: Arrow only guarantees the
            // buffer holds the bits, so a word-rounded view could extend past the allocation.
            long presenceBytes = ((long) bitOffset + children + 7) >>> 3;
            presenceBits = MemorySegment.ofAddress(validityAddr).reinterpret(presenceBytes);
            presenceBitOffset = bitOffset;
        }
        return new DecodedBatch(0, children - 1L, values, kind, valueBitOffset, null, presenceBits, presenceBitOffset);
    }

    /**
     * Byte length of the borrowed child values buffer, rejecting any unknown kind before
     * {@code reinterpret}. {@code count} is the number of child values, not rows.
     */
    private long valuesByteLength(int kind, int count, int valueBitOffset, long row) throws IOException {
        return switch (kind) {
            case DecodedBatch.KIND_LONG, DecodedBatch.KIND_DOUBLE -> (long) count * Long.BYTES;
            case DecodedBatch.KIND_INT, DecodedBatch.KIND_UINT_BITS, DecodedBatch.KIND_FLOAT -> (long) count * Integer.BYTES;
            case DecodedBatch.KIND_SHORT, DecodedBatch.KIND_USHORT, DecodedBatch.KIND_HALF_FLOAT -> (long) count * Short.BYTES;
            case DecodedBatch.KIND_BYTE, DecodedBatch.KIND_UBYTE -> (long) count * Byte.BYTES;
            // One bit per value, so size to the significant bytes exactly as the presence bitmap is
            // sized. The offset is a bit index and can exceed a byte, so it is included in the span.
            case DecodedBatch.KIND_BOOL -> ((long) valueBitOffset + count + 7) >>> 3;
            default -> throw contractViolation(row, "unknown value kind " + kind);
        };
    }

    private IOException contractViolation(long row, String detail) {
        return new IOException(
            "native list cursor returned an invalid batch at row " + row + " (" + detail + ") for " + file + "/" + column
        );
    }

    private void checkStatus(long rc, long row) throws IOException {
        if (rc == ParquetCodecBridge.RC_EOF) {
            throw new IOException("native list cursor exhausted before row " + row + " (" + file + "/" + column + ")");
        }
        if (rc != ParquetCodecBridge.RC_OK) {
            throw new IOException("Unexpected native list cursor status " + rc + " at row " + row + " (" + file + "/" + column + ")");
        }
    }

    @Override
    protected void doClose() {
        // Drop the resident batch before freeing the cursor: its off-heap views point into buffers
        // the cursor owns and must not outlive them.
        decodedListBatch = null;
        try {
            ParquetCodecBridge.closeColumnCursor(ptr);
        } catch (IOException e) {
            // A negative status means Rust panicked tearing the cursor down; #[ffm_safe] caught it at
            // the boundary. doClose() cannot throw a checked exception, so keep the message here.
            LOGGER.error("failed to close native list cursor for {}/{}", file, column, e);
        }
    }
}
