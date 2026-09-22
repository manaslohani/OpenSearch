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
 * Column reader backed by a forward-only native Arrow column cursor.
 *
 * <p>The native cursor only advances forward. This wrapper still serves a request that falls behind
 * the current batch by reopening the cursor (cheap, since file metadata is cached) and scanning
 * forward again, so an ascending walk that occasionally rewinds keeps working.
 *
 * <p>Each batch call returns borrowed Arrow buffer addresses; the resident {@link DecodedBatch}
 * points off-heap views at them and reads values in place, with no copy. Those views are valid only
 * until the next batch call on this reader, which always replaces the batch first.
 *
 * <p>Binary (BYTE_ARRAY) columns are served two ways. {@link #openBinary} readers copy each batch into
 * reader-owned staging buffers exposed as a {@link DecodedBinaryBatch}, valid until the reader's next
 * load; that path also accepts view-encoded string columns, which have no single contiguous buffer to
 * point at. A plain {@code Binary} column opened with {@link #open} is borrowed zero-copy instead: the
 * i32 offsets buffer beside the data buffer is exported as {@link DecodedBatch#KIND_BINARY}.
 *
 * <p>Scope is single-valued columns: fixed-width numerics, bit-packed booleans, {@code half_float}, and
 * variable-width binary (keyword/ip through {@link #openBinary}, binary through either). Types the
 * native cursor cannot serve are rejected when the cursor is opened, so an unsupported field is
 * unreadable rather than read incorrectly.
 *
 * <p>Out of scope for this reader: repeated ({@code LIST}) columns and {@code scaled_float}.
 *
 * <p>{@code scaled_float} needs care because it cannot be rejected here: it is written as a plain
 * long column ({@code CoreDataFieldPlugin} maps it to {@code LongParquetField}), so on the wire it
 * is indistinguishable from a long, while OpenSearch doc values hold
 * {@code round(value * scaling_factor)}. This reader is given a file and a column name, not a
 * {@code MappedFieldType}, so the field must be excluded where the mapping type is known before it
 * reaches this reader.
 *
 * <p>The Parquet page index is exposed through {@link #pageIndex()} for a DocValues skipper to skip
 * whole pages without decoding them.
 *
 * <p>This extends {@link NativeHandle}, so the native cursor pointer is tracked in the shared
 * live-handle registry, and {@link #close()} is idempotent through the base class: closing an
 * already-closed reader is a no-op and the cursor is freed exactly once.
 */
public final class ParquetColumnReader extends NativeHandle implements NumericValueReader, BinaryValueReader {

    private static final Logger LOGGER = LogManager.getLogger(ParquetColumnReader.class);

    /**
     * Store pointer meaning "read from the local filesystem", which is every hot shard: its Parquet
     * files are on the node's disk. A warm shard's files live in the remote object store and it
     * passes its own store pointer instead.
     */
    public static final long LOCAL_STORE = 0L;

    /** Number of scalar out-parameters {@code nextBatch} writes back. */
    private static final int OUT_PARAM_COUNT = 8;

    /** Starting binary value-buffer capacity; an overflow retry ratchets it to the reported need. */
    private static final long INITIAL_BINARY_VALUE_BYTES = 64 * 1024L;

    private final Path file;
    private final String column;

    /**
     * Ceiling on the rows the native cursor may return, captured once at open and handed to the
     * native side in the same call. Deliberately a value rather than a live setting lookup: the
     * cursor is configured once, so re-reading a dynamic setting here could reject a batch the
     * native cursor was legitimately told to produce.
     */
    private final int maxBatchSize;

    /**
     * True when this reader serves a variable-width (keyword/ip) column through the binary batch
     * export; false for the numeric zero-copy path. Fixed at open.
     */
    private final boolean binary;

    /**
     * Staging buffers the native binary export copies into, sized once at open: a batch never
     * exceeds {@code maxBatchSize} rows, so the offsets and presence buffers cannot overflow.
     * Null for numeric readers.
     */
    private final Arena binaryArena;
    private final MemorySegment offsetsBuf;
    private final MemorySegment presenceBuf;
    private final long presenceWords;

    /**
     * Replaced wholesale when a batch's value bytes outgrow it (grow-only ratchet); kept in its
     * own arena so the old buffer can be freed without touching the fixed ones.
     */
    private Arena valueArena;
    private MemorySegment valueBuf;

    private DecodedBatch decodedBatch;
    private DecodedBinaryBatch decodedBinaryBatch;

    /** Loaded once on first request and cached for this reader's lifetime. */
    private ColumnPageIndex pageIndex;

    private ParquetColumnReader(long handle, Path file, String column, int maxBatchSize, boolean binary) {
        super(handle);
        this.file = file;
        this.column = column;
        this.maxBatchSize = maxBatchSize;
        this.binary = binary;
        if (binary) {
            this.presenceWords = (maxBatchSize + 63) / 64;
            // Shared, not confined: the reader is opened by the segment-opening thread and read by
            // search threads.
            this.binaryArena = Arena.ofShared();
            this.offsetsBuf = binaryArena.allocate(ValueLayout.JAVA_INT, maxBatchSize + 1L);
            this.presenceBuf = binaryArena.allocate(ValueLayout.JAVA_LONG, presenceWords);
            this.valueArena = Arena.ofShared();
            this.valueBuf = valueArena.allocate(INITIAL_BINARY_VALUE_BYTES);
        } else {
            this.presenceWords = 0;
            this.binaryArena = null;
            this.offsetsBuf = null;
            this.presenceBuf = null;
            this.valueArena = null;
            this.valueBuf = null;
        }
    }

    /** Opens a cursor over a local file using the default batch-size settings. */
    public static ParquetColumnReader open(Path file, String column) throws IOException {
        return open(file, column, Settings.EMPTY);
    }

    /** Opens a cursor over a local file, sized from {@code settings}. */
    public static ParquetColumnReader open(Path file, String column, Settings settings) throws IOException {
        return open(file, column, settings, LOCAL_STORE);
    }

    /**
     * Opens a cursor sized from {@code index.parquet.docvalues.initial_batch_size} and
     * {@code index.parquet.docvalues.max_batch_size}.
     *
     * @param settings index settings, or {@link Settings#EMPTY} to take the defaults
     * @param storePtr native object store to read through, or {@link #LOCAL_STORE} for a local file
     */
    public static ParquetColumnReader open(Path file, String column, Settings settings, long storePtr) throws IOException {
        return open(
            file,
            column,
            DatafusionSettings.docValuesInitialBatchSize(settings),
            DatafusionSettings.docValuesMaxBatchSize(settings),
            storePtr
        );
    }

    /**
     * Opens a cursor over a local file with explicit window sizes, bypassing settings
     * resolution.
     */
    public static ParquetColumnReader open(Path file, String column, int initialBatchSize, int maxBatchSize) throws IOException {
        return open(file, column, initialBatchSize, maxBatchSize, LOCAL_STORE);
    }

    /**
     * Opens a cursor with explicit window sizes, bypassing settings resolution.
     *
     * @param initialBatchSize rows in the first decode window; must be in {@code 1..=maxBatchSize}
     * @param maxBatchSize     ceiling the adaptive window grows to
     * @param storePtr         native object store to read through, or {@link #LOCAL_STORE} for a local file
     */
    public static ParquetColumnReader open(Path file, String column, int initialBatchSize, int maxBatchSize, long storePtr)
        throws IOException {
        long handle = ParquetCodecBridge.openColumnCursor(file.toString(), column, initialBatchSize, maxBatchSize, storePtr);
        return new ParquetColumnReader(handle, file, column, maxBatchSize, false);
    }

    /** Opens a binary (keyword/ip) cursor over a local file using the default batch-size settings. */
    public static ParquetColumnReader openBinary(Path file, String column) throws IOException {
        return openBinary(file, column, Settings.EMPTY);
    }

    /** Opens a binary cursor over a local file, sized from {@code settings}. */
    public static ParquetColumnReader openBinary(Path file, String column, Settings settings) throws IOException {
        return openBinary(file, column, settings, LOCAL_STORE);
    }

    /** Opens a binary cursor sized from the {@code index.parquet.docvalues.*} batch settings. */
    public static ParquetColumnReader openBinary(Path file, String column, Settings settings, long storePtr) throws IOException {
        return openBinary(
            file,
            column,
            DatafusionSettings.docValuesInitialBatchSize(settings),
            DatafusionSettings.docValuesMaxBatchSize(settings),
            storePtr
        );
    }

    /** Opens a binary cursor with explicit window sizes, bypassing settings resolution. */
    public static ParquetColumnReader openBinary(Path file, String column, int initialBatchSize, int maxBatchSize, long storePtr)
        throws IOException {
        long handle = ParquetCodecBridge.openColumnCursor(file.toString(), column, initialBatchSize, maxBatchSize, storePtr);
        return new ParquetColumnReader(handle, file, column, maxBatchSize, true);
    }

    @Override
    public DecodedBatch decodedBatch() {
        if (binary) {
            throw new IllegalStateException("column " + column + " is binary; use decodedBinaryBatch()");
        }
        return decodedBatch;
    }

    @Override
    public DecodedBinaryBatch decodedBinaryBatch() {
        if (binary == false) {
            throw new IllegalStateException("column " + column + " is numeric; use decodedBatch()");
        }
        return decodedBinaryBatch;
    }

    /** Per-page statistics for a DocValues skipper, loaded once and cached for this reader's lifetime. */
    public ColumnPageIndex pageIndex() throws IOException {
        ensureOpen();
        if (pageIndex == null) {
            pageIndex = loadPageIndex();
        }
        return pageIndex;
    }

    /**
     * Whether this column is physically repeated (a Parquet LIST), read from the file schema recorded
     * at open. Never advances the cursor.
     */
    public boolean isPhysicallyRepeated() throws IOException {
        ensureOpen();
        return ParquetCodecBridge.isRepeated(ptr);
    }

    /**
     * Ensures the resident batch contains {@code row}. A row already resident is served without
     * touching the cursor, a row ahead of it advances the cursor, and a row behind it reopens the
     * cursor first.
     */
    @Override
    public void loadBatchContaining(long row) throws IOException {
        ensureOpen();
        if (binary) {
            DecodedBinaryBatch currentBinary = decodedBinaryBatch;
            if (currentBinary != null) {
                if (currentBinary.contains(row)) {
                    return;
                }
                if (row < currentBinary.firstRow()) {
                    reopen();
                }
            }
            loadBinaryBatch(row);
            return;
        }
        DecodedBatch current = decodedBatch;
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
        loadBatch(row);
    }

    /** Replaces the forward-only cursor with a fresh one at row zero. Only reached on a backward request. */
    private void reopen() throws IOException {
        decodedBatch = null;
        decodedBinaryBatch = null;
        ParquetCodecBridge.resetColumnCursor(ptr);
    }

    private void loadBatch(long row) throws IOException {
        long firstRow;
        long lastRow;
        long valuesAddr;
        long validityAddr;
        int kind;
        int bitOffset;
        int valueBitOffset;
        long offsetsAddr;

        // Drop the resident batch before crossing over. A successful native call frees the buffers
        // the old batch borrowed, so any exit between here and the assignment below - a bad status
        // or a failed framing check - must not leave a DecodedBatch whose views address freed
        // memory.
        decodedBatch = null;

        // The eight scalar out-parameters are tiny and read out immediately, so a per-call arena is
        // enough; the borrowed value/validity/offsets buffers live in native (Rust-owned) memory and
        // are reinterpreted separately below, outside this arena.
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

            long rc = ParquetCodecBridge.nextBatch(
                ptr,
                row,
                firstRowOut,
                lastRowOut,
                valuesAddrOut,
                validityAddrOut,
                validityBitOffsetOut,
                valueKindOut,
                valueBitOffsetOut,
                offsetsAddrOut
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
        }

        // Validate the native cursor's framing before pointing memory views at the borrowed
        // buffers. reinterpret() is unbounded (it trusts the native address and length), so these
        // checks fail fast on a malformed contract instead of reading out of bounds. They do not,
        // and cannot, make a correct-looking but wrong address safe - that is inherent to a
        // zero-copy FFM borrow.
        if (firstRow < 0 || lastRow < firstRow || row < firstRow || row > lastRow) {
            throw contractViolation(row, "row range [" + firstRow + ", " + lastRow + "]");
        }
        long batchRowsLong = lastRow - firstRow + 1;
        if (batchRowsLong > maxBatchSize) {
            throw contractViolation(row, batchRowsLong + " rows exceeds cap " + maxBatchSize);
        }
        if (valuesAddr == 0 || bitOffset < 0 || valueBitOffset < 0) {
            throw contractViolation(
                row,
                "values address " + valuesAddr + ", bit offset " + bitOffset + ", value bit offset " + valueBitOffset
            );
        }
        int batchRows = (int) batchRowsLong;

        MemorySegment presenceBits;
        int presenceBitOffset;
        if (validityAddr == 0) {
            presenceBits = null;
            presenceBitOffset = 0;
        } else {
            // Size to the bitmap's significant bytes, not up to a word: Arrow only guarantees the
            // buffer holds the bits, so a word-rounded view could extend past the allocation.
            long presenceBytes = ((long) bitOffset + batchRows + 7) >>> 3;
            presenceBits = MemorySegment.ofAddress(validityAddr).reinterpret(presenceBytes);
            presenceBitOffset = bitOffset;
        }

        // Borrowed Arrow buffers, read in place: O(rows accessed), no copy. Valid until the next
        // batch call on this cursor, which clears the resident batch before borrowing again.
        MemorySegment values;
        MemorySegment offsets;
        if (kind == DecodedBatch.KIND_BINARY) {
            // Variable-width: an i32 offsets buffer of batchRows+1 entries plus a data buffer whose
            // length is offsets[batchRows] (the end of the last row). The offsets buffer is sized and
            // read first; the data buffer is then sized to exactly the bytes the offsets span.
            if (offsetsAddr == 0) {
                throw contractViolation(row, "binary batch with no offsets buffer");
            }
            long offsetsBytes = ((long) batchRows + 1) * Integer.BYTES;
            offsets = MemorySegment.ofAddress(offsetsAddr).reinterpret(offsetsBytes);
            int firstOffset = offsets.getAtIndex(ValueLayout.JAVA_INT, 0);
            int lastOffset = offsets.getAtIndex(ValueLayout.JAVA_INT, batchRows);
            if (firstOffset < 0 || lastOffset < firstOffset) {
                throw contractViolation(row, "binary offsets span [" + firstOffset + ", " + lastOffset + "]");
            }
            // Rows index absolute into the data buffer, so the view must reach offsets[batchRows];
            // a sliced parent can start at a non-zero firstOffset, so size to the end, not the span.
            values = MemorySegment.ofAddress(valuesAddr).reinterpret(lastOffset);
        } else {
            values = MemorySegment.ofAddress(valuesAddr).reinterpret(valuesByteLength(kind, batchRows, valueBitOffset, row));
            offsets = null;
        }
        decodedBatch = new DecodedBatch(firstRow, lastRow, values, kind, valueBitOffset, offsets, presenceBits, presenceBitOffset);
    }

    /**
     * Loads a copied-out binary batch containing {@code row}: values packed back to back, fence-post
     * offsets, and presence bits, staged in this reader's buffers and served as bounded off-heap
     * views; consumers copy one row at a time into reusable scratch via
     * {@link DecodedBinaryBatch#copyValue}.
     */
    private void loadBinaryBatch(long row) throws IOException {
        // Dropped before the call: a failed load must not leave a batch describing stale buffers.
        decodedBinaryBatch = null;

        try (Arena callArena = Arena.ofConfined()) {
            MemorySegment out = callArena.allocate(ValueLayout.JAVA_LONG, 3);
            MemorySegment firstRowOut = out.asSlice(0L, Long.BYTES);
            MemorySegment lastRowOut = out.asSlice(Long.BYTES, Long.BYTES);
            MemorySegment actualLenOut = out.asSlice(2L * Long.BYTES, Long.BYTES);

            // Two attempts: only RC_OVERFLOW triggers the second, and the retry's buffer is sized to
            // the exact reported need over the batch the native side staged, so it cannot overflow
            // again unless the native contract is broken.
            for (int attempt = 0; attempt < 2; attempt++) {
                long rc = ParquetCodecBridge.nextBinaryBatch(
                    ptr,
                    row,
                    firstRowOut,
                    lastRowOut,
                    valueBuf,
                    valueBuf.byteSize(),
                    actualLenOut,
                    offsetsBuf,
                    maxBatchSize + 1L,
                    presenceBuf,
                    presenceWords
                );
                if (rc == ParquetCodecBridge.RC_OVERFLOW) {
                    growBinaryValueBuffer(actualLenOut.get(ValueLayout.JAVA_LONG, 0), row);
                    continue;
                }
                checkStatus(rc, row);

                long firstRow = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
                long lastRow = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
                long actualLen = actualLenOut.get(ValueLayout.JAVA_LONG, 0);
                if (firstRow < 0 || lastRow < firstRow || row < firstRow || row > lastRow) {
                    throw contractViolation(row, "row range [" + firstRow + ", " + lastRow + "]");
                }
                long batchRowsLong = lastRow - firstRow + 1;
                if (batchRowsLong > maxBatchSize) {
                    throw contractViolation(row, batchRowsLong + " rows exceeds cap " + maxBatchSize);
                }
                if (actualLen < 0 || actualLen > valueBuf.byteSize()) {
                    throw contractViolation(row, "value length " + actualLen + " outside buffer of " + valueBuf.byteSize());
                }
                int batchRows = (int) batchRowsLong;
                int firstPost = offsetsBuf.getAtIndex(ValueLayout.JAVA_INT, 0);
                int lastPost = offsetsBuf.getAtIndex(ValueLayout.JAVA_INT, batchRows);
                if (firstPost != 0 || lastPost != actualLen) {
                    throw contractViolation(row, "offset posts [" + firstPost + ", " + lastPost + "] vs length " + actualLen);
                }

                // Bounded views of exactly the filled bytes, not the buffers' full capacity, so the
                // batch's bounds-checked reads cannot wander into leftover garbage. Valid until the
                // next load on this reader, which replaces the batch (and may replace the buffers)
                // before the native side writes again.
                decodedBinaryBatch = new DecodedBinaryBatch(
                    firstRow,
                    lastRow,
                    valueBuf.asSlice(0, actualLen),
                    offsetsBuf.asSlice(0, (batchRows + 1L) * Integer.BYTES),
                    presenceBuf.asSlice(0, (long) ((batchRows + 63) >>> 6) * Long.BYTES)
                );
                return;
            }
            throw contractViolation(row, "cursor still overflowing after growing to " + valueBuf.byteSize() + " bytes");
        }
    }

    /** Grow-only ratchet: frees the old value buffer and allocates one at the reported need. */
    private void growBinaryValueBuffer(long neededBytes, long row) throws IOException {
        if (neededBytes <= valueBuf.byteSize()) {
            throw contractViolation(row, "overflow reported at " + neededBytes + " bytes into a buffer of " + valueBuf.byteSize());
        }
        Arena old = valueArena;
        valueArena = Arena.ofShared();
        valueBuf = valueArena.allocate(neededBytes);
        old.close();
    }

    /**
     * Sizes the parallel arrays from the current page count and retries once at the count the native
     * side reports if the page table grew between the two calls, so a raced size change widens the
     * arrays rather than truncating the table.
     */
    private ColumnPageIndex loadPageIndex() throws IOException {
        int capacity = Math.max((int) ParquetCodecBridge.pageCount(ptr), 1);
        for (int attempt = 0; attempt < 2; attempt++) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment firstRow = arena.allocate(ValueLayout.JAVA_LONG, capacity);
                MemorySegment rowCount = arena.allocate(ValueLayout.JAVA_LONG, capacity);
                MemorySegment nullCount = arena.allocate(ValueLayout.JAVA_LONG, capacity);
                MemorySegment minLong = arena.allocate(ValueLayout.JAVA_LONG, capacity);
                MemorySegment maxLong = arena.allocate(ValueLayout.JAVA_LONG, capacity);
                MemorySegment actualPages = arena.allocate(ValueLayout.JAVA_LONG, 1);

                long rc = ParquetCodecBridge.pageIndex(ptr, firstRow, rowCount, nullCount, minLong, maxLong, capacity, actualPages);
                if (rc == ParquetCodecBridge.RC_OVERFLOW) {
                    int actual = (int) actualPages.get(ValueLayout.JAVA_LONG, 0);
                    if (actual <= capacity) {
                        throw contractViolation(0, "page-index overflow reported " + actual + " pages into capacity " + capacity);
                    }
                    capacity = actual;
                    continue;
                }

                int pages = (int) actualPages.get(ValueLayout.JAVA_LONG, 0);
                long[] rowCountOf = toLongArray(rowCount, pages);
                long totalRows = 0;
                for (long rows : rowCountOf) {
                    totalRows += rows;
                }
                return new ColumnPageIndex(
                    toLongArray(firstRow, pages),
                    rowCountOf,
                    toLongArray(nullCount, pages),
                    toLongArray(minLong, pages),
                    toLongArray(maxLong, pages),
                    totalRows
                );
            }
        }
        throw contractViolation(0, "page index still overflowing after growing to " + capacity + " pages");
    }

    private static long[] toLongArray(MemorySegment segment, int length) {
        return length == 0 ? new long[0] : segment.asSlice(0, (long) length * Long.BYTES).toArray(ValueLayout.JAVA_LONG);
    }

    /**
     * Byte length of the borrowed values buffer for a batch, rejecting any kind this reader does not
     * understand. Called before {@code reinterpret} so an unknown kind never sizes a memory view.
     */
    private long valuesByteLength(int kind, int batchRows, int valueBitOffset, long row) throws IOException {
        return switch (kind) {
            case DecodedBatch.KIND_LONG, DecodedBatch.KIND_DOUBLE -> (long) batchRows * Long.BYTES;
            case DecodedBatch.KIND_INT, DecodedBatch.KIND_UINT_BITS, DecodedBatch.KIND_FLOAT -> (long) batchRows * Integer.BYTES;
            case DecodedBatch.KIND_SHORT, DecodedBatch.KIND_USHORT, DecodedBatch.KIND_HALF_FLOAT -> (long) batchRows * Short.BYTES;
            case DecodedBatch.KIND_BYTE, DecodedBatch.KIND_UBYTE -> (long) batchRows * Byte.BYTES;
            // One bit per row, so size to the significant bytes exactly as the presence bitmap is
            // sized. The offset is a bit index and can exceed a byte, so it is included in the span.
            case DecodedBatch.KIND_BOOL -> ((long) valueBitOffset + batchRows + 7) >>> 3;
            default -> throw contractViolation(row, "unknown value kind " + kind);
        };
    }

    private IOException contractViolation(long row, String detail) {
        String shape = binary ? "binary" : "numeric";
        return new IOException(
            "native " + shape + " cursor returned an invalid batch at row " + row + " (" + detail + ") for " + file + "/" + column
        );
    }

    private void checkStatus(long rc, long row) throws IOException {
        String shape = binary ? "binary" : "numeric";
        if (rc == ParquetCodecBridge.RC_EOF) {
            throw new IOException("native " + shape + " cursor exhausted before row " + row + " (" + file + "/" + column + ")");
        }
        if (rc != ParquetCodecBridge.RC_OK) {
            throw new IOException(
                "Unexpected native " + shape + " cursor status " + rc + " at row " + row + " (" + file + "/" + column + ")"
            );
        }
    }

    @Override
    protected void doClose() {
        // Drop the resident batches before freeing the cursor: a DecodedBatch holds off-heap views
        // into buffers the native cursor owns, so it must not stay reachable once those buffers are
        // freed.
        decodedBatch = null;
        decodedBinaryBatch = null;
        try {
            ParquetCodecBridge.closeColumnCursor(ptr);
        } catch (IOException e) {
            // A negative status means Rust panicked tearing the cursor down; #[ffm_safe] caught it at
            // the boundary. doClose() cannot throw a checked exception, so keep the message here.
            LOGGER.error("failed to close native column cursor for {}/{}", file, column, e);
        } finally {
            if (valueArena != null) {
                valueArena.close();
            }
            if (binaryArena != null) {
                binaryArena.close();
            }
        }
    }
}
