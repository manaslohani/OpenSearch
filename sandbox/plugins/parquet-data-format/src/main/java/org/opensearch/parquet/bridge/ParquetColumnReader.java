/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.cache.CacheStats;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;
import org.opensearch.parquet.codec.cache.PageCache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * FFM wrapper over a single native Parquet column-reader handle, and the owner of that
 * column's performance state (the Layer 3/4 {@link ColumnPageIndex} built at open, and the
 * current Layer 1/2 {@link PageCache}).
 *
 * <p>This is the only class besides {@link RustBridge} that deals with the native
 * column-reader surface; iterator implementations (task 5+) see only the plain-Java
 * {@link PageCache} / {@link ColumnPageIndex} data structures.
 *
 * <p>Threading: a reader is single-threaded (one segment per query thread). It is not safe
 * for concurrent use. {@link #close()} is idempotent.
 *
 * <p>Buffer ownership: scratch out-buffers handed to the native functions are drawn from a
 * shared {@link BufferPool} (Layer 5). The slow-path reads and the page decode both follow
 * the grow-and-retry overflow protocol: on {@link RustBridge#RC_OVERFLOW} the required sizes
 * are read from the out-parameters, larger buffers are obtained from the pool, and the call
 * is retried exactly once.
 */
public final class ParquetColumnReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(ParquetColumnReader.class);

    /** Sentinel handle for a reader whose native handle has been released. */
    private static final long CLOSED_HANDLE = -1L;

    private final ParquetPhysicalType type;
    private final boolean repeated;
    private final BufferPool bufferPool;
    private final Path file;
    private final String column;
    private final CacheStats stats = new CacheStats();

    private long handle;
    private ColumnPageIndex pageIndex;
    private PageCache cache;

    private ParquetColumnReader(long handle, Path file, String column, ParquetPhysicalType type, boolean repeated, BufferPool bufferPool) {
        this.handle = handle;
        this.file = file;
        this.column = column;
        this.type = type;
        this.repeated = repeated;
        this.bufferPool = bufferPool;
    }

    /**
     * Opens a column reader for {@code column} in the Parquet {@code file}, validating that
     * the column exists and its physical type matches {@code expected}. Eagerly loads the
     * column's {@link ColumnPageIndex} (Layer 3/4).
     *
     * @param repeated whether the column is multi-valued (max repetition level &gt; 0)
     * @throws IOException if the file/column cannot be opened or the type mismatches
     */
    public static ParquetColumnReader open(Path file, String column, ParquetPhysicalType expected, boolean repeated, BufferPool pool)
        throws IOException {
        long h = RustBridge.openColumnReader(file.toString(), column, expected.code());
        ParquetColumnReader reader = new ParquetColumnReader(h, file, column, expected, repeated, pool);
        try {
            reader.pageIndex = reader.loadPageIndex();
        } catch (IOException | RuntimeException e) {
            // Never leak the native handle if page-index load fails after open.
            reader.close();
            throw e;
        }
        return reader;
    }

    /** The column's Parquet physical type. */
    public ParquetPhysicalType type() {
        return type;
    }

    /** Whether the column is multi-valued. */
    public boolean isRepeated() {
        return repeated;
    }

    /** The Layer 3/4 page index, loaded at {@link #open}. */
    public ColumnPageIndex pageIndex() {
        return pageIndex;
    }

    /** Per-column cache hit/miss counters across all caching layers (for diagnostics). */
    public CacheStats stats() {
        return stats;
    }

    /**
     * Returns the currently cached page, or {@code null} when no page is loaded or the last
     * {@link #loadPageContaining(long)} landed on an all-nulls page (Layer 4 skip).
     */
    public PageCache cache() {
        return cache;
    }

    // ── Slow-path single/repeated reads (used by ordinal-table construction, task 6) ──

    /** Result of a single-valued read: whether the row had a value, and its raw {@code long} bits. */
    public record Value(boolean present, long bits) {
        public static final Value ABSENT = new Value(false, 0L);
    }

    /**
     * Slow-path single-value read at global {@code row}. For primitive columns the returned
     * {@code bits} are the raw value bits (INT32 sign-extended, FLOAT/DOUBLE via
     * {@code *toRawBits}, BOOL as 0/1). For {@code BYTE_ARRAY} columns use
     * {@link #readBytesAtRow(long)} instead.
     */
    public Value readValueAtRow(long row) throws IOException {
        ensureOpen();
        stats.slowValueRead();
        MemorySegment present = bufferPool.longOut("present");
        MemorySegment longOut = bufferPool.longOut("long");
        MemorySegment lenOut = bufferPool.longOut("len");
        long rc = RustBridge.readValueAtRow(handle, row, present, longOut, MemorySegment.NULL, 0L, lenOut);
        if (rc == RustBridge.RC_OVERFLOW) {
            // Primitive reads never overflow (no byte payload); a BYTE_ARRAY column was
            // queried through the primitive path.
            throw new IOException("readValueAtRow: unexpected overflow for primitive read at row " + row);
        }
        boolean isPresent = present.get(ValueLayout.JAVA_LONG, 0) != 0L;
        return isPresent ? new Value(true, longOut.get(ValueLayout.JAVA_LONG, 0)) : Value.ABSENT;
    }

    /**
     * Slow-path single-value read of a {@code BYTE_ARRAY} column at global {@code row}.
     * Returns the value bytes, or {@code null} when the row is null. Follows the
     * grow-and-retry overflow protocol.
     */
    public byte[] readBytesAtRow(long row) throws IOException {
        ensureOpen();
        stats.slowValueRead();
        MemorySegment present = bufferPool.longOut("present");
        MemorySegment longOut = bufferPool.longOut("long");
        MemorySegment lenOut = bufferPool.longOut("len");

        long cap = 64;
        MemorySegment buf = bufferPool.bytes("value", cap);
        long rc = RustBridge.readValueAtRow(handle, row, present, longOut, buf, cap, lenOut);
        if (rc == RustBridge.RC_OVERFLOW) {
            long required = lenOut.get(ValueLayout.JAVA_LONG, 0);
            buf = bufferPool.bytes("value", required);
            cap = required;
            rc = RustBridge.readValueAtRow(handle, row, present, longOut, buf, cap, lenOut);
            if (rc == RustBridge.RC_OVERFLOW) {
                throw new IOException("readBytesAtRow: overflow persisted after retry at row " + row);
            }
        }
        if (present.get(ValueLayout.JAVA_LONG, 0) == 0L) {
            return null;
        }
        long len = lenOut.get(ValueLayout.JAVA_LONG, 0);
        if (len < 0) {
            return null;
        }
        return buf.asSlice(0, len).toArray(ValueLayout.JAVA_BYTE);
    }

    /** Result of a repeated primitive read: the per-value raw {@code long} bits, in row order. */
    public record RepeatedValues(long[] bits) {
        public static final RepeatedValues EMPTY = new RepeatedValues(new long[0]);

        public int count() {
            return bits.length;
        }
    }

    /**
     * Slow-path repeated read of a primitive column at global {@code row}. Returns the raw
     * {@code long} bits of each repeated value, in row order. Follows the grow-and-retry
     * overflow protocol.
     */
    public RepeatedValues readRepeatedAtRow(long row) throws IOException {
        ensureOpen();
        stats.slowRepeatedRead();
        MemorySegment countOut = bufferPool.longOut("count");

        long cap = 8;
        MemorySegment longs = bufferPool.longs("repeated", cap);
        long rc = RustBridge.readRepeatedAtRow(handle, row, countOut, longs, cap, MemorySegment.NULL, MemorySegment.NULL, 0L);
        if (rc == RustBridge.RC_OVERFLOW) {
            long required = countOut.get(ValueLayout.JAVA_LONG, 0);
            longs = bufferPool.longs("repeated", required);
            cap = required;
            rc = RustBridge.readRepeatedAtRow(handle, row, countOut, longs, cap, MemorySegment.NULL, MemorySegment.NULL, 0L);
            if (rc == RustBridge.RC_OVERFLOW) {
                throw new IOException("readRepeatedAtRow: overflow persisted after retry at row " + row);
            }
        }
        int count = (int) countOut.get(ValueLayout.JAVA_LONG, 0);
        if (count == 0) {
            return RepeatedValues.EMPTY;
        }
        long[] out = longs.asSlice(0, (long) count * ValueLayout.JAVA_LONG.byteSize()).toArray(ValueLayout.JAVA_LONG);
        return new RepeatedValues(out);
    }

    /**
     * Slow-path repeated read of a {@code BYTE_ARRAY} column at global {@code row}. Returns one
     * {@code byte[]} per repeated value in row order, or {@code null} when the row's list is
     * empty. Follows the grow-and-retry overflow protocol for both the element-count buffer
     * and the concatenated-bytes buffer.
     */
    public byte[][] readRepeatedBytesAtRow(long row) throws IOException {
        ensureOpen();
        stats.slowRepeatedRead();
        MemorySegment countOut = bufferPool.longOut("count");

        long countCap = 8;
        long byteCap = 256;
        MemorySegment offsets = bufferPool.longs("repeatedOffsets", countCap + 1);
        MemorySegment byteBuf = bufferPool.bytes("repeatedBytes", byteCap);

        long rc = RustBridge.readRepeatedAtRow(handle, row, countOut, MemorySegment.NULL, countCap, byteBuf, offsets, byteCap);
        if (rc == RustBridge.RC_OVERFLOW) {
            long requiredCount = countOut.get(ValueLayout.JAVA_LONG, 0);
            countCap = Math.max(requiredCount, countCap);
            offsets = bufferPool.longs("repeatedOffsets", countCap + 1);
            // First retry establishes the offsets so we can learn the required byte size.
            rc = RustBridge.readRepeatedAtRow(handle, row, countOut, MemorySegment.NULL, countCap, byteBuf, offsets, byteCap);
            if (rc == RustBridge.RC_OVERFLOW) {
                // Byte buffer too small: offsets[count] reports the required total byte size.
                int cnt = (int) countOut.get(ValueLayout.JAVA_LONG, 0);
                long requiredBytes = offsets.getAtIndex(ValueLayout.JAVA_LONG, cnt);
                byteCap = Math.max(requiredBytes, byteCap);
                byteBuf = bufferPool.bytes("repeatedBytes", byteCap);
                rc = RustBridge.readRepeatedAtRow(handle, row, countOut, MemorySegment.NULL, countCap, byteBuf, offsets, byteCap);
                if (rc == RustBridge.RC_OVERFLOW) {
                    throw new IOException("readRepeatedBytesAtRow: overflow persisted after retry at row " + row);
                }
            }
        }

        int count = (int) countOut.get(ValueLayout.JAVA_LONG, 0);
        if (count == 0) {
            return null;
        }
        byte[][] out = new byte[count][];
        for (int i = 0; i < count; i++) {
            int start = (int) offsets.getAtIndex(ValueLayout.JAVA_LONG, i);
            int end = (int) offsets.getAtIndex(ValueLayout.JAVA_LONG, i + 1);
            out[i] = byteBuf.asSlice(start, (long) (end - start)).toArray(ValueLayout.JAVA_BYTE);
        }
        return out;
    }

    // ── Page index + page decode (Layer 1-4 hot path) ──

    /**
     * Loads the page that contains global {@code row} into the cache (Layer 1/2), applying
     * the Layer 4 all-nulls skip first. On an all-nulls page the cache is set to {@code null}
     * and no decode happens. Single-valued columns only; repeated columns use the slow path.
     */
    public void loadPageContaining(long row) throws IOException {
        ensureOpen();
        // Layer 3 — OffsetIndex jump-table lookup (consulted on every page miss).
        stats.pageIndexLookup();
        int pageIdx = pageIndex.pageForRow(row);
        if (pageIdx < 0) {
            throw new IOException("loadPageContaining: row " + row + " out of range (rows " + pageIndex.totalRows() + ")");
        }
        if (pageIndex.isAllNulls(pageIdx)) {
            // Layer 4 — whole page is null, resolved with no decode.
            stats.allNullPageSkip();
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "[PARQUET_DV_TRACE] loadPageContaining: col='{}' row={} page={} is ALL-NULLS (Layer 4 skip, no decode) file={}",
                    column,
                    row,
                    pageIdx,
                    file
                );
            }
            cache = null; // Layer 4 — whole page is null, no decode.
            return;
        }
        // FFM — page decode crossing.
        stats.pageDecode();
        if (logger.isTraceEnabled()) {
            logger.trace(
                "[PARQUET_DV_TRACE] loadPageContaining: col='{}' row={} -> decoding page={} ({} rows) [FFM crossing] file={}",
                column,
                row,
                pageIdx,
                pageIndex.numRowsOf(pageIdx),
                file
            );
        }
        cache = decodePage(row);
    }

    private PageCache decodePage(long row) throws IOException {
        MemorySegment firstRowOut = bufferPool.longOut("firstRow");
        MemorySegment lastRowOut = bufferPool.longOut("lastRow");
        MemorySegment valueLenOut = bufferPool.longOut("valueLen");

        // Size the page from the index so the first attempt usually fits.
        int pageIdx = pageIndex.pageForRow(row);
        int rows = (int) pageIndex.numRowsOf(pageIdx);
        int presenceWords = (rows + 63) >>> 6;

        long valueCap = (long) rows * ValueLayout.JAVA_LONG.byteSize();
        long offsetsCap = type.isPrimitive() ? 0 : (rows + 1);
        MemorySegment valueBuf = bufferPool.bytes("pageValue", Math.max(valueCap, 1));
        MemorySegment offsets = type.isPrimitive() ? MemorySegment.NULL : bufferPool.ints("pageOffsets", offsetsCap);
        MemorySegment presence = bufferPool.longs("pagePresence", presenceWords);

        long rc = RustBridge.decodePageAtRow(
            handle,
            row,
            firstRowOut,
            lastRowOut,
            valueBuf,
            valueCap,
            valueLenOut,
            offsets,
            offsetsCap,
            presence,
            presenceWords
        );

        if (rc == RustBridge.RC_OVERFLOW) {
            // Re-size from the reported page range + value length and retry once.
            long fr = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
            long lr = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
            int actualRows = (int) (lr - fr + 1);
            int actualPresenceWords = (actualRows + 63) >>> 6;
            long requiredValueBytes = valueLenOut.get(ValueLayout.JAVA_LONG, 0);

            valueCap = Math.max(requiredValueBytes, 1);
            offsetsCap = type.isPrimitive() ? 0 : (actualRows + 1);
            valueBuf = bufferPool.bytes("pageValue", valueCap);
            offsets = type.isPrimitive() ? MemorySegment.NULL : bufferPool.ints("pageOffsets", offsetsCap);
            presence = bufferPool.longs("pagePresence", actualPresenceWords);

            rc = RustBridge.decodePageAtRow(
                handle,
                row,
                firstRowOut,
                lastRowOut,
                valueBuf,
                valueCap,
                valueLenOut,
                offsets,
                offsetsCap,
                presence,
                actualPresenceWords
            );
            if (rc == RustBridge.RC_OVERFLOW) {
                throw new IOException("decodePageAtRow: overflow persisted after retry at row " + row);
            }
            presenceWords = actualPresenceWords;
        }

        long firstRow = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
        long lastRow = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
        long valueLen = valueLenOut.get(ValueLayout.JAVA_LONG, 0);
        int pageRows = (int) (lastRow - firstRow + 1);

        PageCache pc = new PageCache();
        pc.firstRow = firstRow;
        pc.lastRow = lastRow;
        pc.presenceBits = presence.asSlice(0, (long) ((pageRows + 63) >>> 6) * ValueLayout.JAVA_LONG.byteSize())
            .toArray(ValueLayout.JAVA_LONG);

        if (type.isPrimitive()) {
            pc.values = valueBuf.asSlice(0, (long) pageRows * ValueLayout.JAVA_LONG.byteSize()).toArray(ValueLayout.JAVA_LONG);
        } else {
            pc.byteBuf = valueLen > 0 ? valueBuf.asSlice(0, valueLen).toArray(ValueLayout.JAVA_BYTE) : new byte[0];
            pc.byteOffsets = offsets.asSlice(0, (long) (pageRows + 1) * ValueLayout.JAVA_INT.byteSize()).toArray(ValueLayout.JAVA_INT);
        }
        return pc;
    }

    private ColumnPageIndex loadPageIndex() throws IOException {
        long numPages = RustBridge.getColumnNumPages(handle);
        int n = Math.toIntExact(numPages);

        MemorySegment firstRow = bufferPool.longs("idxFirstRow", Math.max(n, 1));
        MemorySegment fileOffset = bufferPool.longs("idxFileOffset", Math.max(n, 1));
        MemorySegment compressed = bufferPool.ints("idxCompressed", Math.max(n, 1));
        MemorySegment nullCount = bufferPool.longs("idxNullCount", Math.max(n, 1));
        MemorySegment minLong = bufferPool.longs("idxMin", Math.max(n, 1));
        MemorySegment maxLong = bufferPool.longs("idxMax", Math.max(n, 1));
        MemorySegment actualPages = bufferPool.longOut("idxActualPages");

        long rc = RustBridge.getColumnPageIndex(handle, firstRow, fileOffset, compressed, nullCount, minLong, maxLong, n, actualPages);
        if (rc == RustBridge.RC_OVERFLOW) {
            // Page count grew between the two calls (shouldn't happen for an immutable
            // file, but handle defensively): re-size to the reported count and retry once.
            n = Math.toIntExact(actualPages.get(ValueLayout.JAVA_LONG, 0));
            firstRow = bufferPool.longs("idxFirstRow", Math.max(n, 1));
            fileOffset = bufferPool.longs("idxFileOffset", Math.max(n, 1));
            compressed = bufferPool.ints("idxCompressed", Math.max(n, 1));
            nullCount = bufferPool.longs("idxNullCount", Math.max(n, 1));
            minLong = bufferPool.longs("idxMin", Math.max(n, 1));
            maxLong = bufferPool.longs("idxMax", Math.max(n, 1));
            rc = RustBridge.getColumnPageIndex(handle, firstRow, fileOffset, compressed, nullCount, minLong, maxLong, n, actualPages);
            if (rc == RustBridge.RC_OVERFLOW) {
                throw new IOException("getColumnPageIndex: overflow persisted after retry");
            }
        }

        long[] firstRowArr = toLongArray(firstRow, n);
        long[] fileOffsetArr = toLongArray(fileOffset, n);
        int[] compressedArr = toIntArray(compressed, n);
        long[] nullCountArr = toLongArray(nullCount, n);
        long[] minArr = toLongArray(minLong, n);
        long[] maxArr = toLongArray(maxLong, n);

        // The per-page first-row offsets don't encode the last page's length, so take the
        // file's authoritative row count from metadata. open() is invoked once per column,
        // so this extra metadata read is not on the hot path.
        long totalRows = RustBridge.getFileMetadata(file.toString()).numRows();
        return new ColumnPageIndex(firstRowArr, fileOffsetArr, compressedArr, nullCountArr, minArr, maxArr, totalRows);
    }

    private static long[] toLongArray(MemorySegment seg, int n) {
        if (n == 0) {
            return new long[0];
        }
        return seg.asSlice(0, (long) n * ValueLayout.JAVA_LONG.byteSize()).toArray(ValueLayout.JAVA_LONG);
    }

    private static int[] toIntArray(MemorySegment seg, int n) {
        if (n == 0) {
            return new int[0];
        }
        return seg.asSlice(0, (long) n * ValueLayout.JAVA_INT.byteSize()).toArray(ValueLayout.JAVA_INT);
    }

    /** Decodes a UTF-8 byte slice from the binary page cache. */
    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void ensureOpen() {
        if (handle == CLOSED_HANDLE) {
            throw new IllegalStateException("ParquetColumnReader is closed");
        }
    }

    /** Idempotent: releases the native handle exactly once; never leaks on repeated calls. */
    @Override
    public void close() throws IOException {
        if (handle == CLOSED_HANDLE) {
            return;
        }
        // Emit the per-column cache effectiveness summary before releasing the handle.
        if (stats.isEmpty() == false) {
            logger.info("[PARQUET_DV_CACHE_STATS] col='{}' file={} | {}", column, file, stats.summary());
        }
        long h = handle;
        handle = CLOSED_HANDLE;
        cache = null;
        RustBridge.closeColumnReader(h);
    }
}
