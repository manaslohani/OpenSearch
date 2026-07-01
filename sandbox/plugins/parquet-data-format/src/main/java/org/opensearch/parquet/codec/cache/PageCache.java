/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Layer 1 (page-resident value cache) + Layer 2 (page-resident presence bitset) for a
 * single decoded Parquet page of a single-valued column.
 *
 * <p>Holds the inclusive global row range {@code [firstRow, lastRow]}, the decoded values
 * (raw {@code long} bits for primitives, or a backing byte buffer + CSR offsets for
 * {@code BYTE_ARRAY}), and a packed presence bitset with one bit per row in the page.
 * Iterators serve cache hits with a presence bit-test plus an index lookup — no FFM
 * call. A single instance per column is resident at a time (sliding window, ascending
 * doc IDs, no LRU).
 *
 * <p><b>Zero-copy primitive values.</b> For primitive columns {@link #values} references the
 * off-heap {@link MemorySegment} that the native decode wrote into <em>directly</em> — the
 * page's decoded {@code long}s are read in place via {@link #valueAt(long)} rather than copied
 * into a heap {@code long[]}. This is safe only because the backing segment is a
 * <em>per-column</em> pool slot (see {@code ParquetColumnReader.decodePage}): the segment a
 * cache holds is never reused by another column, and this column's own next page decode does
 * not run until this cache is replaced. Presence bits and the binary byte buffer are still
 * copied to the heap (small, and the binary path hands a {@code byte[]} to a {@code BytesRef}).
 *
 * <p>This is the task-3 (Layer 1/2) structure; the column-reader wrapper (task 2) populates
 * and owns it, so it is introduced here. Task 3 refines and adds dedicated unit tests.
 */
public final class PageCache {

    /** Inclusive global index of the first row in the cached page. */
    public long firstRow;
    /** Inclusive global index of the last row in the cached page. */
    public long lastRow;

    /**
     * Decoded raw bits, one {@code long} slot per row (primitive columns); {@code null} for
     * binary columns. This is an off-heap segment owned by the column's buffer-pool slot,
     * sliced to exactly {@code rowCount()} longs — read in place, never copied.
     */
    public MemorySegment values;

    /** Backing byte buffer for binary columns (concatenated value bytes). Null for primitives. */
    public byte[] byteBuf;
    /** CSR offsets into {@link #byteBuf}, length {@code rowsInPage + 1}. Null for primitives. */
    public int[] byteOffsets;

    /** Packed presence bitset: bit {@code i} set when row {@code firstRow + i} is non-null. */
    public long[] presenceBits;

    /** Number of rows in the cached page. */
    public int rowCount() {
        return (int) (lastRow - firstRow + 1);
    }

    /**
     * Constant-time presence test for a global row that lies within {@code [firstRow, lastRow]}.
     */
    public boolean isPresent(long row) {
        int idx = (int) (row - firstRow);
        return (presenceBits[idx >> 6] & (1L << (idx & 63))) != 0L;
    }

    /** Returns the raw {@code long} bits for a primitive value at the given global row. */
    public long valueAt(long row) {
        return values.getAtIndex(ValueLayout.JAVA_LONG, row - firstRow);
    }

    /** True when the given global row falls within this cached page's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }
}
