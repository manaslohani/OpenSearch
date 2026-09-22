/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * One decoded batch of a repeated (list) Parquet column, read in place. Row {@code r}'s values are
 * the child range {@code [offset(r), offset(r + 1))}. The offsets and child buffers are borrowed
 * Arrow buffers, valid only until the next batch call on the owning cursor.
 *
 * @param firstRow    inclusive global index of the first row in the batch
 * @param lastRow     inclusive global index of the last row in the batch
 * @param offsets     off-heap view of the {@code i32} list offsets at {@code firstRow};
 *                    holds {@code lastRow - firstRow + 2} entries
 * @param childValues off-heap view of the flattened child values, addressed from child index 0
 */
public record DecodedListBatch(long firstRow, long lastRow, MemorySegment offsets, DecodedBatch childValues) {

    /** True when the given global row falls within this batch's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }

    /** First child index of {@code row}'s value list. The caller must have accepted {@code row} via {@link #contains}. */
    public int startOffset(long row) {
        return offsets.getAtIndex(ValueLayout.JAVA_INT, row - firstRow);
    }

    /** One-past-the-last child index of {@code row}'s value list. */
    public int endOffset(long row) {
        return offsets.getAtIndex(ValueLayout.JAVA_INT, row - firstRow + 1);
    }
}
