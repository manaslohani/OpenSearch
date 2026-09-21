/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListValueReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * {@link SortedNumericDocValues} over a repeated Parquet primitive column, one Parquet row per document.
 *
 * <p>Lucene requires each document's values ascending, so rows are sorted on read unless
 * {@code valuesSorted} says the writer already sorted them.
 */
public final class ParquetSortedNumericDocValues extends SortedNumericDocValues {

    private final ListValueReader reader;
    private final int maxDoc;
    private final boolean valuesSorted;

    private int doc = -1;
    /** Reused per-document value buffer, grown as needed; only {@code [0, count)} is live. */
    private long[] values = new long[8];
    private int count;
    private int cursor;

    /**
     * @param valuesSorted whether the writer stored each row ascending; false sorts on read
     */
    public ParquetSortedNumericDocValues(ListValueReader reader, int maxDoc, boolean valuesSorted) {
        this.reader = reader;
        this.maxDoc = maxDoc;
        this.valuesSorted = valuesSorted;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        count = 0;
        cursor = 0;
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        DecodedListBatch batch = reader.decodedListBatch();
        if (batch == null || batch.contains(target) == false) {
            reader.loadListBatchContaining(target);
            batch = reader.decodedListBatch();
        }
        if (batch == null || batch.contains(target) == false) {
            throw new IllegalStateException("list batch does not contain target doc " + target);
        }
        int start = batch.startOffset(target);
        int end = batch.endOffset(target);
        DecodedBatch children = batch.childValues();

        ensureCapacity(end - start);
        int n = 0;
        for (int child = start; child < end; child++) {
            // Null elements are dropped: SortedNumericDocValues has no per-value absence.
            if (children.isPresent(child)) {
                values[n++] = children.valueAt(child);
            }
        }
        count = n;
        cursor = 0;
        if (count == 0) {
            // Null and empty lists both mean no values, reported as advanceExact == false.
            return false;
        }
        // Lucene requires ascending values; skip only when the writer already sorted.
        if (valuesSorted == false) {
            Arrays.sort(values, 0, count);
        }
        return true;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public long nextValue() {
        return values[cursor++];
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    private void ensureCapacity(int needed) {
        if (needed > values.length) {
            values = new long[Math.max(needed, values.length * 2)];
        }
    }
}
