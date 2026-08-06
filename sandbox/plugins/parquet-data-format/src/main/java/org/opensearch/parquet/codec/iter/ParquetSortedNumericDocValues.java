/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.LongsRef;
import org.opensearch.parquet.bridge.NumericPageReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * {@link SortedNumericDocValues} over a multi-valued Parquet primitive column.
 *
 * <p>Each {@link #advanceExact(int)} reads the row's repeated values from the resident adaptive
 * batch, loading the next batch when necessary, then sorts them ascending to satisfy Lucene's
 * contract. The per-doc values are buffered in a reused array and walked by
 * {@link #nextValue()}.
 */
public final class ParquetSortedNumericDocValues extends SortedNumericDocValues {

    private final NumericPageReader reader;
    private final int maxDoc;

    private int doc = -1;
    /** Reused per-doc value buffer; the reader fills it in place, so steady state allocates nothing. */
    private final LongsRef values = new LongsRef(8);
    private int count;
    private int cursor;

    public ParquetSortedNumericDocValues(NumericPageReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            count = 0;
            cursor = 0;
            return false;
        }
        doc = target;
        reader.readRepeatedLongsAtRow(target, values);
        count = values.length;
        cursor = 0;
        if (count == 0) {
            return false; // empty list = missing.
        }
        // Lucene's SortedNumeric contract requires ascending order.
        Arrays.sort(values.longs, 0, count);
        return true;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public long nextValue() {
        return values.longs[cursor++];
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
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
}
