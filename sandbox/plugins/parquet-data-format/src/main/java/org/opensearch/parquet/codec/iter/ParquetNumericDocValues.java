/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.NumericDocValues;
import org.opensearch.index.fielddata.BulkLongDocValues;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/**
 * Cache-aware {@link NumericDocValues} over a single-valued Parquet primitive column.
 *
 * <p>Hot path: a presence bit-test plus a {@code long[]} index lookup against the column
 * reader's current {@link PageCache}. Cold path (page miss): {@link ParquetColumnReader#loadPageContaining}
 * decodes the page (or applies the Layer 4 all-nulls skip). Float/double values are stored as
 * raw IEEE-754 bits at page-decode time, so {@link #longValue()} returns the Lucene-encoded
 * form directly.
 *
 * <p>Implements {@link BulkLongDocValues}: for a match-all aggregation the resident {@link PageCache}
 * holds a whole page's values contiguously in a {@code long[]}, so a doc-id run inside a present page
 * can be handed to the aggregator in one {@link System#arraycopy} instead of the per-doc chain.
 */
public final class ParquetNumericDocValues extends NumericDocValues implements BulkLongDocValues {

    private final ParquetColumnReader reader;
    private final int maxDoc;

    private int doc = -1;
    private long currentValue;
    private boolean currentPresent;

    public ParquetNumericDocValues(ParquetColumnReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return false;
        }
        doc = target;
        PageCache cache = reader.cache();
        if (cache != null && target <= cache.lastRow && target >= cache.firstRow) {
            // Layer 1/2 hit — served from the resident page, no FFM crossing.
        } else {
            // Layer 1/2 miss — load the page containing this row (Layer 3 → 4 → FFM).
            reader.loadPageContaining(target);
            cache = reader.cache();
            if (cache == null) { // Layer 4: page is all-nulls.
                currentPresent = false;
                currentValue = 0L;
                return false;
            }
        }
        // For a required (no-null) page, presence is a constant true — skip the presenceBits[] load
        // and mask entirely (that bit-test was a large slice of per-doc cost on dense columns like
        // UserID). Nullable pages still consult the bitset. Short-circuit keeps valueAt off the
        // absent branch.
        currentPresent = cache.allPresent || cache.isPresent(target);
        currentValue = currentPresent ? cache.valueAt(target) : 0L;
        return currentPresent;
    }

    /**
     * Bulk raw-long read for a match-all aggregation over a fully-present page. Loads the page
     * containing {@code minDoc} if needed, then, when that page has no nulls, copies the contiguous
     * run {@code [minDoc, pageEnd]} straight out of the resident {@code long[]} in one arraycopy.
     * Returns the number of docs written (capped by {@code maxExclusive} and {@code dst.length}); the
     * caller resumes from {@code minDoc + count}. Returns 0 to signal "fall back to per-doc" — when
     * the page is not fully present (nullable column) or {@code minDoc} is absent — so correctness
     * never depends on the bulk path (it only ever produces the same values the per-doc path would).
     */
    @Override
    public int fillLongs(int minDoc, int maxExclusive, long[] dst) throws IOException {
        if (minDoc >= maxDoc || minDoc >= maxExclusive) {
            return 0;
        }
        PageCache cache = reader.cache();
        if (cache == null || minDoc > cache.lastRow || minDoc < cache.firstRow) {
            reader.loadPageContaining(minDoc);
            cache = reader.cache();
            if (cache == null) { // all-nulls page: nothing present here, let caller handle per-doc
                return 0;
            }
        }
        // Bulk copy only when the whole page is present; a nullable page needs the per-doc presence
        // test, so signal fallback. (allPresent is the common case for required columns like UserID.)
        if (cache.allPresent == false) {
            return 0;
        }
        int start = (int) (minDoc - cache.firstRow);
        // Run extends to the end of this page, clamped by the caller's range and the buffer size.
        int pageRemaining = (int) (cache.lastRow - minDoc + 1);
        int wantRange = maxExclusive - minDoc;
        int n = Math.min(Math.min(pageRemaining, wantRange), dst.length);
        if (n <= 0) {
            return 0;
        }
        System.arraycopy(cache.values, start, dst, 0, n);
        // Keep the iterator's cursor coherent with the last doc we served in bulk.
        doc = minDoc + n - 1;
        currentPresent = true;
        currentValue = dst[n - 1];
        return n;
    }

    @Override
    public long longValue() {
        return currentValue;
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
