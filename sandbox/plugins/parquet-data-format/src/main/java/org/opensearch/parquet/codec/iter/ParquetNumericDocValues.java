/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.NumericDocValues;
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
 */
public final class ParquetNumericDocValues extends NumericDocValues {

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
        // PROFILING BUILD: the per-doc body is split into named helper methods so each step
        // shows as its own flamegraph frame under `-XX:CompileCommand=dontinline,...`. Do NOT
        // merge this branch into a real optimization branch — the extra call layers add real
        // overhead and only exist to attribute advanceExact's self-time. See the run recipe in
        // the commit message. Steps: (a) bounds, resolvePage = (b) cache()+(c) range+(d) miss/decode,
        // (e) isPresent, (f) valueAt, plus the shared "absent" bookkeeping. After this split
        // advanceExact's own self-time is just call dispatch + the doc store + the null test.
        if (beyondMaxDoc(target)) {                    // (a) maxDoc bounds check
            return false;
        }
        doc = target;
        PageCache cache = resolvePage(target);         // (b) cache() + (c) range + (d) miss/decode
        if (cache == null) {                           // Layer 4 all-nulls (or miss resolved to null)
            return markAbsent();
        }
        return finishFromCache(cache, target);         // (e) isPresent + (f) valueAt
    }

    /**
     * Resolves the resident page for {@code target}: the current page on a Layer 1/2 hit, else loads
     * the page containing the row (Layer 3 → 4 → FFM → liquid) and returns it, or {@code null} when
     * that page is all-nulls. Extracted + dontinlined so the hit/miss resolution is a named frame and
     * advanceExact's own self-time collapses to near-zero (only the bounds-call, the doc store, the
     * null test, and call dispatch remain there — i.e. "technically nothing remains").
     */
    private PageCache resolvePage(int target) throws IOException {
        PageCache cache = reader.cache();              // (b) resident-page getter
        if (inResidentRange(cache, target)) {          // (c) hit/miss decision
            return cache;                              // Layer 1/2 hit — no FFM crossing
        }
        // Layer 1/2 miss — load the page containing this row (Layer 3 → 4 → FFM → liquid).
        reader.loadPageContaining(target);             // (d) decode/miss path
        return reader.cache();                         // freshly-decoded page, or null if all-nulls
    }

    /** (a) maxDoc bounds check. Extracted + dontinlined so its self-time is a separate frame. */
    private boolean beyondMaxDoc(int target) {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return true;
        }
        return false;
    }

    /** (c) resident-page range check (hit vs miss). Extracted + dontinlined for its own frame. */
    private static boolean inResidentRange(PageCache cache, int target) {
        return cache != null && target <= cache.lastRow && target >= cache.firstRow;
    }

    /**
     * (e) presence bit-test + (f) value read, split from advanceExact so PageCache.isPresent and
     * PageCache.valueAt (dontinlined) attribute their own self-time here instead of being inlined
     * into advanceExact's opaque self-blob.
     */
    private boolean finishFromCache(PageCache cache, int target) {
        currentPresent = cache.isPresent(target);      // (e)
        currentValue = currentPresent ? cache.valueAt(target) : 0L; // (f)
        return currentPresent;
    }

    /**
     * The "produce an absent result" bookkeeping, shared by the all-nulls miss tail and (via the
     * bounds-check path) any absent exit. Dontinlined so this tail's cost is a named frame; on a
     * dense/required column (e.g. UserID) it should sample near-zero, proving it is not a cost center.
     */
    private boolean markAbsent() {
        currentPresent = false;
        currentValue = 0L;
        return false;
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
