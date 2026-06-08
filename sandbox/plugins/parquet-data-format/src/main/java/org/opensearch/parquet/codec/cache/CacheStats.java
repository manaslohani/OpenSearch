/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.util.Locale;

/**
 * Per-column hit/miss counters for the Parquet DocValues cache layers, used to measure how much
 * each caching layer contributes during a query. One instance is owned by each
 * {@code ParquetColumnReader}; it is single-threaded (one segment per query thread), so counters
 * are plain {@code long}s with no synchronization.
 *
 * <p>Layer mapping (per the codec design):
 * <ul>
 *   <li><b>Layer 1/2</b> — page-resident value + presence cache. A <em>hit</em> is a row served from
 *       the currently resident {@link PageCache} with no FFM call; a <em>miss</em> triggers a page load.</li>
 *   <li><b>Layer 3</b> — OffsetIndex jump table ({@code pageForRow} binary search), consulted on every miss.</li>
 *   <li><b>Layer 4</b> — page-stat all-nulls skip; a miss resolved without decoding the page.</li>
 *   <li><b>FFM</b> — calls that cross the native boundary (page decodes and slow-path single/repeated
 *       reads). This is the cost the upper layers exist to avoid.</li>
 * </ul>
 */
public final class CacheStats {

    // Layer 1/2 — page-resident value + presence cache.
    private long pageCacheHits;
    private long pageCacheMisses;
    private long presentValues;   // rows that resolved to a non-null value (data, not caching)
    private long absentValues;    // rows that resolved to null

    // Layer 3 — OffsetIndex jump-table lookups (one per miss).
    private long pageIndexLookups;

    // Layer 4 — all-nulls page skips (a miss resolved with no decode).
    private long allNullPageSkips;

    // FFM boundary crossings.
    private long pageDecodes;       // parquet_decode_page_at_row
    private long slowValueReads;    // parquet_read_value_at_row (single)
    private long slowRepeatedReads; // parquet_read_repeated_at_row

    /** Records a Layer 1/2 page-cache hit (row served from the resident page, no FFM). */
    public void pageCacheHit() {
        pageCacheHits++;
    }

    /** Records a Layer 1/2 page-cache miss (a page load is required). */
    public void pageCacheMiss() {
        pageCacheMisses++;
    }

    /** Records that a resolved row had a present (non-null) value. */
    public void present() {
        presentValues++;
    }

    /** Records that a resolved row was null/absent. */
    public void absent() {
        absentValues++;
    }

    /** Records a Layer 3 jump-table lookup ({@code pageForRow}). */
    public void pageIndexLookup() {
        pageIndexLookups++;
    }

    /** Records a Layer 4 all-nulls page skip (miss resolved without a decode). */
    public void allNullPageSkip() {
        allNullPageSkips++;
    }

    /** Records an FFM page decode crossing. */
    public void pageDecode() {
        pageDecodes++;
    }

    /** Records an FFM slow-path single-value read crossing. */
    public void slowValueRead() {
        slowValueReads++;
    }

    /** Records an FFM slow-path repeated-value read crossing. */
    public void slowRepeatedRead() {
        slowRepeatedReads++;
    }

    public long pageCacheHits() {
        return pageCacheHits;
    }

    public long pageCacheMisses() {
        return pageCacheMisses;
    }

    public long pageDecodes() {
        return pageDecodes;
    }

    public long allNullPageSkips() {
        return allNullPageSkips;
    }

    /** Total page-cache lookups (hits + misses). */
    public long pageCacheLookups() {
        return pageCacheHits + pageCacheMisses;
    }

    /** Layer 1/2 hit rate in [0,1]; 0 when there were no lookups. */
    public double pageCacheHitRate() {
        long total = pageCacheLookups();
        return total == 0 ? 0.0 : (double) pageCacheHits / total;
    }

    /** Total FFM boundary crossings across all access paths. */
    public long ffmCrossings() {
        return pageDecodes + slowValueReads + slowRepeatedReads;
    }

    /** True when no access has been recorded (used to suppress empty summaries). */
    public boolean isEmpty() {
        return pageCacheLookups() == 0 && slowValueReads == 0 && slowRepeatedReads == 0;
    }

    /** A single-line, human-readable summary suitable for an INFO log on reader close. */
    public String summary() {
        return String.format(
            Locale.ROOT,
            "L1/2 page-cache: hits=%d misses=%d (hitRate=%.2f%%) | L3 jumpTableLookups=%d | "
                + "L4 allNullSkips=%d | FFM: pageDecodes=%d slowValueReads=%d slowRepeatedReads=%d (totalCrossings=%d) | "
                + "values: present=%d absent=%d",
            pageCacheHits,
            pageCacheMisses,
            pageCacheHitRate() * 100.0,
            pageIndexLookups,
            allNullPageSkips,
            pageDecodes,
            slowValueReads,
            slowRepeatedReads,
            ffmCrossings(),
            presentValues,
            absentValues
        );
    }
}
