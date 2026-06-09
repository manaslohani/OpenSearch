/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

/**
 * Per-resolver counters for the docId&rarr;Parquet-row translation layer (the
 * {@code RowIdRemappingDocValues} resolver backed by {@code __row_id__}).
 *
 * <p>One instance is created per codec DocValues iterator (each builds its own resolver), so this is
 * single-threaded for the lifetime of one iterator &mdash; counters are plain {@code long}s with no
 * synchronization, mirroring {@link CacheStats}. It is registered once with the query-scoped
 * {@code QueryParquetStats} and summed live at end of query.
 *
 * <h2>Why timing is sampled</h2>
 * {@code toRowId(docId)} runs once per document &mdash; up to 100M+ times per query. Calling
 * {@code System.nanoTime()} on every call (~20-30ns each) would add seconds of pure measurement
 * overhead and corrupt the result. Instead the resolver times only one in every
 * {@code 1/SAMPLE_RATE} calls and the total is extrapolated from the samples (the same approach
 * OpenSearch's own {@code search.profile.Timer} uses).
 */
public final class RowIdStats {

    /** Sample one call in every {@code SAMPLE_MASK + 1} (power-of-two mask for a cheap test). */
    public static final long SAMPLE_MASK = 1023L;

    /** True when this resolver is the no-op IDENTITY mapping (segment has docId == rowId). */
    private boolean identity;

    /** Number of {@code toRowId} calls that performed a {@code __row_id__} lookup. */
    private long lookups;

    /** Number of timed samples taken. */
    private long sampledCalls;

    /** Total nanoseconds across the sampled calls. */
    private long sampledNanos;

    /** Marks this resolver as the IDENTITY (no-op) mapping. */
    public void markIdentity() {
        identity = true;
    }

    /** Records one {@code __row_id__} lookup (called per document on a backed resolver). */
    public void recordLookup() {
        lookups++;
    }

    /** Records one timed sample's elapsed nanoseconds. */
    public void recordSample(long nanos) {
        sampledCalls++;
        sampledNanos += nanos;
    }

    public boolean isIdentity() {
        return identity;
    }

    public long lookups() {
        return lookups;
    }

    public long sampledCalls() {
        return sampledCalls;
    }

    public long sampledNanos() {
        return sampledNanos;
    }

    /**
     * Total time spent in this resolver's {@code __row_id__} lookups, extrapolated from the samples:
     * {@code avgSampleNanos * lookups}. Returns 0 when nothing was sampled.
     */
    public long estimatedTotalNanos() {
        if (sampledCalls == 0) {
            return 0L;
        }
        return (long) ((double) sampledNanos / sampledCalls * lookups);
    }

    /** True when this resolver did no work (not used / no lookups and not marked identity). */
    public boolean isEmpty() {
        return identity == false && lookups == 0;
    }
}
