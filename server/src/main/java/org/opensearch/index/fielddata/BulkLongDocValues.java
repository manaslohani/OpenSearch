/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;

/**
 * Optional bulk read for a single-valued {@link NumericDocValues} that can hand back a contiguous
 * run of raw {@code long} values without a per-doc {@code advanceExact}/{@code longValue} round trip.
 *
 * <p>Implemented by columnar doc-values sources (e.g. the Parquet codec) whose values for a doc-id
 * range are laid out contiguously in memory, so a match-all aggregation can consume them in a tight,
 * vectorizable loop instead of the per-doc virtual-call chain. Sources that cannot satisfy a bulk
 * read (or the run is not fully present, or crosses an internal boundary) return a short count and
 * the caller falls back to the per-doc path for the remainder.
 *
 * @opensearch.internal
 */
public interface BulkLongDocValues {

    /**
     * Fills {@code dst} with the raw {@code long} values for the docs in {@code [minDoc, maxExclusive)}
     * that form a contiguous, fully-present run starting at {@code minDoc}, and returns how many were
     * written. A return of {@code 0} means no value could be produced at {@code minDoc} (e.g. the doc
     * is absent, or a bulk read is not possible right now); the caller should fall back to the per-doc
     * path for {@code minDoc}. The returned count never exceeds {@code min(maxExclusive - minDoc, dst.length)}.
     *
     * @param minDoc       first doc id to read (inclusive)
     * @param maxExclusive one past the last doc id the caller wants
     * @param dst          destination buffer for raw long bits
     * @return number of leading docs from {@code minDoc} written into {@code dst}
     */
    int fillLongs(int minDoc, int maxExclusive, long[] dst) throws IOException;
}
