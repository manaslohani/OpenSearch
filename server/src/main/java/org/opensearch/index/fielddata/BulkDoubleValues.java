/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import java.io.IOException;

/**
 * Optional bulk read for a single-valued {@link NumericDoubleValues} that can hand back a contiguous
 * run of {@code double} values (already converted from the underlying storage encoding) without a
 * per-doc {@code advanceExact}/{@code doubleValue} round trip.
 *
 * <p>Implemented by the double wrappers over a columnar {@link BulkLongDocValues} source: the wrapper
 * pulls raw longs in bulk from the underlying source and applies its own per-type conversion
 * (long-to-double cast, sortable-bits decode, etc.), so conversion knowledge stays in the wrapper and
 * the aggregator only sees ready-to-sum doubles. Callers must fall back to the per-doc path when the
 * returned count is short.
 *
 * @opensearch.internal
 */
public interface BulkDoubleValues {

    /**
     * Fills {@code dst} with the converted {@code double} values for the docs in
     * {@code [minDoc, maxExclusive)} that form a contiguous, fully-present, single-valued run starting
     * at {@code minDoc}, and returns how many were written. A return of {@code 0} means no value could
     * be produced at {@code minDoc}; the caller should fall back to the per-doc path for that doc. The
     * returned count never exceeds {@code min(maxExclusive - minDoc, dst.length)}.
     */
    int fillDoubles(int minDoc, int maxExclusive, double[] dst) throws IOException;
}
