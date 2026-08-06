/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.lucene.util.LongsRef;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/** Minimal page/batch source used by the numeric DocValues iterator. */
public interface NumericPageReader {

    /** The currently decoded row range, or {@code null} when none is loaded. */
    PageCache cache();

    /** Loads a decoded range containing {@code row}. */
    void loadPageContaining(long row) throws IOException;

    /**
     * Reads all primitive values for one repeated row into {@code dst}, growing
     * {@code dst.longs} when needed and setting {@code dst.length}. Steady-state
     * calls must not allocate: this runs once per document on the hot path.
     */
    void readRepeatedLongsAtRow(long row, LongsRef dst) throws IOException;
}
