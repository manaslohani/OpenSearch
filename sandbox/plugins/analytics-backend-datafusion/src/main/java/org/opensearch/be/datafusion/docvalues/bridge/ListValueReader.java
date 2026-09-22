/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import java.io.IOException;

/** Minimal decoded-list-batch source used by a multi-valued DocValues iterator. */
public interface ListValueReader {

    /** The currently decoded list batch, or {@code null} when none is loaded. */
    DecodedListBatch decodedListBatch();

    /** Loads a decoded list batch containing {@code row}. */
    void loadListBatchContaining(long row) throws IOException;
}
