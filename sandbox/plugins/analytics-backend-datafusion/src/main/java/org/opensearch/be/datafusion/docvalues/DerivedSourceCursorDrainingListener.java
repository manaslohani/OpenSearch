/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

/**
 * Frees the fetch thread's reused derived-source cursors at the end of each fetch phase - on success and
 * on failure - so a long-lived reused reader does not pin native cursors idle between pages.
 *
 * <p>A scroll keeps one wrapped {@code DirectoryReader} (hence one {@link DerivedSourceCursorCache}) for the
 * whole keep-alive, so without this the thread that derived a page's {@code _source} would hold its per-column
 * cursors (measured at hundreds of MiB native) idle until the next page or the scroll's expiry. Draining the
 * calling thread's entries here releases them immediately while the reader stays open for later pages; the
 * wrapping {@link ParquetDocValuesDirectoryReader#doClose()} remains the request-end backstop.
 *
 * <p>Registered per pluggable-data-format index via {@code IndexModule.addSearchOperationListener}. The hook is
 * safe because {@code SearchService} runs both callbacks synchronously, on the same thread that executed
 * {@code FetchPhase.execute}, after the last hit's derived {@code _source} has been materialized into the hit
 * (the fetch loop is single-threaded per request and response serialization reads the stored bytes, not the
 * cursors). It therefore only frees cursors the fetch thread is done with.
 */
public final class DerivedSourceCursorDrainingListener implements SearchOperationListener {

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        drain(searchContext);
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        drain(searchContext);
    }

    private static void drain(SearchContext searchContext) {
        ContextIndexSearcher searcher = searchContext.searcher();
        if (searcher == null) {
            return;
        }
        ParquetDocValuesDirectoryReader.drainCurrentThread(searcher.getDirectoryReader());
    }
}
