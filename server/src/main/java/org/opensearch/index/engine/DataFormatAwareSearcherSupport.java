/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.engine.exec.SearchableDirectoryReaderProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builds the Lucene {@link Engine.SearcherSupplier} that lets {@code _search} reach a composite,
 * data-format-aware shard.
 *
 * <p>Shared by every data-format-aware engine that can serve searches, because the binding must be
 * identical whichever engine produced the reader: {@link DataFormatAwareEngine} for a hot shard and
 * {@link DataFormatAwareReadOnlyEngine} for one tiered to warm. It pins the reader's
 * {@link org.opensearch.index.engine.exec.coord.CatalogSnapshot} for the supplier's lifetime, obtains a
 * {@link DirectoryReader} from the format whose reader is a {@link SearchableDirectoryReaderProvider},
 * wraps it in an {@link OpenSearchDirectoryReader}, and applies the caller's searcher wrapper.
 */
final class DataFormatAwareSearcherSupport {

    private DataFormatAwareSearcherSupport() {}

    /**
     * Builds a point-in-time searcher supplier over {@code readerRef}'s reader. The
     * {@link org.opensearch.index.engine.exec.coord.CatalogSnapshot} the reader was taken against is pinned
     * for the supplier's lifetime; the {@link DirectoryReader} exposed by the searchable format is wrapped
     * in an {@link OpenSearchDirectoryReader} so the standard searcher-wrapping path (and any registered
     * reader wrappers) apply.
     *
     * <p>Takes ownership of {@code readerRef}: it is released by the returned supplier's {@code close()},
     * or immediately if this method throws.
     */
    static Engine.SearcherSupplier acquireSearcherSupplier(
        ShardId shardId,
        EngineConfig engineConfig,
        GatedCloseable<Reader> readerRef,
        Function<Engine.Searcher, Engine.Searcher> wrapper,
        Logger logger
    ) {
        try {
            final DirectoryReader rawDirectoryReader = extractDirectoryReader(shardId, engineConfig, readerRef.get());
            // IndexShard.wrapSearcher later asserts the reader is an OpenSearchDirectoryReader.
            final DirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(rawDirectoryReader, shardId);
            return new Engine.SearcherSupplier(wrapper) {
                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    return new Engine.Searcher(
                        source,
                        directoryReader,
                        engineConfig.getSimilarity(),
                        engineConfig.getQueryCache(),
                        engineConfig.getQueryCachingPolicy(),
                        () -> {}
                    );
                }

                @Override
                protected void doClose() {
                    IOUtils.closeWhileHandlingException(readerRef);
                }
            };
        } catch (IllegalStateException e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw e;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw new EngineException(shardId, "failed to build searcher supplier from composite reader", e);
        }
    }

    /**
     * Resolves the Lucene {@link DirectoryReader} to search over by capability rather than by format name:
     * the reader is taken from whichever registered format exposes a {@link SearchableDirectoryReaderProvider}.
     * Selecting by capability keeps the server core free of concrete format names, which
     * belong to the plugins that contribute the formats. Exactly one format must be searchable: none means
     * the index cannot serve searches, more than one is an ambiguous configuration.
     */
    private static DirectoryReader extractDirectoryReader(ShardId shardId, EngineConfig engineConfig, Reader reader) {
        List<String> matches = new ArrayList<>();
        SearchableDirectoryReaderProvider provider = null;
        for (DataFormat format : engineConfig.getDataFormatRegistry().getRegisteredFormats()) {
            Object formatReader = reader.reader(format);
            if (formatReader instanceof SearchableDirectoryReaderProvider searchable) {
                matches.add(format.name());
                provider = searchable;
            }
        }
        if (matches.isEmpty()) {
            throw new IllegalStateException(
                "No searchable reader (SearchableDirectoryReaderProvider) available for composite index " + shardId
            );
        }
        if (matches.size() > 1) {
            throw new IllegalStateException(
                "Multiple searchable readers for composite index " + shardId + "; ambiguous formats " + matches
            );
        }
        return provider.directoryReader();
    }
}
