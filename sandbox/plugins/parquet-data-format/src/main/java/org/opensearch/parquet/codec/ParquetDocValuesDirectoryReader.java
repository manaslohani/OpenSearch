/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.cache.QueryParquetStats;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A {@link FilterDirectoryReader} that wraps each leaf in a {@link ParquetDocValuesLeafReader} so
 * that Parquet-resident doc values (fields that live only in Parquet and have no Lucene segment
 * {@link org.apache.lucene.index.FieldInfo}) are served to the standard OpenSearch search and
 * aggregation path at read time.
 *
 * <p>Registered as the index reader wrapper via
 * {@code IndexModule.setReaderWrapper(...)}; OpenSearch applies it inside
 * {@code IndexShard#wrapSearcher}, which runs on the composite engine's
 * {@code DataFormatAwareEngine} searcher path. Per-leaf wrapping self-gates: a leaf is only wrapped
 * when a Parquet file resolves for its segment and the mapping declares at least one Parquet-codec
 * field missing doc values in the Lucene segment (see {@link ParquetDocValuesLeafReader#wrapIfApplicable}).
 */
public final class ParquetDocValuesDirectoryReader extends FilterDirectoryReader {

    // Dedicated stats channel, NOT the class-named logger, so the per-query summary can be toggled
    // in isolation (logger.org.opensearch.parquet.stats.query=TRACE) without turning on any other
    // codec class's logging. Enabling it affects only this one diagnostic line.
    private static final Logger statsLogger = LogManager.getLogger("org.opensearch.parquet.stats.query");

    // Parallel timing channel — toggled independently of statsLogger. When at TRACE, the native
    // phase timers are enabled and baselined at query start, and per-query nanoTime is accumulated.
    private static final Logger timingLog = LogManager.getLogger("org.opensearch.parquet.timing");

    private final MapperService mapperService;
    private final QueryParquetStats queryStats;

    private ParquetDocValuesDirectoryReader(DirectoryReader in, MapperService mapperService, QueryParquetStats queryStats)
        throws IOException {
        super(in, new ParquetSubReaderWrapper(mapperService, queryStats));
        this.mapperService = mapperService;
        this.queryStats = queryStats;
        // Baseline the process-wide liquid counters so doClose() can report per-query deltas. Only
        // when the summary will actually be logged — the snapshot is an FFM crossing, so this keeps
        // it fully off (no native call) on the raw-performance path where TRACE is disabled.
        if (statsLogger.isTraceEnabled()) {
            RustBridge.LiquidCacheStats base = RustBridge.liquidCacheStats();
            queryStats.captureLiquidBaseline(base.hits(), base.misses(), base.puts());
        }
        // Parallel timing path: enable native phase timers and baseline them so doClose() can report
        // per-query deltas. Fully off (no native call, no nanoTime) when the timing channel is not at TRACE.
        if (timingLog.isTraceEnabled()) {
            RustBridge.timingSetEnabled(true);
            RustBridge.TimingStats tbase = RustBridge.timingSnapshot();
            queryStats.captureTimingBaseline(tbase.getNanos(), tbase.decodeNanos(), tbase.putNanos());
        }
    }

    /**
     * Wraps {@code in} so Parquet-resident doc values become visible to Lucene query/aggregation
     * code paths.
     */
    public static DirectoryReader wrap(DirectoryReader in, MapperService mapperService) throws IOException {
        return new ParquetDocValuesDirectoryReader(in, mapperService, new QueryParquetStats());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        // A reopened reader is a fresh search view; give it its own accumulator.
        return new ParquetDocValuesDirectoryReader(in, mapperService, new QueryParquetStats());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // Delegate to the wrapped reader's cache helper: this reader does not change the set of
        // live docs, so it is cache-coherent with the underlying OpenSearchDirectoryReader.
        return in.getReaderCacheHelper();
    }

    @Override
    protected void doClose() throws IOException {
        IOException first = null;
        try {
            for (LeafReader leaf : getSequentialSubReaders()) {
                if (leaf instanceof ParquetDocValuesLeafReader parquetLeaf) {
                    try {
                        parquetLeaf.closeParquetResources();
                    } catch (IOException e) {
                        if (first == null) {
                            first = e;
                        }
                    }
                }
            }
        } finally {
            try {
                super.doClose();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
            // The per-query [PARQUET_DV_QUERY_STATS] summary is TRACE-only so it is NOT emitted during
            // raw-performance runs. Counters are always accumulated (cheap); to see the summary enable
            // the dedicated stats channel: logger.org.opensearch.parquet.stats.query=TRACE.
            if (queryStats != null && queryStats.isEmpty() == false && statsLogger.isTraceEnabled()) {
                RustBridge.LiquidCacheStats now = RustBridge.liquidCacheStats();
                statsLogger.trace("[PARQUET_DV_QUERY_STATS] {}", queryStats.summary(now.hits(), now.misses(), now.puts()));
            }
            if (queryStats != null && queryStats.isEmpty() == false && timingLog.isTraceEnabled()) {
                RustBridge.TimingStats tnow = RustBridge.timingSnapshot();
                timingLog.trace("[PARQUET_DV_TIMING] {}", queryStats.timingSummary(tnow.getNanos(), tnow.decodeNanos(), tnow.putNanos()));
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /** Per-leaf wrapper that swaps in {@link ParquetDocValuesLeafReader} when applicable. */
    private static final class ParquetSubReaderWrapper extends SubReaderWrapper {
        private final MapperService mapperService;
        private final QueryParquetStats queryStats;

        private ParquetSubReaderWrapper(MapperService mapperService, QueryParquetStats queryStats) {
            this.mapperService = mapperService;
            this.queryStats = queryStats;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return ParquetDocValuesLeafReader.wrapIfApplicable(reader, mapperService, queryStats);
            } catch (IOException e) {
                // SubReaderWrapper.wrap cannot throw checked exceptions; surface as unchecked so
                // the search fails loudly rather than silently dropping Parquet doc values.
                throw new UncheckedIOException("failed to wrap leaf reader for Parquet doc values", e);
            }
        }
    }
}
