/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * Query-scoped accumulator that sums per-column {@link CacheStats} across every segment touched by
 * a single search, so the Parquet read-path cost can be summarized in one log line per query.
 *
 * <p>One instance is created per search (by {@code ParquetDocValuesDirectoryReader}) and shared by
 * all the per-segment {@code ParquetColumnReader}s that search opens. Each column reader
 * {@link #register(CacheStats) registers} its {@link CacheStats} when it is opened; the values are
 * summed <b>live</b> at {@link #summary()} time. Registering at open (rather than merging at close)
 * means the roll-up does not depend on reader-close ordering — by the time the per-query summary is
 * produced (end of search), every registered reader has finished collecting, so the live sums are
 * final. The registry is a {@link ConcurrentLinkedQueue} so concurrent segment slices can register
 * safely, and each reader mutates only its own {@link CacheStats} during collection.
 */
public final class QueryParquetStats {

    private final ConcurrentLinkedQueue<CacheStats> registered = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<RowIdStats> rowIdRegistered = new ConcurrentLinkedQueue<>();
    private final LongAdder producerSetupNanos = new LongAdder();
    private final LongAdder readerOpenNanos = new LongAdder();
    private final LongAdder readerCloseNanos = new LongAdder();

    /** Registers a column reader's stats; its counters are summed live when {@link #summary()} runs. */
    public void register(CacheStats s) {
        if (s != null) {
            registered.add(s);
        }
    }

    /** Registers a resolver's docId&rarr;row translation stats; summed live at {@link #summary()}. */
    public void registerRowId(RowIdStats s) {
        if (s != null) {
            rowIdRegistered.add(s);
        }
    }

    /** Adds time spent constructing a producer (file resolve + metadata read) for one segment. */
    public void addProducerSetupNanos(long nanos) {
        producerSetupNanos.add(nanos);
    }

    /** Adds time spent in the native {@code openColumnReader} FFM crossing (once per column). */
    public void addReaderOpenNanos(long nanos) {
        readerOpenNanos.add(nanos);
    }

    /** Adds time spent in the native {@code closeColumnReader} FFM crossing (once per column). */
    public void addReaderCloseNanos(long nanos) {
        readerCloseNanos.add(nanos);
    }

    /** True when nothing was recorded (used to suppress an empty summary). */
    public boolean isEmpty() {
        return registered.isEmpty() && rowIdRegistered.isEmpty() && producerSetupNanos.sum() == 0;
    }

    /** A single-line, human-readable per-query summary. */
    public String summary() {
        long columns = 0;
        long hits = 0, misses = 0, present = 0, absent = 0;
        long jumpTableLookups = 0, allNullSkips = 0;
        long pageDecodes = 0, slowValueReads = 0, slowRepeatedReads = 0;
        long pageDecodeNanos = 0, pageIndexLoadNanos = 0, slowReadNanos = 0;
        for (CacheStats s : registered) {
            columns++;
            hits += s.pageCacheHits();
            misses += s.pageCacheMisses();
            present += s.presentValues();
            absent += s.absentValues();
            jumpTableLookups += s.pageIndexLookups();
            allNullSkips += s.allNullPageSkips();
            pageDecodes += s.pageDecodes();
            slowValueReads += s.slowValueReads();
            slowRepeatedReads += s.slowRepeatedReads();
            pageDecodeNanos += s.pageDecodeNanos();
            pageIndexLoadNanos += s.pageIndexLoadNanos();
            slowReadNanos += s.slowReadNanos();
        }
        // RowId docId->row translation layer. COUNTS ONLY — the per-doc time is not code-timed
        // (it is too hot to measure accurately); obtain its wall-clock from a CPU flamegraph.
        long rowIdResolvers = 0, rowIdIdentity = 0, rowIdLookups = 0;
        for (RowIdStats r : rowIdRegistered) {
            rowIdResolvers++;
            if (r.isIdentity()) {
                rowIdIdentity++;
            }
            rowIdLookups += r.lookups();
        }

        long lookups = hits + misses;
        double hitRate = lookups == 0 ? 0.0 : (double) hits / lookups * 100.0;
        double decodeMs = pageDecodeNanos / 1_000_000.0;
        double indexLoadMs = pageIndexLoadNanos / 1_000_000.0;
        double slowReadMs = slowReadNanos / 1_000_000.0;
        double setupMs = producerSetupNanos.sum() / 1_000_000.0;
        double readerOpenMs = readerOpenNanos.sum() / 1_000_000.0;
        double readerCloseMs = readerCloseNanos.sum() / 1_000_000.0;
        // Sum of the accurately-measured coarse sections only (excludes the un-timed per-doc loop).
        double measuredParquetMs = decodeMs + indexLoadMs + slowReadMs + setupMs + readerOpenMs + readerCloseMs;
        return String.format(
            Locale.ROOT,
            "segments/columns=%d | L1/2 cache: hits=%d misses=%d (hitRate=%.2f%%) | "
                + "L3 jumpTableLookups=%d | L4 allNullSkips=%d | "
                + "FFM: pageDecodes=%d slowValueReads=%d slowRepeatedReads=%d | "
                + "RowId: resolvers=%d identity=%d lookups=%d (time via flamegraph) | "
                + "timings(ms): pageDecode=%.1f pageIndexLoad=%.1f slowRead=%.1f "
                + "readerOpen=%.1f readerClose=%.1f producerSetup=%.1f measuredParquetTime=%.1f | "
                + "values: present=%d absent=%d",
            columns,
            hits,
            misses,
            hitRate,
            jumpTableLookups,
            allNullSkips,
            pageDecodes,
            slowValueReads,
            slowRepeatedReads,
            rowIdResolvers,
            rowIdIdentity,
            rowIdLookups,
            decodeMs,
            indexLoadMs,
            slowReadMs,
            readerOpenMs,
            readerCloseMs,
            setupMs,
            measuredParquetMs,
            present,
            absent
        );
    }
}
