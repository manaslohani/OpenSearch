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
    private final LongAdder producerSetupNanos = new LongAdder();

    /** Registers a column reader's stats; its counters are summed live when {@link #summary()} runs. */
    public void register(CacheStats s) {
        if (s != null) {
            registered.add(s);
        }
    }

    /** Adds time spent constructing a producer (file resolve + metadata read) for one segment. */
    public void addProducerSetupNanos(long nanos) {
        producerSetupNanos.add(nanos);
    }

    /** True when nothing was recorded (used to suppress an empty summary). */
    public boolean isEmpty() {
        return registered.isEmpty() && producerSetupNanos.sum() == 0;
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
        long lookups = hits + misses;
        double hitRate = lookups == 0 ? 0.0 : (double) hits / lookups * 100.0;
        double decodeMs = pageDecodeNanos / 1_000_000.0;
        double indexLoadMs = pageIndexLoadNanos / 1_000_000.0;
        double slowReadMs = slowReadNanos / 1_000_000.0;
        double setupMs = producerSetupNanos.sum() / 1_000_000.0;
        double totalParquetMs = decodeMs + indexLoadMs + slowReadMs + setupMs;
        return String.format(
            Locale.ROOT,
            "segments/columns=%d | L1/2 cache: hits=%d misses=%d (hitRate=%.2f%%) | "
                + "L3 jumpTableLookups=%d | L4 allNullSkips=%d | "
                + "FFM: pageDecodes=%d slowValueReads=%d slowRepeatedReads=%d | "
                + "timings(ms): pageDecode=%.1f pageIndexLoad=%.1f slowRead=%.1f producerSetup=%.1f totalParquetTime=%.1f | "
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
            decodeMs,
            indexLoadMs,
            slowReadMs,
            setupMs,
            totalParquetMs,
            present,
            absent
        );
    }
}
