/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level registry of segment-lifetime {@link ParquetDocValuesProducer}s.
 *
 * <p>Search-time wrappers ({@link ParquetDocValuesLeafReader}) are request-scoped, but Lucene and
 * OpenSearch caches — fielddata, global ordinals — legitimately retain them beyond the request
 * and call doc-values accessors later (composite aggregations resolve global ordinals through a
 * reader-keyed cache, for example). Those late calls cannot be served by the request's producer,
 * which closes with its search. They are routed here instead: one shared producer per segment
 * core, created on first use and closed by the segment core's closed-listener.
 *
 * <p>Shared producers are accessed concurrently (any query may race a cached consumer), which the
 * producer supports for the accessors reachable from caches: sorted/sorted-set iterators use
 * dedicated per-instance readers with instance-scoped buffer slots, and the producer's lazy maps
 * are thread-safe. Native cursors opened on this path are reclaimed by {@code Cleaner} when their
 * iterators become unreachable, with the producer close (segment close) as the final backstop.
 */
final class SharedProducerRegistry {

    private static final Map<Object, ParquetDocValuesProducer> PRODUCERS = new ConcurrentHashMap<>();

    private SharedProducerRegistry() {}

    /**
     * The segment-lifetime producer for the segment identified by {@code coreHelper}, creating it
     * on first use. Returns {@code null} when the segment exposes no core cache helper (no safe
     * lifecycle to attach to — callers must fail rather than leak).
     */
    static ParquetDocValuesProducer get(
        IndexReader.CacheHelper coreHelper,
        SegmentReadState segmentReadState,
        MapperService mapperService
    ) throws IOException {
        if (coreHelper == null) {
            return null;
        }
        Object key = coreHelper.getKey();
        ParquetDocValuesProducer existing = PRODUCERS.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (PRODUCERS) {
            existing = PRODUCERS.get(key);
            if (existing != null) {
                return existing;
            }
            ParquetDocValuesProducer created = new ParquetDocValuesProducer(segmentReadState, mapperService);
            PRODUCERS.put(key, created);
            coreHelper.addClosedListener(k -> {
                ParquetDocValuesProducer removed = PRODUCERS.remove(k);
                if (removed != null) {
                    try {
                        removed.close();
                    } catch (IOException e) {
                        // Segment is going away; nothing actionable.
                    }
                }
            });
            return created;
        }
    }

    /** Number of live shared producers (tests / diagnostics). */
    static int size() {
        return PRODUCERS.size();
    }
}
