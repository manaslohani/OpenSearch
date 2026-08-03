/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-level cache of {@link TermDictionary} instances, keyed by (segment core key, field).
 *
 * <p>Segments are immutable, so a loaded dictionary is valid for the segment's lifetime and is
 * released via the core's closed-listener when the segment goes away. Total heap is bounded:
 * when the budget is exhausted, dictionaries are still served but not cached (each producer
 * pays the O(distinct) load), which degrades latency, never correctness or memory.
 */
public final class TermDictionaryCache {

    /** Sentinel for "checked and not eligible" so ineligible fields are not re-probed. */
    private static final TermDictionary INELIGIBLE = TermDictionary.sentinel();

    private static final Map<Object, Map<String, TermDictionary>> CACHE = new ConcurrentHashMap<>();
    private static final AtomicLong CACHED_BYTES = new AtomicLong();

    private TermDictionaryCache() {}

    /**
     * The dictionary for {@code field} in {@code leaf}'s segment, or {@code null} when the
     * field is above the term budget (or has no usable terms index).
     */
    public static TermDictionary get(LeafReader leaf, String field, int maxTerms, long maxCacheBytes) throws IOException {
        IndexReader.CacheHelper helper = leaf.getCoreCacheHelper();
        if (helper == null) {
            Terms terms = leaf.terms(field);
            return TermDictionary.load(terms, maxTerms);
        }
        Object key = helper.getKey();
        Map<String, TermDictionary> perSegment = CACHE.get(key);
        if (perSegment == null) {
            perSegment = new ConcurrentHashMap<>();
            Map<String, TermDictionary> existing = CACHE.putIfAbsent(key, perSegment);
            if (existing != null) {
                perSegment = existing;
            } else {
                helper.addClosedListener(k -> {
                    Map<String, TermDictionary> removed = CACHE.remove(k);
                    if (removed != null) {
                        long freed = removed.values().stream().filter(d -> d != INELIGIBLE).mapToLong(TermDictionary::sizeInBytes).sum();
                        CACHED_BYTES.addAndGet(-freed);
                    }
                });
            }
        }
        TermDictionary cached = perSegment.get(field);
        if (cached != null) {
            return cached == INELIGIBLE ? null : cached;
        }
        TermDictionary loaded = TermDictionary.load(leaf.terms(field), maxTerms);
        if (loaded == null) {
            perSegment.put(field, INELIGIBLE);
            return null;
        }
        if (CACHED_BYTES.addAndGet(loaded.sizeInBytes()) <= maxCacheBytes) {
            perSegment.put(field, loaded);
        } else {
            // Over budget: serve uncached; the producer keeps its own reference for the query.
            CACHED_BYTES.addAndGet(-loaded.sizeInBytes());
        }
        return loaded;
    }

    /** Currently cached bytes (tests / diagnostics). */
    public static long cachedBytes() {
        return CACHED_BYTES.get();
    }
}
