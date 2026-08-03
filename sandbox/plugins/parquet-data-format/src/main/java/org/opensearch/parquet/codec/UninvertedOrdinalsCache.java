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
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level cache of {@link UninvertedOrdinals}, keyed by (segment core key, field).
 *
 * <p>Builds are serialized node-wide (one postings sweep at a time — the transient packed buffer
 * and the sweep's CPU never stack). Entries are evicted (and their mapped files closed) by the
 * segment core's closed-listener; the on-disk artifact is keyed by the segment's stable id and
 * survives restarts, so a re-opened segment maps the existing file instead of rebuilding.
 */
public final class UninvertedOrdinalsCache {

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        UninvertedOrdinalsCache.class
    );
    /** Marks a (segment, field) whose ordinals failed coverage verification — do not retry. */
    private static final Map<Object, java.util.Set<String>> INELIGIBLE = new ConcurrentHashMap<>();

    private static final Map<Object, Map<String, UninvertedOrdinals>> CACHE = new ConcurrentHashMap<>();
    private static final Object BUILD_LOCK = new Object();
    /** Default under java.io.tmpdir (unit tests); the plugin points this at the node data path. */
    private static volatile Path ORDS_DIR = Path.of(System.getProperty("java.io.tmpdir"), "opensearch-parquet-ords");

    private static volatile boolean shuttingDown = false;

    /**
     * Called once at plugin init: ord files live with the node's data, not in tmp. Also performs
     * crash hygiene: interrupted builds' {@code .tmp} files are deleted, and the directory is
     * trimmed to the disk budget oldest-first (nothing is pinned yet at startup).
     */
    public static void setOrdsDir(Path dir) {
        ORDS_DIR = dir;
        shuttingDown = false;
        cleanupAtStartup(dir);
    }

    /** Called at plugin close: aborts in-flight builds so node shutdown is not held hostage. */
    public static void shutdown() {
        shuttingDown = true;
    }

    /**
     * Builds ordinals, retrying ONCE after deleting the on-disk file when verification fails on
     * a pre-existing file: a file left by a crashed or killed process may be stale for reasons a
     * rebuild fixes (segment data moved on after an unclean stop). Only a failure on a FRESH
     * build is genuine (unindexed stored values) and latches the field ineligible.
     */
    private static UninvertedOrdinals buildWithRetry(String fileKey, Terms terms, int maxDoc, long expectedNonNullDocs)
        throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        boolean preExisting = java.nio.file.Files.exists(ORDS_DIR.resolve(fileName));
        try {
            return UninvertedOrdinals.build(
                ORDS_DIR,
                fileKey,
                terms,
                maxDoc,
                expectedNonNullDocs,
                () -> shuttingDown || Thread.currentThread().isInterrupted()
            );
        } catch (IllegalStateException e) {
            if (preExisting == false) {
                throw e;
            }
            LOGGER.warn("ord file [{}] failed verification ({}); deleting and rebuilding once", fileName, e.getMessage());
            java.nio.file.Files.deleteIfExists(ORDS_DIR.resolve(fileName));
            return UninvertedOrdinals.build(
                ORDS_DIR,
                fileKey,
                terms,
                maxDoc,
                expectedNonNullDocs,
                () -> shuttingDown || Thread.currentThread().isInterrupted()
            );
        }
    }

    private static void cleanupAtStartup(Path dir) {
        long budget = ParquetDocValuesProducer.uninvertMaxDiskBytes();
        long used = 0;
        List<Path> files = new java.util.ArrayList<>();
        try (java.util.stream.Stream<Path> listing = java.nio.file.Files.list(dir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                if (file.getFileName().toString().endsWith(".tmp")) {
                    java.nio.file.Files.deleteIfExists(file); // interrupted build leftovers
                } else {
                    used += java.nio.file.Files.size(file);
                    files.add(file);
                }
            }
        } catch (java.nio.file.NoSuchFileException e) {
            return;
        } catch (IOException e) {
            LOGGER.warn("ords directory startup cleanup failed for [{}]: {}", dir, e.getMessage());
            return;
        }
        if (used <= budget) {
            return;
        }
        files.sort(java.util.Comparator.comparingLong(f -> {
            try {
                return java.nio.file.Files.getLastModifiedTime(f).toMillis();
            } catch (IOException e) {
                return Long.MAX_VALUE;
            }
        }));
        for (Path victim : files) {
            if (used <= budget) {
                break;
            }
            try {
                long size = java.nio.file.Files.size(victim);
                java.nio.file.Files.deleteIfExists(victim);
                used -= size;
                LOGGER.info("reclaimed ord file [{}] at startup (over budget)", victim.getFileName());
            } catch (IOException e) {
                // skip
            }
        }
    }

    private UninvertedOrdinalsCache() {}

    /** Transient refusal: budget can be raised or freed, so it is never latched as INELIGIBLE. */
    private static final class BudgetExceededException extends IllegalStateException {
        BudgetExceededException(String message) {
            super(message);
        }
    }

    /**
     * Keeps the ords directory within {@code parquet.docvalues.uninvert.max_disk_bytes}. Files
     * belonging to live cache entries are pinned; everything else (closed segments, merged-away
     * segments, other fields' leftovers) is reclaimable oldest-mtime-first. If the new file
     * still does not fit after reclaim, the build is refused — bounded disk, loud fallback.
     */
    private static void enforceDiskBudget(String fileKey, Terms terms, int maxDoc) throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        if (java.nio.file.Files.exists(ORDS_DIR.resolve(fileName))) {
            return; // reusing an existing file adds no disk
        }
        long budget = ParquetDocValuesProducer.uninvertMaxDiskBytes();
        long termCount = Math.max(terms.size(), 0);
        long bits = org.apache.lucene.util.packed.DirectWriter.bitsRequired(termCount + 1);
        long estimate = (maxDoc * bits + 7) / 8 + 1024;
        java.util.Set<String> pinned = new java.util.HashSet<>();
        for (Map<String, UninvertedOrdinals> perSegment : CACHE.values()) {
            for (UninvertedOrdinals live : perSegment.values()) {
                pinned.add(live.fileName());
            }
        }
        long used = 0;
        List<Path> reclaimable = new java.util.ArrayList<>();
        try (java.util.stream.Stream<Path> listing = java.nio.file.Files.list(ORDS_DIR)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                used += java.nio.file.Files.size(file);
                if (pinned.contains(file.getFileName().toString()) == false) {
                    reclaimable.add(file);
                }
            }
        } catch (java.nio.file.NoSuchFileException e) {
            return; // directory not created yet: nothing used
        }
        if (used + estimate <= budget) {
            return;
        }
        reclaimable.sort(java.util.Comparator.comparingLong(f -> {
            try {
                return java.nio.file.Files.getLastModifiedTime(f).toMillis();
            } catch (IOException e) {
                return Long.MAX_VALUE;
            }
        }));
        for (Path victim : reclaimable) {
            if (used + estimate <= budget) {
                break;
            }
            try {
                long size = java.nio.file.Files.size(victim);
                java.nio.file.Files.deleteIfExists(victim);
                used -= size;
            } catch (IOException e) {
                // still referenced by an mmap on some platforms or raced; skip
            }
        }
        if (used + estimate > budget) {
            throw new BudgetExceededException(
                "uninverted ordinals disk budget exceeded: "
                    + used
                    + "B used + "
                    + estimate
                    + "B needed > "
                    + budget
                    + "B (parquet.docvalues.uninvert.max_disk_bytes)"
            );
        }
    }

    /**
     * The uninverted ordinals for {@code field}, building (or re-mapping) on first use.
     * Returns {@code null} when the segment lacks a core cache identity or a terms index.
     */
    static UninvertedOrdinals get(LeafReader leaf, SegmentInfo segmentInfo, String field, long expectedNonNullDocs) throws IOException {
        IndexReader.CacheHelper helper = leaf.getCoreCacheHelper();
        Terms terms = leaf.terms(field);
        if (helper == null || terms == null) {
            return null;
        }
        Object key = helper.getKey();
        java.util.Set<String> ineligible = INELIGIBLE.get(key);
        if (ineligible != null && ineligible.contains(field)) {
            return null;
        }
        Map<String, UninvertedOrdinals> perSegment = CACHE.computeIfAbsent(key, k -> {
            helper.addClosedListener(closedKey -> {
                INELIGIBLE.remove(closedKey);
                Map<String, UninvertedOrdinals> removed = CACHE.remove(closedKey);
                if (removed != null) {
                    for (UninvertedOrdinals ords : removed.values()) {
                        try {
                            ords.close();
                        } catch (IOException e) {
                            // Segment is going away; nothing actionable.
                        }
                    }
                }
            });
            return new ConcurrentHashMap<>();
        });
        UninvertedOrdinals cached = perSegment.get(field);
        if (cached != null) {
            return cached;
        }
        synchronized (BUILD_LOCK) {
            cached = perSegment.get(field);
            if (cached != null) {
                return cached;
            }
            String fileKey = StringHelper.idToString(segmentInfo.getId()) + "-" + field;
            try {
                enforceDiskBudget(fileKey, terms, leaf.maxDoc());
                UninvertedOrdinals built = buildWithRetry(fileKey, terms, leaf.maxDoc(), expectedNonNullDocs);
                perSegment.put(field, built);
                return built;
            } catch (BudgetExceededException e) {
                // Disk budget refusals are transient (budget can be raised, files can be
                // reclaimed): log and fall back WITHOUT latching, so the next query retries.
                LOGGER.warn("refusing uninverted ordinals for field [{}]: {}", field, e.getMessage());
                return null;
            } catch (IllegalStateException e) {
                // Coverage verification failed: postings do not represent every stored value
                // (ignore_above truncation and the like). Serving them would silently
                // undercount. Remember the refusal and let global-ordinal consumers hit the
                // streaming iterator's loud fail-fast toward execution_hint:map.
                LOGGER.warn("refusing uninverted ordinals for field [{}]: {}", field, e.getMessage());
                INELIGIBLE.computeIfAbsent(key, k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add(field);
                return null;
            }
        }
    }
}
