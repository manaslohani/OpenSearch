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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.cache.QueryParquetStats;
import org.opensearch.parquet.codec.iter.ParquetBinaryDocValues;
import org.opensearch.parquet.codec.iter.ParquetNumericDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedNumericDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedSetDocValues;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Read-only {@link DocValuesProducer} that materializes per-document values from a Parquet
 * file through Lucene's DocValues iterator API, for <b>flat indices only</b>.
 *
 * <h2>Row ID = Doc ID invariant (precondition)</h2>
 * This producer relies on the composite indexing engine's guarantee that Lucene document
 * {@code N} corresponds to Parquet row position {@code N} within the same segment's Parquet
 * file, with one Lucene document per Parquet row and no translation table. The invariant is
 * verified at construction by asserting the Parquet file's {@code numRows} equals the segment's
 * {@code maxDoc}; a mismatch throws {@link IllegalStateException}. Nested documents are out of
 * scope.
 *
 * <h2>Laziness and lifecycle</h2>
 * The constructor resolves the Parquet file and checks the invariant but opens no native
 * column-reader handle. Each {@code getX(field)} lazily opens (and caches) a
 * {@link ParquetColumnReader}; {@code getSorted}/{@code getSortedSet} additionally build (and
 * cache) an {@link OrdinalTable} on first access. {@link #close()} releases every reader,
 * ordinal table, and the shared {@link BufferPool}, and is idempotent.
 *
 * <h2>Thread-safety under intra-segment concurrent search</h2>
 * One producer serves one segment, but under intra-segment concurrent search that segment's
 * {@code [0,maxDoc)} range is split into partitions run on different threads, each calling
 * {@code getNumeric}/{@code getBinary} on this same producer. The numeric/binary iterators hold a
 * live reference to a mutable {@link ParquetColumnReader} (its {@code PageCache} cursor is
 * reassigned on every page miss), so a single shared reader would be raced. To keep those readers
 * single-threaded as they require, this producer hands each thread its OWN {@link ParquetColumnReader}
 * (own native handle, own {@link BufferPool}/arena, own page cache) via a {@link ThreadLocal} map;
 * a thread reusing its reader across the contiguous doc range keeps its page cache warm. All readers
 * and pools created by any thread are tracked in concurrent registries so {@link #close()} (which may
 * run on a different thread) reaps every one.
 *
 * <p>The sorted/keyword ({@link OrdinalTable}) path is different: the table is fully materialized at
 * build time and read-only thereafter, so it is safely shared across threads once built. Its
 * (one-time, per-field) build is serialized with a lock and runs on a dedicated build-only reader.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesProducer.class);

    private final Path parquetFile;
    private final MapperService mapperService;
    private final int maxDoc;
    private final long parquetRowCount;

    /**
     * Registries of every reader/pool created on any thread, so {@link #close()} frees them all
     * regardless of which thread allocated them. {@link BufferPool} uses a shared arena, so
     * cross-thread close is legal. Declared before the {@link ThreadLocal}s below because the pool
     * supplier registers into {@link #allBufferPools} at creation.
     */
    private final Queue<ParquetColumnReader> allColumnReaders = new ConcurrentLinkedQueue<>();
    private final Queue<BufferPool> allBufferPools = new ConcurrentLinkedQueue<>();

    /**
     * Per-thread column readers for the numeric/binary hot path. Each partition thread gets its own
     * field→reader map with its own {@link BufferPool}, so the mutable page-cursor state is never
     * shared across threads. Populated lazily on the calling (partition) thread.
     */
    private final ThreadLocal<Map<String, ParquetColumnReader>> threadColumnReaders = ThreadLocal.withInitial(HashMap::new);
    // Each thread's pool is registered for close the moment it is created (in the initial supplier),
    // so it is never leaked even if the subsequent reader open fails.
    private final ThreadLocal<BufferPool> threadBufferPool = ThreadLocal.withInitial(() -> {
        BufferPool pool = new BufferPool();
        allBufferPools.add(pool);
        return pool;
    });

    /** Ordinal tables are build-once/read-only; shared across threads. Guarded by {@link #ordinalLock}. */
    private final Map<String, OrdinalTable> ordinalTables = new ConcurrentHashMap<>();
    private final Object ordinalLock = new Object();

    /** Nanoseconds spent in producer setup (file resolve + metadata read); flushed when query stats are attached. */
    private final long setupNanos;
    /** Optional per-query accumulator; propagated to each column reader so its stats roll up at close. */
    private QueryParquetStats queryStats;

    private boolean closed;

    /**
     * Constructs the producer for {@code state}'s segment.
     *
     * @param mapperService resolves OpenSearch mapping types for DV-type validation (may be
     *                      {@code null} only in low-level tests that bypass type validation)
     * @throws IOException if the Parquet file for the segment cannot be resolved (Req 9.3)
     * @throws IllegalStateException if the Row ID = Doc ID invariant is violated (Req 12.3)
     */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService) throws IOException {
        long setupStart = System.nanoTime();
        this.mapperService = mapperService;
        this.maxDoc = state.segmentInfo.maxDoc();

        Path resolved = ParquetSegmentLayout.resolve(state);
        if (resolved == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "no Parquet file found for segment '%s' (maxDoc=%d); cannot serve Parquet doc values",
                    state.segmentInfo.name,
                    maxDoc
                )
            );
        }
        this.parquetFile = resolved;

        ParquetFileMetadata metadata = RustBridge.getFileMetadata(parquetFile.toString());
        this.parquetRowCount = metadata.numRows();
        if (parquetRowCount != maxDoc) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Parquet/Lucene row-count mismatch for segment '%s': Lucene maxDoc=%d but Parquet numRows=%d (file=%s). "
                        + "The resolved Parquet file must contain exactly the segment's rows; docId→row translation is handled "
                        + "separately via __row_id__.",
                    state.segmentInfo.name,
                    maxDoc,
                    parquetRowCount,
                    parquetFile
                )
            );
        }
        this.setupNanos = System.nanoTime() - setupStart;
    }

    /**
     * Attaches the per-query accumulator. The producer's setup time is folded in immediately, and
     * the accumulator is propagated to every column reader (existing and future) so each reader's
     * stats roll up into the query total when it closes.
     */
    public void setQueryStats(QueryParquetStats queryStats) {
        this.queryStats = queryStats;
        if (queryStats != null) {
            queryStats.addProducerSetupNanos(setupNanos);
            // Readers are opened lazily per thread; each picks up queryStats at open (see readerFor).
            // Any already-open readers (e.g. an ordinal-table build reader) are updated here too.
            for (ParquetColumnReader reader : allColumnReaders) {
                reader.setQueryStats(queryStats);
            }
        }
    }

    // ── DocValuesProducer API ──

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.NUMERIC);
        ParquetColumnReader reader = readerFor(field, false);
        return new ParquetNumericDocValues(reader, maxDoc);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_NUMERIC);
        ParquetColumnReader reader = readerFor(field, true);
        return new ParquetSortedNumericDocValues(reader, maxDoc);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.BINARY);
        ParquetColumnReader reader = readerFor(field, false);
        return new ParquetBinaryDocValues(reader, maxDoc);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED);
        OrdinalTable table = ordinalTableFor(field, false);
        return new ParquetSortedDocValues(table, maxDoc);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_SET);
        OrdinalTable table = ordinalTableFor(field, true);
        return new ParquetSortedSetDocValues(table, maxDoc);
    }

    /** No skip lists for Parquet-backed doc values. */
    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return null;
    }

    /**
     * Verifies the underlying Parquet file is accessible and its metadata is consistent: the
     * file opens, {@code numRows} matches the value cached at construction, and the metadata
     * round-trip (which includes the writer-side CRC) succeeds.
     */
    @Override
    public void checkIntegrity() throws IOException {
        ParquetFileMetadata metadata = RustBridge.getFileMetadata(parquetFile.toString());
        if (metadata.numRows() != parquetRowCount) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "checkIntegrity: Parquet numRows changed for %s: expected %d, found %d",
                    parquetFile,
                    parquetRowCount,
                    metadata.numRows()
                )
            );
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        // Publish closed before draining so any late reader-open on a straggler thread is a no-op
        // via ensureOpen(). Lucene completes all slice collection before closing the reader, so in
        // practice no partition thread is still active here.
        closed = true;
        // Aggregate cache-effectiveness summary across every column touched by this segment's
        // producer, across all partition threads. Per-column detail is logged by each reader on close.
        if (allColumnReaders.isEmpty() == false && logger.isDebugEnabled()) {
            long hits = 0, misses = 0, decodes = 0, allNullSkips = 0;
            int columns = 0;
            for (ParquetColumnReader reader : allColumnReaders) {
                columns++;
                hits += reader.stats().pageCacheHits();
                misses += reader.stats().pageCacheMisses();
                decodes += reader.stats().pageDecodes();
                allNullSkips += reader.stats().allNullPageSkips();
            }
            long lookups = hits + misses;
            double hitRate = lookups == 0 ? 0.0 : (double) hits / lookups * 100.0;
            logger.debug(
                "[PARQUET_DV_CACHE_STATS] segment summary: file={} readers={} | L1/2 hits={} misses={} (hitRate={}%) "
                    + "| L4 allNullSkips={} | FFM pageDecodes={}",
                parquetFile,
                columns,
                hits,
                misses,
                String.format(Locale.ROOT, "%.2f", hitRate),
                allNullSkips,
                decodes
            );
        }
        IOException first = null;
        // Close every reader opened on any thread, then every pool. Pools use a shared arena, so
        // freeing them here (possibly off the allocating thread) is legal.
        for (ParquetColumnReader reader : allColumnReaders) {
            try {
                reader.close();
            } catch (IOException | RuntimeException e) {
                if (first == null && e instanceof IOException io) {
                    first = io;
                }
                // Suppress per-reader errors so every reader gets a chance to close.
            }
        }
        for (BufferPool pool : allBufferPools) {
            try {
                pool.close();
            } catch (RuntimeException e) {
                // Suppress so every pool gets a chance to close.
            }
        }
        allColumnReaders.clear();
        allBufferPools.clear();
        threadColumnReaders.remove();
        threadBufferPool.remove();
        ordinalTables.clear();
        if (first != null) {
            throw first;
        }
    }

    // ── internals ──

    /** Validates the field's mapping type supports the requested DV type, when a mapper is present. */
    private void validate(FieldInfo field, DocValuesType requested) {
        if (mapperService == null) {
            return; // low-level tests may bypass mapping validation
        }
        FieldTypeMapping.validate(field.getName(), mappingType(field), requested);
    }

    private String mappingType(FieldInfo field) {
        MappedFieldType mft = mapperService.fieldType(field.getName());
        if (mft == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "field '%s' has no mapping; cannot resolve Parquet column type", field.getName())
            );
        }
        return mft.typeName();
    }

    /** Resolves the Parquet physical type for a field from its mapping (or infers for tests). */
    private ParquetPhysicalType physicalType(FieldInfo field) {
        if (mapperService != null) {
            return FieldTypeMapping.forType(mappingType(field)).physical();
        }
        // Without a mapper, infer from the Lucene DV type recorded on the field.
        return switch (field.getDocValuesType()) {
            case BINARY, SORTED, SORTED_SET -> ParquetPhysicalType.BYTE_ARRAY;
            default -> ParquetPhysicalType.INT64;
        };
    }

    /**
     * Returns the calling thread's own {@link ParquetColumnReader} for {@code field}, opening one
     * (with the thread's own {@link BufferPool}) on first use. Each thread's reader is independent —
     * its own native handle and mutable page cache — so concurrent intra-segment partitions never
     * race. Every opened reader/pool is registered for close.
     */
    private ParquetColumnReader readerFor(FieldInfo field, boolean repeated) throws IOException {
        Map<String, ParquetColumnReader> readers = threadColumnReaders.get();
        ParquetColumnReader reader = readers.get(field.getName());
        if (reader == null) {
            BufferPool pool = threadBufferPool.get();
            reader = ParquetColumnReader.open(parquetFile, field.getName(), physicalType(field), repeated, pool);
            reader.setQueryStats(queryStats);
            readers.put(field.getName(), reader);
            allColumnReaders.add(reader);
        }
        return reader;
    }

    /**
     * Returns the shared, immutable {@link OrdinalTable} for {@code field}, building it once. The
     * table is fully materialized (no live reader) and read-only, so it is safe to share across
     * partition threads. The build is serialized and runs on a dedicated reader (with its own pool)
     * that is closed immediately after, so it never interferes with the per-thread hot-path readers.
     */
    private OrdinalTable ordinalTableFor(FieldInfo field, boolean multiValued) throws IOException {
        OrdinalTable table = ordinalTables.get(field.getName());
        if (table != null) {
            return table;
        }
        synchronized (ordinalLock) {
            table = ordinalTables.get(field.getName());
            if (table == null) {
                try (BufferPool buildPool = new BufferPool()) {
                    ParquetColumnReader reader = ParquetColumnReader.open(
                        parquetFile,
                        field.getName(),
                        physicalType(field),
                        multiValued,
                        buildPool
                    );
                    reader.setQueryStats(queryStats);
                    try {
                        table = multiValued
                            ? OrdinalTable.buildMultiValued(reader, maxDoc)
                            : OrdinalTable.buildSingleValued(reader, maxDoc);
                    } finally {
                        reader.close();
                    }
                }
                ordinalTables.put(field.getName(), table);
            }
        }
        return table;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ParquetDocValuesProducer is closed");
        }
    }
}
