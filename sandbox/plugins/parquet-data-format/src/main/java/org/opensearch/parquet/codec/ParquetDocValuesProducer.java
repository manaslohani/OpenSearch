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
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.bridge.DataFusionColumnReader;
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
 * The constructor resolves the Parquet file and checks the invariant but opens no column-reader
 * handle. Each {@code getX(field)} lazily opens and caches the reader selected by the configured
 * decode path; {@code getSorted}/{@code getSortedSet} serve streaming
 * per-document ordinals with no segment-wide ordinal structure (see ParquetSortedSetDocValues). {@link #close()} releases every reader, ordinal table,
 * and the shared {@link BufferPool}, and is idempotent.
 *
 * <p>Not thread-safe: one producer serves one segment on one query thread.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesProducer.class);
    private static volatile boolean useDataFusionDecodePath;
    private static volatile int dataFusionInitialBatchSize = 32;
    private static volatile boolean dataFusionDiagnostics;
    private static volatile int dictionaryMaxTerms = 65536;
    private static volatile long dictionaryCacheBytes = 64 * 1024 * 1024;
    private static volatile long uninvertMaxDiskBytes = 2L * 1024 * 1024 * 1024;

    /** Updates the node-wide DocValues decode path. */
    public static void setDecodePath(String decodePath) {
        useDataFusionDecodePath = ParquetSettings.DECODE_PATH_DATAFUSION.equals(decodePath);
    }

    /** Updates the cardinality budget for dictionary-rank keyword ordinals. */
    public static void setDictionaryMaxTerms(int maxTerms) {
        dictionaryMaxTerms = maxTerms;
    }

    /** Updates the node-wide heap budget for cached term dictionaries. */
    public static void setDictionaryCacheBytes(long bytes) {
        dictionaryCacheBytes = bytes;
    }

    static int dictionaryMaxTerms() {
        return dictionaryMaxTerms;
    }

    static long dictionaryCacheBytes() {
        return dictionaryCacheBytes;
    }

    public static void setUninvertMaxDiskBytes(long bytes) {
        uninvertMaxDiskBytes = bytes;
    }

    static long uninvertMaxDiskBytes() {
        return uninvertMaxDiskBytes;
    }

    /** Updates the starting window used by newly opened DataFusion cursors. */
    public static void setInitialBatchSize(int initialBatchSize) {
        dataFusionInitialBatchSize = initialBatchSize;
    }

    /** Starts or snapshots a process-wide DataFusion cursor diagnostics window. */
    public static synchronized void setDiagnostics(boolean diagnostics) {
        if (diagnostics == dataFusionDiagnostics) {
            return;
        }
        if (diagnostics) {
            RustBridge.dfDiagnosticsReset();
        } else {
            RustBridge.DataFusionDocValuesStats stats = RustBridge.dfDiagnosticsSnapshot();
            double pageRowsAverage = stats.pageSamples() == 0 ? 0.0 : (double) stats.pageRowsTotal() / stats.pageSamples();
            logger.info(
                "[df_docvalues_stats] initial_batch={} opens={} batches={} sequential={} sparse={} decoded_rows={} skipped_rows={} "
                    + "overflow_probes={} range_reads={} range_bytes={} io_ms={} page_samples={} page_rows_avg={} page_rows_min={} "
                    + "page_rows_max={} live_cursors={}",
                dataFusionInitialBatchSize,
                stats.cursorOpens(),
                stats.batchCalls(),
                stats.sequentialBatches(),
                stats.sparseBatches(),
                stats.decodedRows(),
                stats.skippedRows(),
                stats.overflowProbes(),
                stats.rangeReads(),
                stats.rangeBytes(),
                stats.ioNanos() / 1_000_000.0,
                stats.pageSamples(),
                pageRowsAverage,
                stats.pageRowsMin(),
                stats.pageRowsMax(),
                stats.liveCursors()
            );
        }
        dataFusionDiagnostics = diagnostics;
    }

    private final Path parquetFile;
    private final MapperService mapperService;
    private final int maxDoc;
    private final long parquetRowCount;

    private final BufferPool bufferPool = new BufferPool();
    private final Map<String, ParquetColumnReader> columnReaders = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, DataFusionColumnReader> dataFusionColumnReaders = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.List<java.io.Closeable> dedicatedReaders = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

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
    }

    /**
     * Attaches the per-query accumulator. The accumulator is propagated to every column reader
     * (existing and future) so each reader's stats roll up into the query total when it closes.
     */
    public void setQueryStats(QueryParquetStats queryStats) {
        this.queryStats = queryStats;
        if (queryStats != null) {
            for (ParquetColumnReader reader : columnReaders.values()) {
                reader.setQueryStats(queryStats);
            }
        }
    }

    // ── DocValuesProducer API ──

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.NUMERIC);
        if (useDataFusionDecodePath) {
            return new ParquetNumericDocValues(dataFusionReaderFor(field, false), maxDoc);
        }
        ParquetColumnReader reader = readerFor(field, false);
        return new ParquetNumericDocValues(reader, maxDoc);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_NUMERIC);
        if (useDataFusionDecodePath) {
            return new ParquetSortedNumericDocValues(dataFusionReaderFor(field, true), maxDoc);
        }
        ParquetColumnReader reader = readerFor(field, true);
        return new ParquetSortedNumericDocValues(reader, maxDoc);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.BINARY);
        if (useDataFusionDecodePath) {
            return new ParquetBinaryDocValues(dataFusionReaderFor(field, false), maxDoc);
        }
        ParquetColumnReader reader = readerFor(field, false);
        return new ParquetBinaryDocValues(reader, maxDoc);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED);
        return new ParquetSortedDocValues(binaryReaderFor(field, false), maxDoc);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_SET);
        // Convention (mirrors the ordinal-table era and the leaf reader's routing): SORTED_SET
        // reaches this producer only for genuinely repeated columns; single-valued keywords are
        // served through getSorted and wrapped with DocValues.singleton by the leaf reader.
        return new ParquetSortedSetDocValues(binaryReaderFor(field, true), true, maxDoc);
    }

    /**
     * Serves a {@link DocValuesSkipper} backed by the column's Parquet ColumnIndex (per-page
     * min/max/null-count), letting Lucene's range machinery skip whole pages whose stats
     * exclude the query range — no decode, no FFM crossing for skipped pages.
     *
     * <p>Integer-shaped columns only (INT32/INT64/BOOL physical): their raw-bits order is
     * numeric order. Float/double doc values are IEEE-754 raw bits whose order diverges from
     * numeric order for negative values, so page min/max computed on bits would be wrong for
     * them; they get no skipper. BYTE_ARRAY min/max is not exchanged as i64 at all.
     */
    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        ensureOpen();
        ParquetPhysicalType phys = physicalType(field);
        if (phys != ParquetPhysicalType.INT32 && phys != ParquetPhysicalType.INT64 && phys != ParquetPhysicalType.BOOL) {
            return null;
        }
        if (field.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
            // Repeated values may span Parquet pages, so OffsetIndex page rows do not
            // define independent Lucene document ranges. Do not expose unsafe stats.
            return null;
        }
        if (useDataFusionDecodePath) {
            return new ParquetDocValuesSkipper(dataFusionReaderFor(field, false).pageIndex(), maxDoc);
        }
        // Match the repeated flag the field's DV accessor will use — readerFor caches by field
        // name, so opening here with a mismatched flag would poison the cache for the accessor.
        boolean repeated = field.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
        ParquetColumnReader reader = readerFor(field, repeated);
        return new ParquetDocValuesSkipper(reader.pageIndex(), maxDoc);
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
        // Cache-effectiveness is summarized once per query on the dedicated stats channel
        // ([PARQUET_DV_QUERY_STATS]); no per-segment detail line here.
        closed = true;
        IOException first = null;
        for (ParquetColumnReader reader : columnReaders.values()) {
            try {
                reader.close();
            } catch (IOException | RuntimeException e) {
                if (first == null && e instanceof IOException io) {
                    first = io;
                }
                // Suppress per-reader errors so every reader gets a chance to close.
            }
        }
        for (DataFusionColumnReader reader : dataFusionColumnReaders.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            } catch (RuntimeException e) {
                // Suppress so every reader gets a chance to close, but keep the failure visible.
                logger.warn("Failed to close DataFusion column reader for [{}]", parquetFile, e);
            }
        }
        for (java.io.Closeable reader : dedicatedReaders) {
            try {
                reader.close();
            } catch (IOException | RuntimeException e) {
                if (first == null && e instanceof IOException io) {
                    first = io;
                }
            }
        }
        dedicatedReaders.clear();
        columnReaders.clear();
        dataFusionColumnReaders.clear();
        bufferPool.close();
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

    private synchronized ParquetColumnReader readerFor(FieldInfo field, boolean repeated) throws IOException {
        ParquetColumnReader reader = columnReaders.get(field.getName());
        if (reader == null) {
            reader = ParquetColumnReader.open(parquetFile, field.getName(), physicalType(field), repeated, bufferPool);
            reader.setQueryStats(queryStats);
            columnReaders.put(field.getName(), reader);
        }
        return reader;
    }


    /**
     * A dedicated (non-shared) binary reader for one streaming sorted iterator. Concurrent
     * segment-search slices each obtain their own DocValues instance; sharing one forward
     * cursor between them turns every access into a resident-page miss as the slices ping-pong
     * the shared PageCache. Dedicated readers keep each slice's scan sequential. Registered for
     * close with the producer.
     */
    /**
     * Number of rows with a non-null value in this column, from the Parquet page index's
     * per-page null counts; {@code -1} when any page lacks the statistic. Used to verify that
     * postings-derived ordinal tables cover every stored value.
     */
    long nonNullRowCount(FieldInfo field) throws IOException {
        org.opensearch.parquet.codec.cache.ColumnPageIndex idx = dataFusionReaderFor(field, false).pageIndex();
        long nonNull = 0;
        for (int page = 0; page < idx.pageCount(); page++) {
            long nulls = idx.nullCountOf(page);
            if (nulls < 0) {
                return -1;
            }
            nonNull += idx.numRowsOf(page) - nulls;
        }
        return nonNull;
    }

    private synchronized BinaryPageReader binaryReaderFor(FieldInfo field, boolean repeated) throws IOException {
        if (useDataFusionDecodePath) {
            DataFusionColumnReader reader = DataFusionColumnReader.open(
                parquetFile,
                field.getName(),
                physicalType(field),
                repeated,
                bufferPool,
                dataFusionInitialBatchSize
            );
            dedicatedReaders.add(reader);
            return reader;
        }
        // codec_native path: keep the shared per-field reader (its iterators tolerate sharing,
        // and its pool slots are not instance-scoped).
        return readerFor(field, repeated);
    }

    private synchronized DataFusionColumnReader dataFusionReaderFor(FieldInfo field, boolean repeated) throws IOException {
        DataFusionColumnReader reader = dataFusionColumnReaders.get(field.getName());
        if (reader == null) {
            reader = DataFusionColumnReader.open(
                parquetFile,
                field.getName(),
                physicalType(field),
                repeated,
                bufferPool,
                dataFusionInitialBatchSize
            );
            dataFusionColumnReaders.put(field.getName(), reader);
        }
        return reader;
    }

    /** Whether {@link #close()} has run (leaf wrappers reroute to the shared producer then). */
    boolean isClosed() {
        return closed;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ParquetDocValuesProducer is closed");
        }
    }
}
