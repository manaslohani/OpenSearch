/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetCodecBridge;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetListColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.BinaryFramingDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetBinaryDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetNumericDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedNumericDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Read-only {@link DocValuesProducer} that serves single-valued numeric doc values from a Parquet
 * file through Lucene's DocValues iterator API.
 *
 * <p>The constructor resolves the backing file, gates once on the stamped format version, and verifies
 * the footer's writer generation equals the segment's {@code writer_generation} attribute, but opens no
 * cursor. It also captures the store those bytes come from: a hot shard's Parquet files are on local
 * disk, while a shard tiered to warm keeps them only in the remote object store, reachable through the
 * native store the engine stamped on the segment.
 *
 * <p>One producer is cached per segment core by {@link ParquetSegmentResourceCache} and shared
 * across requests; it is closed by the core's closed-listener, not per request. Each
 * {@link #getSortedNumeric(FieldInfo, CursorRegistry)} opens its own dedicated
 * {@link ParquetColumnReader}: a native cursor is forward-only, so one shared across concurrent
 * segment-search slices would be driven backwards by one slice while another advances it. A reader
 * per iterator keeps each slice's scan independent, and the cursor's lifetime belongs to the calling
 * request's {@link CursorRegistry} - the accessor API carries no request identity, so the producer
 * cannot own it.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    /** Scales of the long-encoded {@code major.minor.patch} version: {@code major*1_000_000 + minor*1_000 + patch}. */
    private static final long MAJOR_SCALE = 1_000_000L;
    private static final long MINOR_SCALE = 1_000L;

    /** Oldest stamped format version this codec can decode, long-encoded as {@code major*1_000_000 + minor*1_000 + patch}. */
    static final long MIN_SUPPORTED_FORMAT_VERSION = 1 * MAJOR_SCALE; // 1.0.0

    /**
     * Newest stamped format version this codec can decode. A literal, not a reference to the writer
     * constant {@code ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION}: a writer bump must not silently
     * admit an unseen version. {@code ParquetDocValuesProducerTests} asserts the two are equal.
     */
    static final long MAX_SUPPORTED_FORMAT_VERSION = 1 * MAJOR_SCALE; // 1.0.0

    private final Path parquetFile;
    /**
     * Native object store every cursor reads {@link #parquetFile} through, or
     * {@link ParquetColumnReader#LOCAL_STORE} when the file is on local disk. Captured once from the
     * segment's stamp so a cursor opened later in the segment's life cannot disagree with the row count
     * validated here.
     */
    private final long storePointer;
    private final MapperService mapperService;
    /**
     * Index settings the decode-window sizes are resolved from, captured once so every cursor this
     * producer opens agrees. {@link Settings#EMPTY} when there is no mapper service, which only
     * happens in low-level tests.
     */
    private final Settings indexSettings;
    private final int maxDoc;
    private final long parquetRowCount;
    /**
     * Footer {@code opensearch.values_sorted} marker, read once per segment; false keeps the read-side sort.
     */
    private final boolean valuesSorted;

    private volatile boolean closed;

    /**
     * Per-column physical shape (repeated vs scalar), memoized since a segment's shape is fixed once written.
     */
    private final Map<String, Boolean> repeatedColumns = new ConcurrentHashMap<>();

    /**
     * @param mapperService resolves OpenSearch mapping types for DV-type validation (may be
     *                      {@code null} only in low-level tests that bypass type validation)
     * @throws IOException if the backing Parquet file for the segment cannot be resolved, its stamped
     *                     format version is unsupported, or its footer writer generation does not equal
     *                     the segment's {@code writer_generation} attribute
     */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService) throws IOException {
        this.mapperService = mapperService;
        this.indexSettings = mapperService == null ? Settings.EMPTY : mapperService.getIndexSettings().getSettings();
        this.maxDoc = state.segmentInfo.maxDoc();

        ParquetSegmentLayout.ParquetSource resolved = ParquetSegmentLayout.resolve(state);
        if (resolved == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "no Parquet file bound to segment '%s' (maxDoc=%d); cannot serve Parquet doc values",
                    state.segmentInfo.name,
                    maxDoc
                )
            );
        }
        this.parquetFile = resolved.file();
        this.storePointer = resolved.storePointer();

        ParquetCodecBridge.FileMetadata metadata = ParquetCodecBridge.fileMetadata(parquetFile.toString(), storePointer);
        checkFormatVersion(metadata.opensearchFormatVersion(), parquetFile);
        checkWriterGeneration(
            state.segmentInfo.getAttribute(ParquetSegmentLayout.WRITER_GENERATION_ATTRIBUTE),
            metadata.writerGeneration(),
            parquetFile,
            state.segmentInfo.name
        );
        this.parquetRowCount = metadata.numRows();
        this.valuesSorted = metadata.valuesSorted();
    }

    /**
     * Test seam: builds over an already-resolved file, skipping segment resolution, the format-version
     * gate, and the writer-generation cross-check. Never reached in production, where the per-index
     * resource cache constructs the producer from a {@link SegmentReadState}.
     */
    ParquetDocValuesProducer(Path parquetFile, long storePointer, Settings indexSettings, int maxDoc, MapperService mapperService) {
        this.parquetFile = parquetFile;
        this.storePointer = storePointer;
        this.indexSettings = indexSettings;
        this.maxDoc = maxDoc;
        this.mapperService = mapperService;
        this.parquetRowCount = maxDoc;
        // No footer is read on this seam, so the iterator always sorts.
        this.valuesSorted = false;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        // Every field this codec serves is SORTED_NUMERIC (FieldTypeMapping), so per the
        // DocValuesProducer contract this accessor is never invoked for a valid FieldInfo.
        throw unsupported("numeric", field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        // The doc-values accessor API carries no request identity; a cursor opened here would have no
        // request-scoped owner to close it. Callers go through the leaf wrapper, which supplies the
        // request's CursorRegistry via the overload below.
        throw new UnsupportedOperationException(
            "ParquetDocValuesProducer requires a request-scoped cursor registry; call getSortedNumeric(field, cursors)"
        );
    }

    /**
     * Serves {@code field} over a dedicated cursor recorded on {@code cursors}; LIST columns go through
     * {@link ParquetSortedNumericDocValues}, scalar columns through {@code DocValues.singleton}.
     */
    SortedNumericDocValues getSortedNumeric(FieldInfo field, CursorRegistry cursors) throws IOException {
        validate(field, DocValuesType.SORTED_NUMERIC);
        if (isRepeated(field)) {
            // Routing is by physical shape: a pre-promotion scalar segment would fail the native list downcast.
            return new ParquetSortedNumericDocValues(openListCursor(field.getName(), cursors), maxDoc, valuesSorted);
        }
        // Scalar column, including a pre-promotion segment of a multi-valued field.
        return DocValues.singleton(new ParquetNumericDocValues(openCursor(field.getName(), cursors), maxDoc));
    }

    /**
     * Whether {@code field}'s column is physically a Parquet LIST in this segment, read from the file
     * schema rather than the mapping; probed once per column and memoized.
     */
    boolean isRepeated(FieldInfo field) throws IOException {
        Boolean cached = repeatedColumns.get(field.getName());
        if (cached != null) {
            return cached;
        }
        boolean repeated;
        try (ParquetColumnReader probe = ParquetColumnReader.open(parquetFile, field.getName(), indexSettings, storePointer)) {
            repeated = probe.isPhysicallyRepeated();
        }
        repeatedColumns.put(field.getName(), repeated);
        return repeated;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        // Like getSortedNumeric(FieldInfo), the doc-values accessor API carries no request identity;
        // a cursor opened here would have no request-scoped owner to close it. Callers go through the
        // leaf wrapper, which supplies the request's CursorRegistry via the overload below.
        throw new UnsupportedOperationException(
            "ParquetDocValuesProducer requires a request-scoped cursor registry; call getBinary(field, cursors)"
        );
    }

    /**
     * Serves {@code field} as binary doc values over a dedicated forward-only cursor, recorded on
     * {@code cursors} so the calling request closes it when it ends.
     */
    BinaryDocValues getBinary(FieldInfo field, CursorRegistry cursors) throws IOException {
        validate(field, DocValuesType.BINARY);
        return new BinaryFramingDocValues(new ParquetBinaryDocValues(openCursor(field.getName(), cursors), maxDoc));
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        // Keyword and ip are served as SORTED_SET (FieldTypeMapping), so per the DocValuesProducer
        // contract this accessor is never invoked for a valid FieldInfo.
        throw unsupported("sorted", field);
    }

    /**
     * Serves {@code field} as a singleton over a dedicated forward-only binary cursor, which the
     * iterator that receives it closes; callers recover the inner iterator via
     * {@code DocValues.unwrapSingleton}.
     */
    // TODO(multi-value): no repeated read path; the write path emits single values only.
    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_SET);
        // The cursor opens on the first value request, so a leaf whose documents the query never
        // reaches allocates nothing.
        return DocValues.singleton(new ParquetSortedDocValues(() -> openBinaryCursor(field.getName()), maxDoc));
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        ensureOpen();
        if (mapperService == null || ParquetSegmentResourceCache.isIntegerShaped(mappingType(field)) == false) {
            return null;
        }
        try (ParquetColumnReader reader = ParquetColumnReader.open(parquetFile, field.getName(), indexSettings, storePointer)) {
            return new ParquetDocValuesSkipper(reader.pageIndex(), maxDoc);
        }
    }

    /**
     * Verifies the backing Parquet file is still accessible and its row count matches the value
     * cached at construction.
     *
     * <p>This overlay is not a registered {@code DocValuesFormat}, so CheckIndex/merge verification
     * never reach it.
     */
    @Override
    public void checkIntegrity() throws IOException {
        ParquetCodecBridge.FileMetadata metadata = ParquetCodecBridge.fileMetadata(parquetFile.toString(), storePointer);
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

    /**
     * Number of rows with a non-null value in this column, from the Parquet footer's per-row-group
     * column-chunk statistics; {@code -1} when any row group lacks the null count. Used to verify
     * that postings-derived ordinal tables cover every stored value.
     */
    long nonNullRowCount(FieldInfo field) throws IOException {
        return ParquetCodecBridge.columnNonNullCount(parquetFile.toString(), field.getName(), storePointer);
    }

    @Override
    public void close() {
        // Segment-lifetime producer, closed once by the core's closed-listener. It holds no open file
        // handle in this codec (each cursor opens its own), so close only marks the producer done;
        // numeric cursors are closed by the request's CursorRegistry and binary cursors by the
        // iterator that opened them, not here.
        closed = true;
    }

    /**
     * Rejects a file this codec cannot decode: unstamped, older than {@link #MIN_SUPPORTED_FORMAT_VERSION},
     * or newer than {@link #MAX_SUPPORTED_FORMAT_VERSION}, failing on an out-of-range file rather than reading it
     * with assumptions that may not hold.
     */
    static void checkFormatVersion(long formatVersion, Path file) throws IOException {
        if (formatVersion == ParquetCodecBridge.FORMAT_VERSION_UNKNOWN) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s carries no parseable opensearch.format_version; this doc-values codec requires a stamped version in %s",
                    file,
                    supportedRange()
                )
            );
        }
        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION || formatVersion > MAX_SUPPORTED_FORMAT_VERSION) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s has OpenSearch format version %s, outside this doc-values codec's supported range %s",
                    file,
                    describeFormatVersion(formatVersion),
                    supportedRange()
                )
            );
        }
    }

    /**
     * Rejects a file whose stamped writer generation does not identify it as the one written alongside
     * this segment. The footer generation must equal the segment's {@code writer_generation} attribute;
     * both are stamped by the writer for the same batch, so equality identifies the file written
     * alongside this segment. Fails closed: a missing segment attribute, an unstamped or unparseable
     * footer, and a mismatch are all rejected.
     */
    static void checkWriterGeneration(String segmentAttr, long footerGeneration, Path file, String segmentName) throws IOException {
        if (segmentAttr == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "segment %s carries no %s attribute; cannot verify Parquet file %s",
                    segmentName,
                    ParquetSegmentLayout.WRITER_GENERATION_ATTRIBUTE,
                    file
                )
            );
        }
        if (footerGeneration == ParquetCodecBridge.WRITER_GENERATION_UNKNOWN) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s carries no parseable opensearch.writer_generation; cannot verify it was written for segment %s",
                    file,
                    segmentName
                )
            );
        }
        final long segmentGeneration;
        try {
            segmentGeneration = Long.parseLong(segmentAttr);
        } catch (NumberFormatException e) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "segment %s has unparseable %s attribute '%s'; cannot verify Parquet file %s",
                    segmentName,
                    ParquetSegmentLayout.WRITER_GENERATION_ATTRIBUTE,
                    segmentAttr,
                    file
                )
            );
        }
        if (segmentGeneration != footerGeneration) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s was written by generation %d but segment %s is generation %d",
                    file,
                    footerGeneration,
                    segmentName,
                    segmentGeneration
                )
            );
        }
    }

    /** Renders the inclusive supported version range for an error message. */
    private static String supportedRange() {
        return "[" + describeFormatVersion(MIN_SUPPORTED_FORMAT_VERSION) + ", " + describeFormatVersion(MAX_SUPPORTED_FORMAT_VERSION) + "]";
    }

    /** Renders a long-encoded format version as {@code major.minor.patch} for an error message. */
    private static String describeFormatVersion(long formatVersion) {
        long major = formatVersion / MAJOR_SCALE;
        long minor = formatVersion / MINOR_SCALE % MINOR_SCALE;
        long patch = formatVersion % MINOR_SCALE;
        return major + "." + minor + "." + patch;
    }

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

    /**
     * Opens a dedicated forward-only cursor for one iterator and records it on the request's
     * {@code cursors}, which closes it at request end.
     */
    /** Opens a cursor over {@code field}'s column and records it on the request's registry. */
    ParquetColumnReader openCursor(String field, CursorRegistry cursors) throws IOException {
        ParquetColumnReader reader = ParquetColumnReader.open(parquetFile, field, indexSettings, storePointer);
        cursors.register(reader);
        return reader;
    }

    /**
     * Opens a dedicated forward-only binary cursor for one iterator. It is not recorded on the
     * request's {@code CursorRegistry}: a leaf reader retained by the fielddata cache serves later
     * requests whose registry is already closed, so the iterator that receives this cursor closes it.
     */
    private ParquetColumnReader openBinaryCursor(String field) throws IOException {
        return ParquetColumnReader.openBinary(parquetFile, field, indexSettings, storePointer);
    }

    /**
     * Opens a list cursor for one multi-valued iterator, released with the request's {@code cursors}.
     */
    private ParquetListColumnReader openListCursor(String field, CursorRegistry cursors) throws IOException {
        ParquetListColumnReader reader = ParquetListColumnReader.open(parquetFile, field, indexSettings, storePointer);
        cursors.register(reader);
        return reader;
    }

    private UnsupportedOperationException unsupported(String kind, FieldInfo field) {
        return new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "Parquet DocValues codec does not serve %s doc values (field '%s'); sorted-numeric and sorted-set only",
                kind,
                field.getName()
            )
        );
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ParquetDocValuesProducer is closed");
        }
    }

    /** Whether {@link #close()} has run. */
    boolean isClosed() {
        return closed;
    }
}
