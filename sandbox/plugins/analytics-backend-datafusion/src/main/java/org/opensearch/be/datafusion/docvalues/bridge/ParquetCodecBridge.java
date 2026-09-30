/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for the Parquet read codec: binds the forward-only column-cursor downcalls exported by
 * the native library. Kept separate from the write-path {@code RustBridge} so the read and write
 * native surfaces stay independent. The cursor is column-oriented rather than doc-values specific,
 * so later codec parts (binary/keyword columns, a doc-values skipper) bind their downcalls here too.
 */
public final class ParquetCodecBridge {

    private static final MethodHandle OPEN_CURSOR;
    private static final MethodHandle CLOSE_CURSOR;
    private static final MethodHandle RESET_CURSOR;
    private static final MethodHandle NEXT_BATCH;
    private static final MethodHandle NEXT_BINARY_BATCH;
    private static final MethodHandle NEXT_LIST_BATCH;
    private static final MethodHandle FILE_METADATA;
    private static final MethodHandle COLUMN_NON_NULL_COUNT;
    private static final MethodHandle PAGE_COUNT;
    private static final MethodHandle PAGE_INDEX;
    private static final MethodHandle IS_REPEATED;

    /**
     * Value of {@link FileMetadata#opensearchFormatVersion} when the footer carries no parseable
     * stamp. Mirrors {@code ParquetFileMetadata.FORMAT_VERSION_UNKNOWN} in the parquet-data-format
     * plugin, whose writer stamps the version this reader gates on.
     */
    public static final long FORMAT_VERSION_UNKNOWN = 0L;

    /**
     * Value of {@link FileMetadata#writerGeneration} when the footer carries no parseable
     * {@code opensearch.writer_generation} stamp.
     */
    public static final long WRITER_GENERATION_UNKNOWN = -1L;

    /** Status returned by {@link #nextBatch} when a batch was produced. */
    public static final long RC_OK = 0L;
    /**
     * Status returned by {@link #nextBinaryBatch} when the caller's buffers are too small; the needed
     * value-byte length is in {@code outValueActualLen} and the decoded batch is staged for the retry.
     */
    public static final long RC_OVERFLOW = 1L;
    /** Status returned by {@link #nextBatch} when the cursor is exhausted. A {@code < 0} return is an error pointer. */
    public static final long RC_EOF = 2L;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        OPEN_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_open_iter").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // file_ptr
                ValueLayout.JAVA_LONG,  // file_len
                ValueLayout.ADDRESS,    // column_ptr
                ValueLayout.JAVA_LONG,  // column_len
                ValueLayout.JAVA_LONG,  // initial_batch_size
                ValueLayout.JAVA_LONG,  // max_batch_size
                ValueLayout.JAVA_LONG   // store_ptr
            )
        );
        CLOSE_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_close_iter").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        RESET_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_reset_iter").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        NEXT_BATCH = linker.downcallHandle(
            lib.find("parquet_df_next_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.JAVA_LONG,  // target_row
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_last_row
                ValueLayout.ADDRESS,    // out_values_addr
                ValueLayout.ADDRESS,    // out_validity_addr
                ValueLayout.ADDRESS,    // out_validity_bit_offset
                ValueLayout.ADDRESS,    // out_value_kind
                ValueLayout.ADDRESS,    // out_value_bit_offset
                ValueLayout.ADDRESS     // out_offsets_addr
            )
        );
        NEXT_BINARY_BATCH = linker.downcallHandle(
            lib.find("parquet_df_next_binary_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.JAVA_LONG,  // target_row
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_last_row
                ValueLayout.ADDRESS,    // out_value_buf
                ValueLayout.JAVA_LONG,  // out_value_buf_cap
                ValueLayout.ADDRESS,    // out_value_actual_len
                ValueLayout.ADDRESS,    // out_byte_offsets
                ValueLayout.JAVA_LONG,  // out_byte_offsets_cap
                ValueLayout.ADDRESS,    // out_presence_bitset
                ValueLayout.JAVA_LONG   // out_presence_bits_cap
            )
        );
        NEXT_LIST_BATCH = linker.downcallHandle(
            lib.find("parquet_df_next_list_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.JAVA_LONG,  // target_row
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_last_row
                ValueLayout.ADDRESS,    // out_values_addr
                ValueLayout.ADDRESS,    // out_validity_addr
                ValueLayout.ADDRESS,    // out_validity_bit_offset
                ValueLayout.ADDRESS,    // out_value_kind
                ValueLayout.ADDRESS,    // out_value_bit_offset
                ValueLayout.ADDRESS,    // out_offsets_addr
                ValueLayout.ADDRESS,    // out_value_count
                ValueLayout.ADDRESS     // out_child_offsets_addr
            )
        );
        FILE_METADATA = linker.downcallHandle(
            lib.find("parquet_df_file_metadata").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // file_ptr
                ValueLayout.JAVA_LONG,  // file_len
                ValueLayout.JAVA_LONG,  // store_ptr
                ValueLayout.ADDRESS,    // out_num_rows
                ValueLayout.ADDRESS,    // out_format_version
                ValueLayout.ADDRESS,    // out_writer_generation
                ValueLayout.ADDRESS     // out_values_sorted
            )
        );
        COLUMN_NON_NULL_COUNT = linker.downcallHandle(
            lib.find("parquet_df_column_non_null_count").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // file_ptr
                ValueLayout.JAVA_LONG,  // file_len
                ValueLayout.ADDRESS,    // column_ptr
                ValueLayout.JAVA_LONG,  // column_len
                ValueLayout.JAVA_LONG,  // store_ptr
                ValueLayout.ADDRESS     // out_count
            )
        );
        PAGE_COUNT = linker.downcallHandle(
            lib.find("parquet_df_page_count").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        PAGE_INDEX = linker.downcallHandle(
            lib.find("parquet_df_page_index").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_row_count
                ValueLayout.ADDRESS,    // out_null_count
                ValueLayout.ADDRESS,    // out_min_long
                ValueLayout.ADDRESS,    // out_max_long
                ValueLayout.JAVA_LONG,  // out_buf_capacity
                ValueLayout.ADDRESS     // out_actual_pages
            )
        );
        // Matches the Rust `parquet_df_is_repeated(handle: i64) -> i64`.
        IS_REPEATED = linker.downcallHandle(
            lib.find("parquet_df_is_repeated").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG) // handle
        );
    }

    /**
     * A Parquet file's row count, stamped OpenSearch format version, and stamped writer generation.
     *
     * @param numRows                 rows in the file, captured at construction so {@code checkIntegrity}
     *                                can detect the backing file's row count changing under the reader
     * @param opensearchFormatVersion the {@code opensearch.format_version} footer stamp, long-encoded as
     *                                {@code major*1_000_000 + minor*1_000 + patch}, or
     *                                {@link #FORMAT_VERSION_UNKNOWN}
     *                                if the file carries no parseable stamp
     * @param writerGeneration        the {@code opensearch.writer_generation} footer stamp, or
     *                                {@link #WRITER_GENERATION_UNKNOWN} if the file carries no parseable stamp
     * @param valuesSorted            whether the footer marks each row's values ascending; absent reads false
     */
    public record FileMetadata(long numRows, long opensearchFormatVersion, long writerGeneration, boolean valuesSorted) {
    }

    /**
     * Reads {@code file}'s row count and format-version stamp through the same store and footer cache a
     * cursor over that file would use.
     *
     * <p>Distinct from {@code RustBridge.getFileMetadata}, which opens the path as a local file: a warm
     * shard's Parquet files exist only in its object store, so they are reachable only through
     * {@code storePtr}.
     *
     * @param storePtr native object store to read through, or {@code 0} for a local file
     */
    public static FileMetadata fileMetadata(String file, long storePtr) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var numRowsOut = call.longOut();
            var formatVersionOut = call.longOut();
            var writerGenerationOut = call.longOut();
            var valuesSortedOut = call.longOut();
            call.invokeIO(
                FILE_METADATA,
                f.segment(),
                f.len(),
                storePtr,
                numRowsOut,
                formatVersionOut,
                writerGenerationOut,
                valuesSortedOut
            );
            return new FileMetadata(
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                formatVersionOut.get(ValueLayout.JAVA_LONG, 0),
                writerGenerationOut.get(ValueLayout.JAVA_LONG, 0),
                // 0/1 over an i64 out-param.
                valuesSortedOut.get(ValueLayout.JAVA_LONG, 0) != 0
            );
        }
    }

    /**
     * Rows with a non-null value in {@code column}, summed over the footer's row-group column
     * chunks; {@code -1} when any chunk lacks the statistic, read through {@code storePtr}.
     *
     * @param storePtr native object store to read through, or {@code 0} for a local file
     */
    public static long columnNonNullCount(String file, String column, long storePtr) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var c = call.str(column);
            var countOut = call.longOut();
            call.invokeIO(COLUMN_NON_NULL_COUNT, f.segment(), f.len(), c.segment(), c.len(), storePtr, countOut);
            return countOut.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    /** Number of OffsetIndex data pages for the retained cursor's projected column. */
    public static long pageCount(long handle) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(PAGE_COUNT, handle);
        }
    }

    /**
     * Copies the cursor's per-page OffsetIndex and ColumnIndex values into the caller's parallel
     * arrays. Returns {@link #RC_OK}, or {@link #RC_OVERFLOW} when {@code capacity} is below the page
     * count, in which case {@code outActualPages} holds the true count for a retry; a {@code < 0}
     * return is decoded into an {@link IOException}.
     */
    public static long pageIndex(
        long handle,
        MemorySegment outFirstRow,
        MemorySegment outRowCount,
        MemorySegment outNullCount,
        MemorySegment outMinLong,
        MemorySegment outMaxLong,
        long capacity,
        MemorySegment outActualPages
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                PAGE_INDEX,
                handle,
                outFirstRow,
                outRowCount,
                outNullCount,
                outMinLong,
                outMaxLong,
                capacity,
                outActualPages
            );
        }
    }

    /**
     * Opens a forward-only cursor over one Parquet column and returns its native handle.
     *
     * @param initialBatchSize rows in the first decode window; must be in {@code 1..=maxBatchSize}
     * @param maxBatchSize     ceiling the adaptive window grows to, for this cursor's lifetime
     * @param storePtr         native object-store pointer the cursor reads {@code file} through, or
     *                         {@code 0} to read from the local filesystem. A warm shard's Parquet
     *                         files live in the remote object store, so it passes the pointer from
     *                         {@code ParquetDataFormatStoreHandler.getFormatStoreHandle()}; a hot
     *                         shard's files are local and pass {@code 0}.
     */
    public static long openColumnCursor(String file, String column, long initialBatchSize, long maxBatchSize, long storePtr)
        throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var c = call.str(column);
            return call.invokeIO(OPEN_CURSOR, f.segment(), f.len(), c.segment(), c.len(), initialBatchSize, maxBatchSize, storePtr);
        }
    }

    /** Releases a cursor handle. */
    public static void closeColumnCursor(long handle) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(CLOSE_CURSOR, handle);
        }
    }

    /** Rewinds a cursor to row zero, retaining cached file metadata. */
    public static void resetColumnCursor(long handle) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(RESET_CURSOR, handle);
        }
    }

    /**
     * Whether the cursor's projected column is physically repeated (a Parquet LIST); see
     * {@link ParquetColumnReader#isPhysicallyRepeated()}. A {@code < 0} native return becomes an {@link IOException}.
     */
    public static boolean isRepeated(long handle) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(IS_REPEATED, handle) == 1L;
        }
    }

    /**
     * Advances the cursor to the batch containing {@code targetRow}, writing the batch row range,
     * the borrowed Arrow value and validity buffer addresses, the validity bit offset, the value
     * KIND, and the value bit offset into the caller-owned out-parameters. Returns {@link #RC_OK}
     * or {@link #RC_EOF}; a {@code < 0} return is decoded into an {@link IOException}.
     *
     * <p>{@code outValueBitOffset} is meaningful only for the bit-packed boolean KIND; the
     * byte-addressed kinds fold their offset into {@code outValuesAddr} and report zero.
     * {@code outOffsetsAddr} is written only for the variable-width binary KIND (the i32 offsets
     * buffer at row 0); every other kind reports zero, its values living wholly in {@code outValuesAddr}.
     */
    public static long nextBatch(
        long handle,
        long targetRow,
        MemorySegment outFirstRow,
        MemorySegment outLastRow,
        MemorySegment outValuesAddr,
        MemorySegment outValidityAddr,
        MemorySegment outValidityBitOffset,
        MemorySegment outValueKind,
        MemorySegment outValueBitOffset,
        MemorySegment outOffsetsAddr
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                NEXT_BATCH,
                handle,
                targetRow,
                outFirstRow,
                outLastRow,
                outValuesAddr,
                outValidityAddr,
                outValidityBitOffset,
                outValueKind,
                outValueBitOffset,
                outOffsetsAddr
            );
        }
    }

    /**
     * Copies the batch containing {@code targetRow} into the caller's buffers. Returns {@link #RC_OK},
     * {@link #RC_OVERFLOW}, or {@link #RC_EOF}; a {@code < 0} return is decoded into an {@link IOException}.
     */
    public static long nextBinaryBatch(
        long handle,
        long targetRow,
        MemorySegment outFirstRow,
        MemorySegment outLastRow,
        MemorySegment outValueBuf,
        long outValueBufCap,
        MemorySegment outValueActualLen,
        MemorySegment outByteOffsets,
        long outByteOffsetsCap,
        MemorySegment outPresenceBits,
        long outPresenceBitsCap
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                NEXT_BINARY_BATCH,
                handle,
                targetRow,
                outFirstRow,
                outLastRow,
                outValueBuf,
                outValueBufCap,
                outValueActualLen,
                outByteOffsets,
                outByteOffsetsCap,
                outPresenceBits,
                outPresenceBitsCap
            );
        }
    }

    /**
     * List-aware sibling of {@link #nextBatch} for repeated columns: writes the same row range and
     * flat value/validity buffers, plus the {@code i32} list offsets buffer, the child value count, and
     * (for a variable-width binary child) the child's own {@code i32} offsets buffer. Row {@code r}'s
     * child elements are {@code offsets[r]..offsets[r + 1]}; for a binary child, element {@code c}'s bytes
     * are {@code values[childOffsets[c]..childOffsets[c + 1]]}. {@code outChildOffsets} reports zero for a
     * fixed-width child. A distinct native symbol because {@link #nextBatch}'s ABI is depended on by the
     * shipped single-valued read path.
     */
    public static long nextListBatch(
        long handle,
        long targetRow,
        MemorySegment outFirstRow,
        MemorySegment outLastRow,
        MemorySegment outValuesAddr,
        MemorySegment outValidityAddr,
        MemorySegment outValidityBitOffset,
        MemorySegment outValueKind,
        MemorySegment outValueBitOffset,
        MemorySegment outOffsetsAddr,
        MemorySegment outValueCount,
        MemorySegment outChildOffsets
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                NEXT_LIST_BATCH,
                handle,
                targetRow,
                outFirstRow,
                outLastRow,
                outValuesAddr,
                outValidityAddr,
                outValidityBitOffset,
                outValueKind,
                outValueBitOffset,
                outOffsetsAddr,
                outValueCount,
                outChildOffsets
            );
        }
    }

    private ParquetCodecBridge() {}
}
