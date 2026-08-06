/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.benchmark;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.ParquetDocValuesSkipper;
import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.iter.ParquetNumericDocValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark demonstrating the efficacy of {@link ParquetDocValuesSkipper}: a counting
 * range-scan over a value-sorted column (the shape of a range filter or date-histogram over a
 * timestamp-sorted log index, where Parquet page min/max are tight and disjoint), implemented
 * two ways:
 *
 * <ul>
 *   <li>{@code rangeScanFullIteration} — the pre-skipper reality: iterate every doc,
 *       {@code advanceExact} each, test its value against the range. Every page gets decoded
 *       even when its [min, max] excludes the range entirely.</li>
 *   <li>{@code rangeScanWithSkipper} — the skipper discipline used by Lucene's range
 *       machinery ({@code DocValuesSkipper.advance(min, max)}): pages whose stats exclude the
 *       range are skipped at metadata cost — no decode, no FFM, no per-doc iteration. Only
 *       intersecting pages are iterated.</li>
 * </ul>
 *
 * <p>Both methods compute the identical count (asserted at setup), so the score difference is
 * pure page-exclusion. {@code selectivity} controls the fraction of rows (and, with sorted
 * data, pages) the range covers: at 0.01 the skipper visits ~1% of pages; at 1.0 it visits all
 * of them and measures pure skipper overhead over the full scan.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew -Dsandbox.enabled=true :sandbox:plugins:parquet-data-format:benchmarks:run \
 *   --args 'DocValuesSkipperBenchmark'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class DocValuesSkipperBenchmark {

    /** Total rows; 1M rows / default 20k page_row_limit ≈ 50 pages. */
    @Param({ "1000000" })
    private int rows;

    /**
     * Fraction of rows the range covers, centered in the value space. With sorted data this is
     * also the fraction of pages that intersect the range.
     */
    @Param({ "0.01", "0.1", "1.0" })
    private double selectivity;

    private static final int BATCH_ROWS = 100_000;

    private BufferAllocator allocator;
    private Path file;
    private BufferPool bufferPool;
    private ParquetColumnReader reader;
    private ParquetNumericDocValues docValues;
    private long rangeLo;
    private long rangeHi;
    private long expectedCount;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        RustBridge.initLogger();
        allocator = new RootAllocator();
        file = Files.createTempDirectory("skipper-bench").resolve("bench.parquet");
        writeSortedFile();

        bufferPool = new BufferPool();
        reader = ParquetColumnReader.open(file, "ts", ParquetPhysicalType.INT64, false, bufferPool);
        docValues = new ParquetNumericDocValues(reader, rows);

        // Value of row i is i (sorted), so a centered range of `selectivity * rows` values
        // matches exactly that many docs.
        long span = Math.max(1, (long) (rows * selectivity));
        rangeLo = (rows - span) / 2;
        rangeHi = rangeLo + span - 1;
        expectedCount = rangeHi - rangeLo + 1;

        // Both strategies must agree before we measure either.
        long full = rangeScanFullIteration();
        long skip = rangeScanWithSkipper();
        if (full != expectedCount || skip != expectedCount) {
            throw new IllegalStateException("strategy mismatch: full=" + full + " skipper=" + skip + " expected=" + expectedCount);
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (bufferPool != null) {
            bufferPool.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    /** Pre-skipper: advanceExact every doc and test its value. Decodes every page. */
    @Benchmark
    public long rangeScanFullIteration() throws IOException {
        long count = 0;
        for (int doc = 0; doc < rows; doc++) {
            if (docValues.advanceExact(doc)) {
                long v = docValues.longValue();
                if (v >= rangeLo && v <= rangeHi) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Skipper-driven: {@code advance(rangeLo, rangeHi)} lands on the next page whose
     * [min, max] intersects the range; excluded pages cost one metadata comparison each.
     * Only intersecting pages are iterated (and hence decoded).
     */
    @Benchmark
    public long rangeScanWithSkipper() throws IOException {
        ParquetDocValuesSkipper skipper = new ParquetDocValuesSkipper(reader.pageIndex(), rows);
        long count = 0;
        skipper.advance(rangeLo, rangeHi);
        while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
            int end = skipper.maxDocID(0);
            for (int doc = Math.max(skipper.minDocID(0), 0); doc <= end; doc++) {
                if (docValues.advanceExact(doc)) {
                    long v = docValues.longValue();
                    if (v >= rangeLo && v <= rangeHi) {
                        count++;
                    }
                }
            }
            if (end >= rows - 1) {
                break;
            }
            skipper.advance(end + 1);
            // Re-apply the range so the skipper jumps over non-intersecting pages.
            skipper.advance(rangeLo, rangeHi);
        }
        return count;
    }

    // ── data generation ──

    /** Writes a single INT64 column "ts" with value(row i) == i — sorted, no nulls. */
    private void writeSortedFile() throws Exception {
        Schema schema = new Schema(List.of(new Field("ts", FieldType.nullable(new ArrowType.Int(64, true)), null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("skipper-bench-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }

        for (int start = 0; start < rows; start += BATCH_ROWS) {
            int batch = Math.min(BATCH_ROWS, rows - start);
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector tsVec = (BigIntVector) root.getVector("ts");
                for (int i = 0; i < batch; i++) {
                    tsVec.setSafe(i, start + i);
                }
                root.setRowCount(batch);

                ArrowArray array = ArrowArray.allocateNew(allocator);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
                try (ArrowExport export = new ArrowExport(array, arrowSchema)) {
                    writer.write(export.getArrayAddress(), export.getSchemaAddress());
                }
            }
        }
        writer.flush();
    }

    private ArrowExport exportSchema(Schema schema) {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }
}
