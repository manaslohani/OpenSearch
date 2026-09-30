/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.file.Path;
import java.util.List;

/**
 * Test helper that writes a real {@code list<binary>} Parquet column via {@link NativeParquetWriter} -
 * the shape a multi-valued {@code binary} field writes through {@code BinaryParquetField} into a
 * {@code VarBinaryVector} list child. Codec tests read it back through {@code ParquetListColumnReader}
 * and the multi-valued read path.
 *
 * <p>A {@code null} outer row writes a null list; a {@code null} element writes a null child slot; an
 * empty {@code byte[][]} writes an empty list.
 */
public final class BinaryListColumnFixture {

    private BinaryListColumnFixture() {}

    /**
     * Writes {@code rows} as a {@code list<binary>} column named {@code column}. {@code rows[r] == null}
     * is a null list row; a {@code null} element inside a row is a null child slot.
     */
    public static void write(Path file, BufferAllocator allocator, String column, byte[][][] rows) throws Exception {
        try (ListVector listVector = ListVector.empty(column, allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Binary()));
            VarBinaryVector data = (VarBinaryVector) listVector.getDataVector();

            int childCapacity = 0;
            for (byte[][] row : rows) {
                if (row != null) {
                    childCapacity += row.length;
                }
            }
            listVector.allocateNew();
            data.allocateNew(Math.max(childCapacity, 1));

            int child = 0;
            for (int r = 0; r < rows.length; r++) {
                if (rows[r] == null) {
                    listVector.setNull(r);
                    continue;
                }
                listVector.startNewValue(r);
                for (byte[] value : rows[r]) {
                    if (value == null) {
                        data.setNull(child);
                    } else {
                        data.setSafe(child, value);
                    }
                    child++;
                }
                listVector.endValue(r, rows[r].length);
            }
            data.setValueCount(child);
            listVector.setValueCount(rows.length);

            Schema schema = new Schema(List.of(listVector.getField()));
            NativeParquetWriter parquetWriter = new NativeParquetWriter(file.toString());
            try {
                ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
                Data.exportSchema(allocator, schema, null, schemaExport);
                try (ArrowExport export = new ArrowExport(null, schemaExport)) {
                    parquetWriter.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
                }
                try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(listVector), rows.length)) {
                    ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
                    ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
                    Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
                    try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                        parquetWriter.write(export.getArrayAddress(), export.getSchemaAddress());
                    }
                }
                parquetWriter.flush();
            } finally {
                parquetWriter.cleanup();
            }
        }
    }
}
