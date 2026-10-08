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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.file.Path;
import java.util.List;

/**
 * Test helper that writes a real single-column {@code Binary} Parquet file via {@link NativeParquetWriter}
 * (the shape {@code BinaryParquetField} writes), so codec tests can read it back through the native
 * cursor. Every {@code nullEvery}-th row is left null when {@code nullEvery > 0}.
 */
public final class BinaryColumnFixture {

    private BinaryColumnFixture() {}

    /** Row lengths cycle 0, 3, 6, 9, 200: an empty value, and one whose length needs a two-byte vInt. */
    public static int lengthAt(long row) {
        return row % 5 == 4 ? 200 : (int) (row % 5) * 3;
    }

    /** The value written at a present row; byte {@code i} is a simple function of the row and position. */
    public static byte[] valueAt(long row) {
        byte[] value = new byte[lengthAt(row)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (row * 31 + i);
        }
        return value;
    }

    public static void write(Path file, BufferAllocator allocator, String column, int rowCount, int nullEvery) throws Exception {
        byte[][] values = new byte[rowCount][];
        for (int i = 0; i < rowCount; i++) {
            values[i] = (nullEvery > 0 && i % nullEvery == 0) ? null : valueAt(i);
        }
        writeValues(file, allocator, column, values, nullEvery > 0);
    }

    /**
     * Writes a single-column {@code Binary} Parquet file whose present rows are {@code values}; a null
     * entry writes a null row. {@code nullable} picks the Arrow field type, so a column with no nulls can
     * still be written non-nullable to mirror the write path.
     */
    public static void writeValues(Path file, BufferAllocator allocator, String column, byte[][] values, boolean nullable)
        throws Exception {
        int rowCount = values.length;
        FieldType fieldType = nullable ? FieldType.nullable(new ArrowType.Binary()) : FieldType.notNullable(new ArrowType.Binary());
        Schema schema = new Schema(List.of(new Field(column, fieldType, null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport export = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarBinaryVector vector = (VarBinaryVector) root.getVector(column);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (values[i] == null) {
                    vector.setNull(i);
                } else {
                    vector.setSafe(i, values[i]);
                }
            }
            vector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
            ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
            try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
        }
        writer.flush();
    }
}
