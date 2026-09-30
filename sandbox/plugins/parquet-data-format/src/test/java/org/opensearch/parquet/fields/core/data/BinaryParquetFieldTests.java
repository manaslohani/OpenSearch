/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class BinaryParquetFieldTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testArrowType() {
        BinaryParquetField field = new BinaryParquetField();
        assertTrue(field.getArrowType() instanceof ArrowType.Binary);
        assertTrue(field.getFieldType().isNullable());
    }

    public void testAddToGroup() {
        BinaryParquetField field = new BinaryParquetField();
        MappedFieldType ft = new BinaryFieldMapper.BinaryFieldType("val");
        Schema schema = new Schema(List.of(new Field("val", field.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator("bin-test", 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("bin-test", schema, child);

        // BinaryParquetField uses set() not setSafe(), so allocate capacity first
        ((VarBinaryVector) vsr.getVector("val")).allocateNew(64, 1);

        byte[] data = new byte[] { 1, 2, 3, 4 };
        field.createField(ft, vsr, data);
        vsr.setRowCount(1);

        byte[] result = ((VarBinaryVector) vsr.getVector("val")).get(0);
        assertArrayEquals(data, result);

        vsr.moveToFrozen();
        vsr.close();
    }

    /**
     * A binary field mapped multi_value writes a LIST&lt;Binary&gt; column. The values land in the
     * child VarBinaryVector in ingest order, unsorted and un-deduped -- the stored-fields shape
     * a get-by-id _source is rebuilt from (doc values sort+dedup on read, stored fields do not).
     */
    public void testMultiValueWritesListInIngestOrder() {
        BinaryParquetField field = new BinaryParquetField();
        MappedFieldType ft = new BinaryFieldMapper.BinaryFieldType("val");
        assertTrue(field.supportsMultiValue());

        Schema schema = new Schema(List.of(field.toArrowField("val", true)));
        BufferAllocator child = allocator.newChildAllocator("bin-list-test", 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("bin-list-test", schema, child);

        byte[] a = new byte[] { 3, 2, 1 };
        byte[] b = new byte[] { 3, 2, 1 }; // duplicate of a: must be kept, not deduped
        byte[] c = new byte[] { 9 };
        field.createField(ft, vsr, List.of(a, b, c));
        vsr.setRowCount(1);

        ListVector list = (ListVector) vsr.getVector("val");
        VarBinaryVector data = (VarBinaryVector) list.getDataVector();
        int start = list.getOffsetBuffer().getInt((long) 0 * ListVector.OFFSET_WIDTH);
        int end = list.getOffsetBuffer().getInt((long) 1 * ListVector.OFFSET_WIDTH);
        assertEquals(3, end - start);
        assertArrayEquals(a, data.get(start));
        assertArrayEquals(b, data.get(start + 1));
        assertArrayEquals(c, data.get(start + 2));

        vsr.moveToFrozen();
        vsr.close();
    }

    /**
     * A scalar binary mapping (no multi_value) produces a plain VarBinaryVector, so the write path
     * expects a single byte[]; handing it an array of values is rejected rather than silently
     * written, keeping scalar columns single-valued.
     */
    public void testScalarColumnRejectsArrayValue() {
        BinaryParquetField field = new BinaryParquetField();
        MappedFieldType ft = new BinaryFieldMapper.BinaryFieldType("val");
        Schema schema = new Schema(List.of(field.toArrowField("val", false)));
        BufferAllocator child = allocator.newChildAllocator("bin-scalar-test", 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("bin-scalar-test", schema, child);
        ((VarBinaryVector) vsr.getVector("val")).allocateNew(64, 1);

        expectThrows(ClassCastException.class, () -> field.createField(ft, vsr, List.of(new byte[] { 1 }, new byte[] { 2 })));

        vsr.moveToFrozen();
        vsr.close();
    }
}
