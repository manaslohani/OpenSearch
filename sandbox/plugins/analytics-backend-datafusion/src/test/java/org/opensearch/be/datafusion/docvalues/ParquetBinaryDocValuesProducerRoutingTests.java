/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.BinaryListFramingDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.BinaryFieldMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * The producer routes a repeated binary column to {@link BinaryListFramingDocValues} and a scalar
 * column to the single-valued path, both by physical shape. A physically repeated column served
 * through {@code getBinary} must read back multi-valued and frame like the vanilla binary field.
 */
public class ParquetBinaryDocValuesProducerRoutingTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "blob";

    public void testRepeatedBinaryColumnIsServedThroughTheListConsumer() throws Exception {
        byte[][][] rows = { { b("ab"), b("cd") }, { b("z") }, {}, { b("m"), b("m"), b("a") } };
        Path file = createTempDir().resolve("repeated-binary.parquet");
        BinaryListColumnFixture.write(file, allocator, COLUMN, rows);

        try (
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
                file,
                ParquetColumnReader.LOCAL_STORE,
                Settings.EMPTY,
                rows.length,
                null
            );
            CursorRegistry cursors = new CursorRegistry()
        ) {
            BinaryDocValues dv = producer.getBinary(binaryFieldInfo(), cursors);
            assertTrue("the repeated path must produce the list consumer", dv instanceof BinaryListFramingDocValues);

            // doc 0 and doc 1 hold values; doc 2 is empty; doc 3 has a duplicate to dedup.
            assertArrayEquals(vanillaFrame(rows[0]), frame(dv, 0));
            assertArrayEquals(vanillaFrame(rows[1]), frame(dv, 1));
            assertFalse("the empty list carries no value", dv.advanceExact(2));
            assertArrayEquals(vanillaFrame(rows[3]), frame(dv, 3));
        }
    }

    private static byte[] frame(BinaryDocValues dv, int doc) throws Exception {
        assertTrue("doc " + doc + " must be present", dv.advanceExact(doc));
        BytesRef out = dv.binaryValue();
        return Arrays.copyOfRange(out.bytes, out.offset, out.offset + out.length);
    }

    private static byte[] vanillaFrame(byte[]... values) {
        BinaryFieldMapper.CustomBinaryDocValuesField field = null;
        for (byte[] value : values) {
            if (field == null) {
                field = new BinaryFieldMapper.CustomBinaryDocValuesField(COLUMN, value);
            } else {
                field.add(value);
            }
        }
        BytesRef ref = field.binaryValue();
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }

    private static org.apache.lucene.index.FieldInfo binaryFieldInfo() {
        return new org.apache.lucene.index.FieldInfo(
            COLUMN,
            0,
            false,
            true,
            false,
            org.apache.lucene.index.IndexOptions.NONE,
            org.apache.lucene.index.DocValuesType.BINARY,
            org.apache.lucene.index.DocValuesSkipIndexType.NONE,
            -1,
            new java.util.HashMap<>(),
            0,
            0,
            0,
            0,
            org.apache.lucene.index.VectorEncoding.FLOAT32,
            org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
