/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.opensearch.parquet.codec.ParquetDocValuesFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link CompositeParquetDocValuesFormat} per-field routing (Req 13.1, 13.2,
 * 13.3). Pure-Java: verifies the routing decision and dispatch without writing a segment.
 */
public class CompositeParquetDocValuesFormatTests extends OpenSearchTestCase {

    public void testIsParquetResident() {
        assertTrue(CompositeParquetDocValuesFormat.isParquetResident(List.of("parquet")));
        assertTrue(
            "both formats present defaults to parquet",
            CompositeParquetDocValuesFormat.isParquetResident(List.of("parquet", "lucene"))
        );
        assertFalse(CompositeParquetDocValuesFormat.isParquetResident(List.of("lucene")));
        assertFalse(CompositeParquetDocValuesFormat.isParquetResident(List.of()));
        assertFalse(CompositeParquetDocValuesFormat.isParquetResident(null));
    }

    public void testPerFieldRoutingDispatch() {
        Map<String, List<String>> routing = Map.of(
            "price",
            List.of("parquet"),
            "tags",
            List.of("parquet", "lucene"),
            "title",
            List.of("lucene")
        );
        CompositeParquetDocValuesFormat format = new CompositeParquetDocValuesFormat(null, f -> routing.get(f));

        // Parquet-routed fields dispatch to the Parquet format.
        DocValuesFormat priceFormat = format.getDocValuesFormatForField("price");
        assertEquals(ParquetDocValuesFormat.FORMAT_NAME, priceFormat.getName());

        DocValuesFormat tagsFormat = format.getDocValuesFormatForField("tags");
        assertEquals("both-formats field defaults to Parquet", ParquetDocValuesFormat.FORMAT_NAME, tagsFormat.getName());

        // Lucene-routed and unknown fields dispatch to the native Lucene format (not Parquet).
        DocValuesFormat titleFormat = format.getDocValuesFormatForField("title");
        assertNotEquals(ParquetDocValuesFormat.FORMAT_NAME, titleFormat.getName());

        DocValuesFormat unknownFormat = format.getDocValuesFormatForField("does_not_exist");
        assertNotEquals(ParquetDocValuesFormat.FORMAT_NAME, unknownFormat.getName());
    }
}
