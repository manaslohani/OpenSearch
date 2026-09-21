/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListValueReader;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * Drives {@link ParquetSortedNumericDocValues} over an in-memory {@link DecodedListBatch} laid out as the
 * native list cursor exports it, without the native cursor.
 */
public class ParquetSortedNumericDocValuesTests extends OpenSearchTestCase {

    /** GC-managed arena: the borrowed-buffer views stay valid for as long as the test holds them. */
    private final Arena arena = Arena.ofAuto();

    /** A multi-value row must surface every value through docValueCount()/nextValue(), not just the first. */
    public void testMultiValueRowReturnsAllValues() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 7, 2, 5 }, new long[] { 4, 1 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 2, false);

        assertTrue(dv.advanceExact(0));
        assertEquals("doc0 must expose all three values, not one", 3, dv.docValueCount());
        assertEquals(2L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
        assertEquals(7L, dv.nextValue());

        assertTrue(dv.advanceExact(1));
        assertEquals(2, dv.docValueCount());
        assertEquals(1L, dv.nextValue());
        assertEquals(4L, dv.nextValue());
    }

    /**
     * With no marker, a descending row is returned ascending (min first, max last).
     */
    public void testDescendingRowIsReturnedAscendingWhenMarkerAbsent() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 7, 2, 5 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        long first = dv.nextValue();
        long second = dv.nextValue();
        long third = dv.nextValue();
        assertEquals("min must be first", 2L, first);
        assertEquals(5L, second);
        assertEquals("max must be last", 7L, third);
        assertTrue("values must be strictly ascending", first < second && second < third);
    }

    /**
     * With {@code valuesSorted == true} the iterator trusts the writer and keeps document order.
     */
    public void testMarkerTrueSkipsSortAndKeepsDocumentOrder() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 7, 2, 5 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 1, true);

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        assertEquals(7L, dv.nextValue());
        assertEquals(2L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
    }

    /** An empty list means the document has no values, so advanceExact reports it absent. */
    public void testEmptyListDocumentIsAbsent() throws Exception {
        DecodedListBatch batch = listBatch(new long[] {}, new long[] { 4, 1 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 2, false);

        assertFalse("an empty list is a document with no values", dv.advanceExact(0));
        assertTrue(dv.advanceExact(1));
        assertEquals(2, dv.docValueCount());
    }

    /** nextDoc/advance skip documents whose list is empty, landing on the next document that has values. */
    public void testNextDocSkipsEmptyRows() throws Exception {
        DecodedListBatch batch = listBatch(new long[] {}, new long[] { 9 }, new long[] {});
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 3, false);

        assertEquals(1, dv.nextDoc());
        assertEquals(1, dv.docValueCount());
        assertEquals(9L, dv.nextValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
    }

    /** A row with a single value still comes back as a one-element list, not a scalar. */
    public void testSingleValueRowStillCountsAsAList() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 42 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(1, dv.docValueCount());
        assertEquals(42L, dv.nextValue());
    }

    /** A row of only null elements has no values, so advanceExact reports it absent; a present neighbour survives. */
    public void testNullElementsDroppedViaPresenceBits() throws Exception {
        DecodedListBatch batch = presenceListBatch(new Long[] { null, 5L }, new Long[] { null });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 2, false);

        assertTrue(dv.advanceExact(0));
        assertEquals("only the present element survives", 1, dv.docValueCount());
        assertEquals(5L, dv.nextValue());

        assertFalse("a row of only null elements has no values", dv.advanceExact(1));
        assertEquals(0, dv.docValueCount());
    }

    /**
     * A double row with negatives, -0.0 and NaN comes back ascending in the reader's sortable-long encoding,
     * decoding to the numeric order -0.0 &lt; 0.0 &lt; ... &lt; NaN.
     */
    public void testDoubleRowReturnedInSortableEncodedOrder() throws Exception {
        double[] row = { 3.25, -1.5, -0.0, 0.0, Double.NaN, -2.0 };
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(doubleListBatch(row)), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(row.length, dv.docValueCount());
        double[] expected = { -2.0, -1.5, -0.0, 0.0, 3.25, Double.NaN };
        long previous = 0L;
        for (int i = 0; i < expected.length; i++) {
            long encoded = dv.nextValue();
            if (i > 0) {
                assertTrue("sortable longs must be strictly ascending", encoded > previous);
            }
            previous = encoded;
            double decoded = NumericUtils.sortableLongToDouble(encoded);
            if (Double.isNaN(expected[i])) {
                assertTrue("NaN must sort last", Double.isNaN(decoded));
            } else {
                assertEquals("value " + i, expected[i], decoded, 0.0);
            }
        }
    }

    /**
     * unsigned_long raw bits are sorted by signed order, so values at or above 2^63 (sign bit set) come back
     * before the small positives; every input value is preserved.
     */
    public void testUnsignedLongAbove2Pow63ReturnedInSignedEncodedOrder() throws Exception {
        long twoPow63 = Long.MIN_VALUE; // 2^63 as unsigned bits
        long maxUnsigned = -1L;         // 2^64 - 1 as unsigned bits
        long[] row = { 1L, maxUnsigned, twoPow63, 7L };
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(listBatch(row)), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(4, dv.docValueCount());
        long[] expected = { twoPow63, maxUnsigned, 1L, 7L };
        long previous = 0L;
        for (int i = 0; i < expected.length; i++) {
            long value = dv.nextValue();
            assertEquals("value " + i, expected[i], value);
            if (i > 0) {
                assertTrue("reader emits signed-ascending encoded order", value > previous);
            }
            previous = value;
        }
    }

    /** Duplicate values within one document are all preserved and returned ascending. */
    public void testDuplicateValuesWithinDocumentPreserved() throws Exception {
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(listBatch(new long[] { 5, 5, 2, 5 })), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals("duplicates are kept", 4, dv.docValueCount());
        assertEquals(2L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
    }

    private static ListValueReader reader(DecodedListBatch batch) {
        return new ListValueReader() {
            @Override
            public DecodedListBatch decodedListBatch() {
                return batch;
            }

            @Override
            public void loadListBatchContaining(long row) {
                // The single resident batch already covers every row in these fixtures.
            }
        };
    }

    /**
     * Builds an {@code i32} offsets buffer ({@code rows.length + 1} entries) over a flat {@code i64} child buffer.
     */
    private DecodedListBatch listBatch(long[]... rows) {
        int childCount = Arrays.stream(rows).mapToInt(r -> r.length).sum();
        MemorySegment offsets = arena.allocate((long) (rows.length + 1) * Integer.BYTES);
        MemorySegment values = arena.allocate((long) Math.max(childCount, 1) * Long.BYTES);
        int running = 0;
        int childIndex = 0;
        offsets.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        for (int r = 0; r < rows.length; r++) {
            for (long v : rows[r]) {
                values.setAtIndex(ValueLayout.JAVA_LONG, childIndex++, v);
            }
            running += rows[r].length;
            offsets.setAtIndex(ValueLayout.JAVA_INT, r + 1, running);
        }
        DecodedBatch child = new DecodedBatch(0, childCount - 1L, values, DecodedBatch.KIND_LONG, 0, null, null, 0);
        return new DecodedListBatch(0, rows.length - 1L, offsets, child);
    }

    /**
     * Builds a list batch whose child carries a packed presence bitset; a {@code null} element is absent, a
     * non-null element sets its presence bit and stores its value.
     */
    private DecodedListBatch presenceListBatch(Long[]... rows) {
        int childCount = Arrays.stream(rows).mapToInt(r -> r.length).sum();
        MemorySegment offsets = arena.allocate((long) (rows.length + 1) * Integer.BYTES);
        MemorySegment values = arena.allocate((long) Math.max(childCount, 1) * Long.BYTES);
        MemorySegment presence = arena.allocate(Math.max((childCount + 7) / 8, 1));
        int running = 0;
        int childIndex = 0;
        offsets.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        for (int r = 0; r < rows.length; r++) {
            for (Long v : rows[r]) {
                if (v != null) {
                    values.setAtIndex(ValueLayout.JAVA_LONG, childIndex, v);
                    long byteIdx = childIndex >>> 3;
                    byte bits = presence.get(ValueLayout.JAVA_BYTE, byteIdx);
                    presence.set(ValueLayout.JAVA_BYTE, byteIdx, (byte) (bits | (1 << (childIndex & 7))));
                }
                childIndex++;
            }
            running += rows[r].length;
            offsets.setAtIndex(ValueLayout.JAVA_INT, r + 1, running);
        }
        DecodedBatch child = new DecodedBatch(0, childCount - 1L, values, DecodedBatch.KIND_LONG, 0, null, presence, 0);
        return new DecodedListBatch(0, rows.length - 1L, offsets, child);
    }

    /** Builds a list batch over a flat {@code f64}-bits child buffer, decoded as {@link DecodedBatch#KIND_DOUBLE}. */
    private DecodedListBatch doubleListBatch(double[]... rows) {
        int childCount = Arrays.stream(rows).mapToInt(r -> r.length).sum();
        MemorySegment offsets = arena.allocate((long) (rows.length + 1) * Integer.BYTES);
        MemorySegment values = arena.allocate((long) Math.max(childCount, 1) * Long.BYTES);
        int running = 0;
        int childIndex = 0;
        offsets.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        for (int r = 0; r < rows.length; r++) {
            for (double v : rows[r]) {
                values.setAtIndex(ValueLayout.JAVA_LONG, childIndex++, Double.doubleToLongBits(v));
            }
            running += rows[r].length;
            offsets.setAtIndex(ValueLayout.JAVA_INT, r + 1, running);
        }
        DecodedBatch child = new DecodedBatch(0, childCount - 1L, values, DecodedBatch.KIND_DOUBLE, 0, null, null, 0);
        return new DecodedListBatch(0, rows.length - 1L, offsets, child);
    }
}
