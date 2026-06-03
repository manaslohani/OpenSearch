/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link PageCache} (task 3.4): presence bit-test correctness across word
 * boundaries and primitive value lookup by global row.
 */
public class PageCacheTests extends OpenSearchTestCase {

    public void testPresenceBitTestWithinAndAcrossWords() {
        PageCache pc = new PageCache();
        pc.firstRow = 100;
        pc.lastRow = 199; // 100 rows
        pc.values = new long[100];
        // Mark even local indices present.
        pc.presenceBits = new long[(100 + 63) >> 6];
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                pc.presenceBits[i >> 6] |= (1L << (i & 63));
            }
            pc.values[i] = 1000 + i;
        }

        assertEquals(100, pc.rowCount());
        for (long row = 100; row <= 199; row++) {
            int local = (int) (row - 100);
            boolean expectedPresent = local % 2 == 0;
            assertEquals("row " + row, expectedPresent, pc.isPresent(row));
            assertEquals("value at row " + row, 1000L + local, pc.valueAt(row));
        }
    }

    public void testPresenceAcrossWord64Boundary() {
        PageCache pc = new PageCache();
        pc.firstRow = 0;
        pc.lastRow = 127; // exactly two 64-bit words
        pc.presenceBits = new long[2];
        // Set local indices 63 and 64 (the word boundary).
        pc.presenceBits[63 >> 6] |= (1L << (63 & 63));
        pc.presenceBits[64 >> 6] |= (1L << (64 & 63));

        assertFalse(pc.isPresent(62));
        assertTrue(pc.isPresent(63));
        assertTrue(pc.isPresent(64));
        assertFalse(pc.isPresent(65));
    }

    public void testContains() {
        PageCache pc = new PageCache();
        pc.firstRow = 10;
        pc.lastRow = 20;
        assertFalse(pc.contains(9));
        assertTrue(pc.contains(10));
        assertTrue(pc.contains(15));
        assertTrue(pc.contains(20));
        assertFalse(pc.contains(21));
    }
}
