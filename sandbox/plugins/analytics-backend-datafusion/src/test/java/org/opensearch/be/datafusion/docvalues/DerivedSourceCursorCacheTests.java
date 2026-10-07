/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.DerivedSourceCursorCache.OpenedKeyword;
import org.opensearch.be.datafusion.docvalues.DerivedSourceCursorCache.OpenedNumeric;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link DerivedSourceCursorCache}'s reuse, eviction, threading, and close semantics,
 * driven with fake iterators so no native cursor is opened. Correctness of the real iterators over real
 * Parquet (backward targets, repeat targets, keyword values) is covered by
 * {@link ParquetDerivedSourceCursorReuseTests}.
 */
public class DerivedSourceCursorCacheTests extends OpenSearchTestCase {

    private static final Object SEGMENT_A = new Object();
    private static final Object SEGMENT_B = new Object();

    /** A closeable that counts closes, standing in for a native cursor. */
    private static final class CountingCursor implements Closeable {
        private int closes;

        @Override
        public void close() {
            closes++;
        }
    }

    /** Two hits over the same (segment, field) open the column's cursor once and reuse it. */
    public void testSameSegmentAndFieldReusesOneCursor() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        AtomicInteger opens = new AtomicInteger();
        CountingCursor cursor = new CountingCursor();

        for (int hit = 0; hit < 2; hit++) {
            final int target = hit;
            SortedNumericDocValues dv = cache.sortedNumeric(SEGMENT_A, "n", () -> {
                opens.incrementAndGet();
                return new OpenedNumeric(singleValued(42L), cursor);
            });
            assertTrue(dv.advanceExact(target));
            assertEquals(1, dv.docValueCount());
            assertEquals(42L, dv.nextValue());
        }

        assertEquals("the opener runs once; the second hit reuses the cursor", 1, opens.get());
        assertEquals("one live cached cursor", 1, cache.liveCursorCount());
        assertEquals("cursor stays open while reused", 0, cursor.closes);
        cache.close();
    }

    /** When segment B asks for a column cached for segment A, A's cursor is closed and one stays live. */
    public void testSegmentSwitchEvictsAndClosesPreviousCursor() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        CountingCursor cursorA = new CountingCursor();
        CountingCursor cursorB = new CountingCursor();

        cache.sortedNumeric(SEGMENT_A, "n", () -> new OpenedNumeric(singleValued(1L), cursorA));
        assertEquals(0, cursorA.closes);

        SortedNumericDocValues fromB = cache.sortedNumeric(SEGMENT_B, "n", () -> new OpenedNumeric(singleValued(2L), cursorB));
        assertEquals("segment A's cursor is closed eagerly on switch", 1, cursorA.closes);
        assertEquals("at most one live cursor per column across segments", 1, cache.liveCursorCount());
        assertTrue(fromB.advanceExact(0));
        assertEquals(2L, fromB.nextValue());
        cache.close();
    }

    /** Multi-valued numerics are delegated through the per-call wrapper, and restart on each advanceExact. */
    public void testMultiValuedNumericDelegatesThroughWrapper() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        SortedNumericDocValues dv = cache.sortedNumeric(
            SEGMENT_A,
            "n",
            () -> new OpenedNumeric(multiValued(10L, 20L, 30L), new CountingCursor())
        );

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        assertEquals(10L, dv.nextValue());
        assertEquals(20L, dv.nextValue());
        assertEquals(30L, dv.nextValue());

        // A second advanceExact must restart the value cursor rather than continue past the end.
        assertTrue(dv.advanceExact(1));
        assertEquals(3, dv.docValueCount());
        assertEquals(10L, dv.nextValue());
        cache.close();
    }

    /** A call from a non-owning thread bypasses the cache: a fresh cursor, uncached, and others untouched. */
    public void testOtherThreadBypassesCacheAndTouchesNothing() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        CountingCursor owned = new CountingCursor();

        // Bind the cache to this thread and cache one column.
        cache.sortedNumeric(SEGMENT_A, "n", () -> new OpenedNumeric(singleValued(1L), owned));
        assertEquals(1, cache.liveCursorCount());

        AtomicInteger bypassOpens = new AtomicInteger();
        CountingCursor bypassCursor = new CountingCursor();
        Thread other = new Thread(() -> {
            try {
                SortedNumericDocValues dv = cache.sortedNumeric(SEGMENT_A, "n", () -> {
                    bypassOpens.incrementAndGet();
                    return new OpenedNumeric(singleValued(99L), bypassCursor);
                });
                assertTrue(dv.advanceExact(0));
                assertEquals(99L, dv.nextValue());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        other.start();
        other.join();

        assertEquals("the other thread opened its own fresh cursor", 1, bypassOpens.get());
        assertEquals("the bypass path does not cache", 1, cache.liveCursorCount());
        assertEquals("the bypass path does not close the owner thread's cursor", 0, owned.closes);
        assertEquals("the bypass path leaves its own cursor to the registry/Cleaner, not the cache", 0, bypassCursor.closes);
        cache.close();
        assertEquals("request close closes the owner thread's cached cursor", 1, owned.closes);
    }

    /** Request close frees every cached cursor (numeric and keyword), and a second close is a no-op. */
    public void testCloseFreesAllCursorsAndIsIdempotent() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        CountingCursor numericCursor = new CountingCursor();
        CountingCursor keywordCursor = new CountingCursor();

        cache.sortedNumeric(SEGMENT_A, "n", () -> new OpenedNumeric(singleValued(1L), numericCursor));
        cache.keyword(SEGMENT_A, "k", () -> new OpenedKeyword(constantSorted(new BytesRef("x")), keywordCursor));
        assertEquals(2, cache.liveCursorCount());

        cache.close();
        assertEquals("numeric cursor closed at request end", 1, numericCursor.closes);
        assertEquals("keyword cursor closed at request end", 1, keywordCursor.closes);
        assertEquals(0, cache.liveCursorCount());

        cache.close(); // second close must not throw; the cursors are already drained
        assertEquals("no double close of a drained cursor", 1, numericCursor.closes);
        assertEquals("no double close of a drained cursor", 1, keywordCursor.closes);
    }

    /** A single-valued SortedNumericDocValues returning {@code value} at any present doc. */
    private static SortedNumericDocValues singleValued(long value) {
        return new SortedNumericDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public long nextValue() {
                return value;
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return target;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    /** A multi-valued SortedNumericDocValues whose value cursor restarts on each advanceExact. */
    private static SortedNumericDocValues multiValued(long... values) {
        return new SortedNumericDocValues() {
            private int doc = -1;
            private int index;

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                index = 0;
                return true;
            }

            @Override
            public long nextValue() {
                return values[index++];
            }

            @Override
            public int docValueCount() {
                return values.length;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc;
            }

            @Override
            public int advance(int target) {
                doc = target;
                index = 0;
                return target;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    /** A single-valued SortedDocValues serving {@code term} at any present doc; ordinal 0. */
    private static SortedDocValues constantSorted(BytesRef term) {
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public int ordValue() {
                return 0;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return term;
            }

            @Override
            public int getValueCount() {
                return 1;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return target;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }
}
