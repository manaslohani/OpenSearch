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
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * Each thread caches its own cursor for the same column, reuses it across hits, and a segment switch on
     * one thread evicts only that thread's cursor - never the other thread's live one.
     */
    public void testEachThreadGetsItsOwnCursorAndEvictsOnlyItsOwn() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        CountingCursor cursorB = new CountingCursor();
        AtomicInteger opensB = new AtomicInteger();

        // Thread B caches its own column first and leaves it live.
        runOnThread(() -> {
            for (int hit = 0; hit < 2; hit++) {
                final int target = hit;
                SortedNumericDocValues dv = cache.sortedNumeric(SEGMENT_A, "n", () -> {
                    opensB.incrementAndGet();
                    return new OpenedNumeric(singleValued(2L), cursorB);
                });
                assertTrue(dv.advanceExact(target));
                assertEquals("thread B reads its own value", 2L, dv.nextValue());
            }
            assertEquals("thread B opened its column once across both hits", 1, opensB.get());
            assertEquals("thread B sees exactly its own one live cursor", 1, cache.liveCursorCount());
        });

        CountingCursor cursorA1 = new CountingCursor();
        CountingCursor cursorA2 = new CountingCursor();
        AtomicInteger opensA = new AtomicInteger();

        // Thread A caches the same column name independently, then switches segment and evicts ONLY its own.
        runOnThread(() -> {
            for (int hit = 0; hit < 2; hit++) {
                final int target = hit;
                SortedNumericDocValues dv = cache.sortedNumeric(SEGMENT_A, "n", () -> {
                    opensA.incrementAndGet();
                    return new OpenedNumeric(singleValued(1L), cursorA1);
                });
                assertTrue(dv.advanceExact(target));
                assertEquals("thread A reads its own value", 1L, dv.nextValue());
            }
            assertEquals("thread A opened its own column once, not shared with B", 1, opensA.get());
            assertEquals("thread A sees exactly its own one live cursor", 1, cache.liveCursorCount());

            SortedNumericDocValues fromB = cache.sortedNumeric(SEGMENT_B, "n", () -> new OpenedNumeric(singleValued(3L), cursorA2));
            assertEquals("thread A's segment switch closed thread A's own cursor", 1, cursorA1.closes);
            assertEquals("thread A still holds one live cursor after the switch", 1, cache.liveCursorCount());
            assertTrue(fromB.advanceExact(0));
            assertEquals(3L, fromB.nextValue());
        });

        assertEquals("thread A's eviction never touched thread B's cursor", 0, cursorB.closes);

        cache.close();
        assertEquals("close frees thread A's live cursor", 1, cursorA2.closes);
        assertEquals("close frees thread B's live cursor", 1, cursorB.closes);
        assertEquals("A's already-evicted cursor is not closed twice", 1, cursorA1.closes);
    }

    /**
     * Scroll-like handoff: page 1 on thread A then page 2 on thread B over the same fields. Each thread
     * opens one cursor per column and reuses it across all of its hits, so opens are bounded by columns,
     * not by hits.
     */
    public void testScrollLikeHandoffAcrossThreadsReusesPerThreadCursorPerColumn() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        AtomicInteger opens = new AtomicInteger();

        // Page 1 on thread A: two columns over three hits -> two cursors, each reused across the hits.
        runOnThread(() -> fetchPage(cache, opens, 3));
        assertEquals("page 1 opens one cursor per column (2), not per (hit, field)", 2, opens.get());

        // Page 2 on a different thread B, same fields: B reuses ITS OWN one cursor per column, so it opens
        // two more (confined entries are not shared across threads) - still not one per hit.
        runOnThread(() -> fetchPage(cache, opens, 4));
        assertEquals("page 2 on a new thread opens one more cursor per column (2), still not per hit", 4, opens.get());

        cache.close();
    }

    /** Request close frees every thread's cached cursors after multi-thread use, and a second close is a no-op. */
    public void testCloseAfterMultiThreadUseFreesEveryThreadsCursorsAndIsIdempotent() throws Exception {
        DerivedSourceCursorCache cache = new DerivedSourceCursorCache();
        CountingCursor numericA = new CountingCursor();
        CountingCursor keywordA = new CountingCursor();
        CountingCursor numericB = new CountingCursor();

        runOnThread(() -> {
            cache.sortedNumeric(SEGMENT_A, "n", () -> new OpenedNumeric(singleValued(1L), numericA));
            cache.keyword(SEGMENT_A, "k", () -> new OpenedKeyword(constantSorted(new BytesRef("x")), keywordA));
            assertEquals("thread A holds its two cursors", 2, cache.liveCursorCount());
        });
        runOnThread(() -> {
            cache.sortedNumeric(SEGMENT_A, "n", () -> new OpenedNumeric(singleValued(2L), numericB));
            assertEquals("thread B holds its one cursor", 1, cache.liveCursorCount());
        });

        cache.close();
        assertEquals("thread A's numeric cursor closed at request end", 1, numericA.closes);
        assertEquals("thread A's keyword cursor closed at request end", 1, keywordA.closes);
        assertEquals("thread B's numeric cursor closed at request end", 1, numericB.closes);

        cache.close(); // second close must not throw; the cursors are already drained
        assertEquals("no double close of a drained cursor", 1, numericA.closes);
        assertEquals("no double close of a drained cursor", 1, keywordA.closes);
        assertEquals("no double close of a drained cursor", 1, numericB.closes);
    }

    /** Fetches {@code hits} hits over two numeric columns on the calling thread, counting cursor opens. */
    private static void fetchPage(DerivedSourceCursorCache cache, AtomicInteger opens, int hits) throws Exception {
        for (int hit = 0; hit < hits; hit++) {
            for (String field : new String[] { "a", "b" }) {
                final int target = hit;
                SortedNumericDocValues dv = cache.sortedNumeric(SEGMENT_A, field, () -> {
                    opens.incrementAndGet();
                    return new OpenedNumeric(singleValued(7L), new CountingCursor());
                });
                assertTrue(dv.advanceExact(target));
                assertEquals(7L, dv.nextValue());
            }
        }
        assertEquals("the fetching thread holds one live cursor per column", 2, cache.liveCursorCount());
    }

    /** A single-abstract-method runnable whose body may throw, so cache calls can run off the test thread. */
    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code body} on a fresh thread and rethrows anything it threw, so each logical thread is distinct. */
    private static void runOnThread(ThrowingRunnable body) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        thread.start();
        thread.join();
        Throwable thrown = failure.get();
        if (thrown instanceof Exception exception) {
            throw exception;
        } else if (thrown != null) {
            throw new AssertionError(thrown);
        }
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
