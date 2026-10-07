/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Request-scoped cache that reuses ONE native Parquet cursor per column while deriving {@code _source}.
 *
 * <p>Source derivation ({@code RootObjectMapper.deriveSource}, driven by the {@code SortedNumericDocValuesFetcher}
 * and {@code SortedSetDocValuesFetcher}) calls {@code getSortedNumericDocValues(field)} /
 * {@code getSortedSetDocValues(field)} on the {@link ParquetDocValuesLeafReader#perDocumentValuesReader()
 * per-document view} once per (hit, field), advances it to a single doc, reads it, and drops it. Each fresh
 * accessor opens a new native cursor, so a fetch of H hits over F fields holds H*F live cursors at once,
 * tripping the native cursor memory gate. This cache keeps one inner iterator per column alive and hands
 * every later (hit, field) call a thin per-call wrapper over it, so the peak is ONE cursor per column.
 *
 * <p><b>Lifetime.</b> Created per wrap next to the {@link CursorRegistry} and {@link #close() closed} by the
 * wrapping directory reader when the request ends. A numeric cursor is also recorded on the CursorRegistry
 * (a request-end backstop); closing it here on eviction and again there is idempotent through
 * {@code NativeHandle}. A keyword cursor is closed here explicitly rather than left to the iterator's Cleaner.
 *
 * <p><b>Peak across segments.</b> q24's hits can span many segments, and the fetch visits them in
 * global-docId order, so one segment's hits are all served before the next. When a column's cached entry
 * belongs to a different segment than the one now asking, the old cursor is closed eagerly and a new one is
 * opened, keeping the peak at one cursor per column across the whole request rather than one per
 * (segment, column). This is safe because the fetcher finished with the previous segment's iterator before
 * the next segment asks for the same column.
 *
 * <p><b>Thread confinement.</b> The cache binds to the first thread that uses it. A call from any other
 * thread bypasses the cache entirely - a fresh, uncached cursor, exactly as before this cache existed -
 * because concurrent segment search or a {@code top_hits} sub-fetch can derive source from a different
 * thread, and closing a cursor another thread is mid-read on would be a use-after-close.
 */
final class DerivedSourceCursorCache implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DerivedSourceCursorCache.class);

    /** An opened numeric iterator with the cursor backing it, so the cache can close that cursor early. */
    record OpenedNumeric(SortedNumericDocValues values, AutoCloseable cursor) {
    }

    /** An opened keyword iterator with the closeable that frees its lazily-opened binary cursor. */
    record OpenedKeyword(SortedDocValues values, AutoCloseable cursor) {
    }

    /** One cached column: which segment opened it, the cursor to close, and the inner iterator to reuse. */
    private static final class Entry {
        private final Object segmentKey;
        private final AutoCloseable cursor;
        private final SortedNumericDocValues numericInner;
        private final SortedDocValues keywordInner;

        private Entry(Object segmentKey, AutoCloseable cursor, SortedNumericDocValues numericInner, SortedDocValues keywordInner) {
            this.segmentKey = segmentKey;
            this.cursor = cursor;
            this.numericInner = numericInner;
            this.keywordInner = keywordInner;
        }
    }

    // Keyed per accessor type: a field name is one column of one type, so numeric and keyword never collide,
    // but keeping them apart keeps the stored inner's type unambiguous.
    private final Map<String, Entry> numericEntries = new HashMap<>();
    private final Map<String, Entry> keywordEntries = new HashMap<>();

    /** The thread this cache is confined to; set on first use. */
    private Thread owner;
    private boolean closed;

    /**
     * Serves {@code field}'s numeric values for one hit, reusing the segment's single cursor. On the first
     * call for a (segment, field) the {@code opener} opens a cursor; later calls reuse it; a call from a
     * different segment closes the old cursor first.
     */
    SortedNumericDocValues sortedNumeric(Object segmentKey, String field, CheckedSupplier<OpenedNumeric, IOException> opener)
        throws IOException {
        if (confinedToCaller() == false) {
            // Different thread: behave exactly as before this cache existed - fresh, uncached, closed only
            // by its CursorRegistry entry at request end. Closing it here could race the owning thread.
            return opener.get().values();
        }
        synchronized (this) {
            if (closed) {
                return opener.get().values();
            }
            Entry entry = numericEntries.get(field);
            if (entry != null && entry.segmentKey.equals(segmentKey) == false) {
                closeQuietly(entry.cursor);
                numericEntries.remove(field);
                entry = null;
            }
            if (entry == null) {
                OpenedNumeric opened = opener.get();
                entry = new Entry(segmentKey, opened.cursor(), opened.values(), null);
                numericEntries.put(field, entry);
            }
            return new ReusedSortedNumeric(entry.numericInner);
        }
    }

    /**
     * Serves {@code field}'s keyword value for one hit, reusing the segment's single cursor. Returns a
     * thin per-call {@link SortedSetDocValues} over the shared cached inner: {@code DocValues.singleton}
     * cannot be reused here because it rejects an inner iterator already advanced by a previous hit.
     */
    SortedSetDocValues keyword(Object segmentKey, String field, CheckedSupplier<OpenedKeyword, IOException> opener) throws IOException {
        if (confinedToCaller() == false) {
            return new ReusedSortedSet(opener.get().values());
        }
        synchronized (this) {
            if (closed) {
                return new ReusedSortedSet(opener.get().values());
            }
            Entry entry = keywordEntries.get(field);
            if (entry != null && entry.segmentKey.equals(segmentKey) == false) {
                closeQuietly(entry.cursor);
                keywordEntries.remove(field);
                entry = null;
            }
            if (entry == null) {
                OpenedKeyword opened = opener.get();
                entry = new Entry(segmentKey, opened.cursor(), null, opened.values());
                keywordEntries.put(field, entry);
            }
            return new ReusedSortedSet(entry.keywordInner);
        }
    }

    /** Binds the cache to the first calling thread and reports whether this call is that thread. */
    private synchronized boolean confinedToCaller() {
        Thread caller = Thread.currentThread();
        if (owner == null) {
            owner = caller;
        }
        return owner == caller;
    }

    @Override
    public synchronized void close() {
        closed = true;
        for (Entry entry : numericEntries.values()) {
            closeQuietly(entry.cursor);
        }
        numericEntries.clear();
        for (Entry entry : keywordEntries.values()) {
            closeQuietly(entry.cursor);
        }
        keywordEntries.clear();
    }

    private static void closeQuietly(AutoCloseable cursor) {
        try {
            cursor.close();
        } catch (Exception e) {
            LOGGER.warn("failed to close reused parquet derived-source cursor", e);
        }
    }

    /** Live cached cursors (tests): one per column currently reused on the owning thread. */
    synchronized int liveCursorCount() {
        return numericEntries.size() + keywordEntries.size();
    }

    /**
     * Thin per-call view over a cached inner {@link SortedNumericDocValues}. Each (hit, field) call gets a
     * fresh instance, so no read state leaks between hits; every read delegates to the shared inner.
     * {@code advanceExact} delegates straight through: the inner's native cursor resets itself on a
     * backward target ({@code ParquetColumnReader.loadBatchContaining}), so revisiting an earlier doc
     * reuses the one cursor rather than opening another, and {@code docValueCount}/{@code nextValue}
     * restart because the inner repositions on each {@code advanceExact} (multi-valued columns included).
     */
    private static final class ReusedSortedNumeric extends SortedNumericDocValues {
        private final SortedNumericDocValues inner;

        private ReusedSortedNumeric(SortedNumericDocValues inner) {
            this.inner = inner;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return inner.advanceExact(target);
        }

        @Override
        public long nextValue() throws IOException {
            return inner.nextValue();
        }

        @Override
        public int docValueCount() {
            return inner.docValueCount();
        }

        @Override
        public int docID() {
            return inner.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return inner.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return inner.advance(target);
        }

        @Override
        public long cost() {
            return inner.cost();
        }
    }

    /**
     * Thin per-call single-valued {@link SortedSetDocValues} view over a cached {@link SortedDocValues}
     * inner, mirroring {@code SingletonSortedSetDocValues} but without its fresh-iterator assertion so the
     * shared inner can be reused across hits. The derived-source fetcher only advances and reads one value
     * per doc, so the single ordinal the inner reports is served directly.
     */
    private static final class ReusedSortedSet extends SortedSetDocValues {
        private final SortedDocValues inner;

        private ReusedSortedSet(SortedDocValues inner) {
            this.inner = inner;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return inner.advanceExact(target);
        }

        @Override
        public long nextOrd() throws IOException {
            return inner.ordValue();
        }

        @Override
        public int docValueCount() {
            return 1;
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return inner.lookupOrd((int) ord);
        }

        @Override
        public long getValueCount() {
            return inner.getValueCount();
        }

        @Override
        public int docID() {
            return inner.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return inner.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return inner.advance(target);
        }

        @Override
        public long cost() {
            return inner.cost();
        }
    }
}
