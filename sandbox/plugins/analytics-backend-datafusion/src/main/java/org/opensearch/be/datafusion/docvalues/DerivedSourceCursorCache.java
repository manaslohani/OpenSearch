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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Request-scoped cache that reuses ONE native Parquet cursor per column (per deriving thread) while
 * deriving {@code _source}.
 *
 * <p>Source derivation ({@code RootObjectMapper.deriveSource}, driven by the {@code SortedNumericDocValuesFetcher}
 * and {@code SortedSetDocValuesFetcher}) calls {@code getSortedNumericDocValues(field)} /
 * {@code getSortedSetDocValues(field)} on the {@link ParquetDocValuesLeafReader#perDocumentValuesReader()
 * per-document view} once per (hit, field), advances it to a single doc, reads it, and drops it. Each fresh
 * accessor opens a new native cursor, so a fetch of H hits over F fields holds H*F live cursors at once,
 * tripping the native cursor memory gate. This cache keeps one inner iterator per column alive and hands
 * every later (hit, field) call a thin per-call wrapper over it, so the peak is ONE cursor per column per
 * deriving thread (see Thread confinement).
 *
 * <p><b>Lifetime.</b> Created per wrap next to the {@link CursorRegistry} and {@link #close() closed} by the
 * wrapping directory reader when the request ends. A numeric cursor is also recorded on the CursorRegistry
 * (a request-end backstop); closing it here on eviction and again there is idempotent through
 * {@code NativeHandle}. A keyword cursor is closed here explicitly rather than left to the iterator's Cleaner.
 *
 * <p><b>Peak across segments.</b> q24's hits can span many segments, and the fetch visits them in
 * global-docId order, so one segment's hits are all served before the next. When a column's cached entry
 * belongs to a different segment than the one now asking, the old cursor is closed eagerly and a new one is
 * opened, keeping the peak at one cursor per column (within a thread) across the whole request rather than
 * one per (segment, column). This is safe because the fetcher finished with the previous segment's iterator
 * before the next segment asks for the same column.
 *
 * <p><b>Thread confinement.</b> Scroll and PIT keep one wrapped {@code DirectoryReader} - hence one cache -
 * across many fetch pages, and consecutive pages can run on different search threads; a {@code top_hits}
 * sub-fetch or concurrent segment search likewise derives source from several threads. Rather than bind the
 * cache to the first thread and send every other thread down an uncached path (which would reopen one cursor
 * per (hit, field) for all those pages), each thread gets its own private {@link ThreadState} of column
 * entries, keyed by {@link Thread} in a {@link ConcurrentHashMap}. A thread only ever reads, evicts, or
 * closes its own entries while deriving, so no thread can close a cursor another is mid-read on and the hot
 * path needs no locking. Peak live cursors are therefore columns x distinct deriving threads (bounded by the
 * search thread pool), not hits x columns. {@link #close()} at reader end closes every thread's entries; it
 * runs after all derivation has finished, so it never races a live read, and dropping the map releases the
 * {@link Thread} references it held - they never outlive the reader.
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

    /**
     * One deriving thread's private slice of the cache: its own field-&gt;entry maps, touched only by that
     * thread while in use, so two threads never read or evict the same entry. Numeric and keyword are kept
     * apart so the stored inner's type stays unambiguous (a field name is one column of one type).
     */
    private static final class ThreadState {
        private final Map<String, Entry> numericEntries = new HashMap<>();
        private final Map<String, Entry> keywordEntries = new HashMap<>();
    }

    // Thread -> that thread's own entries. The ConcurrentHashMap guards only the thread->state mapping;
    // each state's inner maps are single-thread-owned during use and so need no locking.
    private final Map<Thread, ThreadState> threadStates = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /**
     * Serves {@code field}'s numeric values for one hit, reusing the calling thread's single cursor for the
     * segment. On the first call for a (thread, segment, field) the {@code opener} opens a cursor; later
     * calls on that thread reuse it; a call from a different segment closes the thread's old cursor first.
     */
    SortedNumericDocValues sortedNumeric(Object segmentKey, String field, CheckedSupplier<OpenedNumeric, IOException> opener)
        throws IOException {
        if (closed) {
            // Reader already retired (no live derivation left): open fresh and uncached; closing it here
            // could race nothing, but there is nowhere to cache it either. The CursorRegistry still frees it.
            return opener.get().values();
        }
        ThreadState state = threadStates.computeIfAbsent(Thread.currentThread(), t -> new ThreadState());
        Entry entry = state.numericEntries.get(field);
        if (entry != null && entry.segmentKey.equals(segmentKey) == false) {
            closeQuietly(entry.cursor);
            state.numericEntries.remove(field);
            entry = null;
        }
        if (entry == null) {
            OpenedNumeric opened = opener.get();
            entry = new Entry(segmentKey, opened.cursor(), opened.values(), null);
            state.numericEntries.put(field, entry);
        }
        return new ReusedSortedNumeric(entry.numericInner);
    }

    /**
     * Serves {@code field}'s keyword value for one hit, reusing the calling thread's single cursor for the
     * segment. Returns a thin per-call {@link SortedSetDocValues} over the shared cached inner:
     * {@code DocValues.singleton} cannot be reused here because it rejects an inner iterator already advanced
     * by a previous hit.
     */
    SortedSetDocValues keyword(Object segmentKey, String field, CheckedSupplier<OpenedKeyword, IOException> opener) throws IOException {
        if (closed) {
            return new ReusedSortedSet(opener.get().values());
        }
        ThreadState state = threadStates.computeIfAbsent(Thread.currentThread(), t -> new ThreadState());
        Entry entry = state.keywordEntries.get(field);
        if (entry != null && entry.segmentKey.equals(segmentKey) == false) {
            closeQuietly(entry.cursor);
            state.keywordEntries.remove(field);
            entry = null;
        }
        if (entry == null) {
            OpenedKeyword opened = opener.get();
            entry = new Entry(segmentKey, opened.cursor(), null, opened.values());
            state.keywordEntries.put(field, entry);
        }
        return new ReusedSortedSet(entry.keywordInner);
    }

    @Override
    public void close() {
        // Runs at reader end, after all derivation has finished, so no thread is still touching its state.
        closed = true;
        for (ThreadState state : threadStates.values()) {
            for (Entry entry : state.numericEntries.values()) {
                closeQuietly(entry.cursor);
            }
            state.numericEntries.clear();
            for (Entry entry : state.keywordEntries.values()) {
                closeQuietly(entry.cursor);
            }
            state.keywordEntries.clear();
        }
        threadStates.clear();
    }

    private static void closeQuietly(AutoCloseable cursor) {
        try {
            cursor.close();
        } catch (Exception e) {
            LOGGER.warn("failed to close reused parquet derived-source cursor", e);
        }
    }

    /** Live cached cursors on the calling thread (tests): one per column it is currently reusing. */
    int liveCursorCount() {
        ThreadState state = threadStates.get(Thread.currentThread());
        return state == null ? 0 : state.numericEntries.size() + state.keywordEntries.size();
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
            // Delegate verbatim to keep the inner's own contract: a streaming Parquet keyword inner
            // (ParquetSortedDocValues) deliberately throws UnsupportedOperationException here because it
            // materializes no segment-global ordinals, steering ordinal-comparing consumers to
            // execution_hint:map. The derived-source fetcher never calls this, so propagating is correct;
            // overriding it to a fabricated count would hide that contract from any other caller.
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
