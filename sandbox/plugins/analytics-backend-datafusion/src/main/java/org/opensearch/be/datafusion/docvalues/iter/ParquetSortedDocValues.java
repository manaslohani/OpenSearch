/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.BinaryValueReader;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBinaryBatch;
import org.opensearch.common.CheckedSupplier;

import java.io.IOException;
import java.lang.ref.Cleaner;

/**
 * Streaming single-valued {@link SortedDocValues} over a Parquet keyword/ip column: per-document
 * values only, no segment-wide ordinal structure. The doc id doubles as a transient ordinal,
 * resolved immediately via {@link #lookupOrd}; segment-global operations ({@link #getValueCount},
 * {@link #lookupTerm}) throw rather than return wrong results, steering ordinal-comparing
 * consumers to {@code execution_hint: map}.
 *
 * <p>The value reader is built lazily on the first value request, so a consumer that never reads
 * values (an ordinal-only aggregation) never opens a native cursor.
 */
public final class ParquetSortedDocValues extends SortedDocValues {

    private static final Logger LOGGER = LogManager.getLogger(ParquetSortedDocValues.class);

    /** Closes the value reader once this iterator is unreachable, so the native cursor is not held until the segment retires. */
    private static final Cleaner CLEANER = Cleaner.create();

    /** Recipe for the value reader; runs on the first value request only. */
    private final CheckedSupplier<BinaryValueReader, IOException> readerFactory;
    private final int maxDoc;

    /** Reusable per-row value copy; the batch's buffers are off-heap and BytesRef needs a heap array. */
    private final BytesRef scratchRef = new BytesRef(BytesRef.EMPTY_BYTES);
    private byte[] scratch = BytesRef.EMPTY_BYTES;

    /** Built lazily by {@link #reader()}; null until the first value request. */
    private BinaryValueReader reader;

    private int doc = -1;
    private boolean currentPresent;

    public ParquetSortedDocValues(CheckedSupplier<BinaryValueReader, IOException> readerFactory, int maxDoc) {
        this.readerFactory = readerFactory;
        this.maxDoc = maxDoc;
    }

    private BinaryValueReader reader() throws IOException {
        if (reader == null) {
            BinaryValueReader opened = readerFactory.get();
            // Capture only the reader: capturing this iterator would keep it reachable, which is why
            // NativeHandle's own cleaner never fires.
            if (opened instanceof AutoCloseable closeable) {
                CLEANER.register(this, () -> closeCursorAndLogFailure(closeable));
            }
            reader = opened;
        }
        return reader;
    }

    /** Closes the value cursor on the cleaner thread, where throwing would kill the thread silently. */
    private static void closeCursorAndLogFailure(AutoCloseable cursor) {
        try {
            cursor.close();
        } catch (Exception e) {
            LOGGER.warn("failed to close parquet value cursor", e);
        }
    }

    /**
     * Closes the lazily-opened value cursor now, for request-scoped reuse ({@code DerivedSourceCursorCache})
     * that frees cursors on eviction and at request end instead of waiting for the Cleaner. Idempotent: the
     * underlying cursor is a {@code NativeHandle} whose close is a no-op once closed, so the registered
     * Cleaner freeing it later is safe (no double free). A cursor never opened has nothing to close.
     */
    public void close() {
        if (reader instanceof AutoCloseable closeable) {
            closeCursorAndLogFailure(closeable);
        }
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return false;
        }
        doc = target;
        BinaryValueReader valueReader = reader();
        DecodedBinaryBatch batch = valueReader.decodedBinaryBatch();
        if (batch == null || batch.contains(target) == false) {
            valueReader.loadBatchContaining(target);
            batch = valueReader.decodedBinaryBatch();
        }
        currentPresent = batch.isPresent(target);
        if (currentPresent) {
            int length = batch.valueLength(target);
            if (scratch.length < length) {
                scratch = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
            }
            batch.copyValue(target, scratch);
            scratchRef.bytes = scratch;
            scratchRef.offset = 0;
            scratchRef.length = length;
        }
        return currentPresent;
    }

    @Override
    public int ordValue() {
        // The doc id doubles as the transient ordinal; lookupOrd verifies it.
        return doc;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
        if (ord != doc || currentPresent == false) {
            throw new UnsupportedOperationException(
                "ordinal "
                    + ord
                    + " was issued for another document (current doc "
                    + doc
                    + "): composite Parquet keyword fields serve per-document streaming ordinals "
                    + "only; consumers requiring segment-global ordinals must use execution_hint:map"
            );
        }
        return scratchRef;
    }

    @Override
    public int getValueCount() {
        throw new UnsupportedOperationException(
            "getValueCount requires segment-global ordinals, which composite Parquet keyword "
                + "fields do not materialize at read time; aggregations on these fields must use "
                + "execution_hint:map"
        );
    }

    @Override
    public int lookupTerm(BytesRef key) {
        throw new UnsupportedOperationException(
            "lookupTerm requires segment-global ordinals, which composite Parquet keyword "
                + "fields do not materialize at read time; aggregations on these fields must use "
                + "execution_hint:map"
        );
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        currentPresent = false;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
