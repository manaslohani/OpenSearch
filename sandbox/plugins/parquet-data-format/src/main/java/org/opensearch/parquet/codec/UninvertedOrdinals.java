/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.PackedInts;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Segment-global ordinals for a high-cardinality keyword field, uninverted once from the Lucene
 * sidecar's postings and spilled to a memory-mapped node-local file — read-side only, with
 * Lucene's own storage economics: the packed doc→ord array lives on disk and only touched pages
 * are resident.
 *
 * <h2>Build</h2>
 * One sequential sweep of the field's terms (already sorted on disk) and their postings assigns
 * each document its term's rank. The transient in-heap packed buffer is released after the spill;
 * builds are serialized node-wide and check the cancellation flag between terms. Deleted
 * documents keep their ordinals (collectors never visit them), matching Lucene's own doc-values
 * semantics until merge.
 *
 * <h2>Read</h2>
 * {@code ordinal(doc)} is one packed read from the mapped file (0 = missing; stored values are
 * ord + 1). {@code lookupOrd} uses sparse in-heap checkpoints (every {@value #CHECKPOINT_INTERVAL}
 * terms) plus a bounded {@code TermsEnum} advance — only final buckets and sort bounds resolve
 * terms, never per-document access.
 */
public final class UninvertedOrdinals implements Closeable {

    static final int CHECKPOINT_INTERVAL = 1024;
    private static final String CODEC_PREFIX = "parquet-ords";

    private final Directory directory;
    private final IndexInput input;
    private final LongValues ords;
    private final BytesRef[] checkpoints;
    private final Terms terms;
    private final int valueCount;
    private final long sizeInBytes;
    private final String fileName;

    private UninvertedOrdinals(
        Directory directory,
        IndexInput input,
        LongValues ords,
        BytesRef[] checkpoints,
        Terms terms,
        int valueCount,
        long sizeInBytes,
        String fileName
    ) {
        this.directory = directory;
        this.input = input;
        this.ords = ords;
        this.checkpoints = checkpoints;
        this.terms = terms;
        this.valueCount = valueCount;
        this.sizeInBytes = sizeInBytes;
        this.fileName = fileName;
    }

    /**
     * Builds (or maps an existing) ordinal file for the field. {@code cancelled} is polled
     * between terms during the sweep so runaway builds die with their task.
     */
    static UninvertedOrdinals build(
        Path ordsDir,
        String fileKey,
        Terms terms,
        int maxDoc,
        long expectedNonNullDocs,
        java.util.function.BooleanSupplier cancelled
    ) throws IOException {
        if (expectedNonNullDocs < 0) {
            throw new IllegalStateException(
                "cannot verify ordinal coverage (column null statistics unavailable); refusing to "
                    + "serve postings-derived ordinals that may silently drop unindexed values"
            );
        }
        long termCount = terms.size();
        if (termCount < 0) {
            throw new IllegalStateException("terms index reports unknown size; cannot uninvert");
        }
        Files.createDirectories(ordsDir);
        Directory directory = new MMapDirectory(ordsDir);
        String fileName = CODEC_PREFIX + "-" + fileKey + ".ord";
        // +1 shifted encoding (0 = missing). DirectWriter supports only specific widths;
        // its bitsRequired rounds up to the nearest supported one.
        int bits = DirectWriter.bitsRequired(termCount + 1);
        List<BytesRef> checkpoints = new ArrayList<>((int) (termCount / CHECKPOINT_INTERVAL) + 1);

        boolean exists;
        try {
            directory.fileLength(fileName);
            exists = true;
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            exists = false;
        }

        if (exists == false) {
            // Sweep: heap-transient packed buffer (released after spill), then one sequential write.
            PackedInts.Mutable building = PackedInts.getMutable(maxDoc, bits, PackedInts.COMPACT);
            TermsEnum termsEnum = terms.iterator();
            PostingsEnum postings = null;
            long ord = 0;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next(), ord++) {
                if ((ord & (CHECKPOINT_INTERVAL - 1)) == 0) {
                    if (cancelled.getAsBoolean()) {
                        throw new IOException("ordinal build cancelled for " + fileKey);
                    }
                }
                postings = termsEnum.postings(postings, PostingsEnum.NONE);
                for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                    building.set(doc, ord + 1);
                }
            }
            String tempName = fileName + ".tmp";
            try {
                directory.deleteFile(tempName); // stale leftover from an interrupted build
            } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                // normal case
            }
            try (IndexOutput out = directory.createOutput(tempName, IOContext.DEFAULT)) {
                DirectWriter writer = DirectWriter.getInstance(out, maxDoc, bits);
                for (int doc = 0; doc < maxDoc; doc++) {
                    writer.add(building.get(doc));
                }
                writer.finish();
            }
            directory.rename(tempName, fileName);
        }

        // Checkpoints are cheap relative to the sweep; collect them on every load.
        TermsEnum termsEnum = terms.iterator();
        long ord = 0;
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next(), ord++) {
            if ((ord % CHECKPOINT_INTERVAL) == 0) {
                checkpoints.add(BytesRef.deepCopyOf(term));
            }
        }

        IndexInput input = directory.openInput(fileName, IOContext.DEFAULT);
        LongValues ords = DirectReader.getInstance(input.randomAccessSlice(0, input.length()), bits);
        long size = directory.fileLength(fileName);
        // Coverage verification: postings only contain INDEXED values. A stored value that was
        // never indexed (ignore_above truncation, analyzer drops) would silently become
        // "missing" and undercount every aggregation on this field. Count assigned ordinals and
        // require exact agreement with the Parquet column's non-null row count; refuse loudly
        // otherwise. One sequential mapped read per segment load (~100ms per 100M rows).
        long assigned = 0;
        for (int doc = 0; doc < maxDoc; doc++) {
            if (ords.get(doc) != 0) {
                assigned++;
            }
        }
        if (assigned != expectedNonNullDocs) {
            input.close();
            directory.close();
            throw new IllegalStateException(
                "ordinal coverage mismatch for "
                    + fileKey
                    + ": postings assign "
                    + assigned
                    + " documents but the column stores "
                    + expectedNonNullDocs
                    + " non-null values — some stored values are not indexed (ignore_above?); "
                    + "refusing uninverted ordinals to avoid silent undercounts"
            );
        }
        return new UninvertedOrdinals(
            directory,
            input,
            ords,
            checkpoints.toArray(new BytesRef[0]),
            terms,
            (int) termCount,
            size,
            fileName
        );
    }

    /** The segment ordinal for {@code doc}, or -1 when the document has no value. */
    public int ordinal(int doc) {
        return (int) ords.get(doc) - 1;
    }

    /** Number of distinct terms. */
    public int valueCount() {
        return valueCount;
    }

    /** On-disk footprint (cache accounting). */
    public long sizeInBytes() {
        return sizeInBytes;
    }

    /** The ord file's name within the ords directory (disk-budget pinning). */
    public String fileName() {
        return fileName;
    }

    /**
     * The field's real terms enumeration — the exact sorted term space these ordinals rank —
     * wrapped with ordinal tracking, because consumers like {@code OrdinalMap} require
     * {@link TermsEnum#ord()} which BlockTree does not implement. Ord seeks use the sparse
     * checkpoints; byte seeks re-derive the position via {@link #rank}.
     */
    public TermsEnum termsEnum() throws IOException {
        return new OrdTrackingTermsEnum(terms.iterator());
    }

    private final class OrdTrackingTermsEnum extends org.apache.lucene.index.FilterLeafReader.FilterTermsEnum {
        private long position = -1;

        OrdTrackingTermsEnum(TermsEnum in) {
            super(in);
        }

        @Override
        public BytesRef next() throws IOException {
            BytesRef term = in.next();
            if (term != null) {
                position++;
            } else {
                position = valueCount;
            }
            return term;
        }

        @Override
        public long ord() {
            return position;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            int checkpoint = (int) (ord / CHECKPOINT_INTERVAL);
            in.seekCeil(checkpoints[checkpoint]);
            position = (long) checkpoint * CHECKPOINT_INTERVAL;
            while (position < ord) {
                in.next();
                position++;
            }
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            boolean found = in.seekExact(text);
            position = found ? rank(text) : -1;
            return found;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            SeekStatus status = in.seekCeil(text);
            if (status == SeekStatus.END) {
                position = valueCount;
            } else {
                int r = rank(in.term());
                position = r >= 0 ? r : -(r + 1);
            }
            return status;
        }
    }

    /** Resolves an ordinal to its term: checkpoint seek plus a bounded enum advance. */
    public BytesRef term(int ord) {
        try {
            TermsEnum termsEnum = terms.iterator();
            int checkpoint = ord / CHECKPOINT_INTERVAL;
            termsEnum.seekCeil(checkpoints[checkpoint]);
            for (int i = checkpoint * CHECKPOINT_INTERVAL; i < ord; i++) {
                termsEnum.next();
            }
            return BytesRef.deepCopyOf(termsEnum.term());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A single-consumer stateful term resolver: ascending ordinal walks cost one enum pass. */
    public TermCursor newTermCursor() {
        return new TermCursor();
    }

    /**
     * Stateful ord→term resolution for one consumer (not thread-safe, like doc-values
     * iterators). A stateless resolver pays a checkpoint seek plus up to
     * {@value #CHECKPOINT_INTERVAL} enum steps on EVERY call, which turns full-column walks
     * quadratic; this cursor advances forward from its last position when the requested
     * ordinal is ahead, so monotonic access — bucket resolution, ordinal-map style walks —
     * amortizes to a single sequential pass over the terms file.
     */
    public final class TermCursor {
        private TermsEnum cursorEnum;
        private long cursorOrd = -1;

        public BytesRef term(int ord) {
            try {
                long behind = cursorEnum == null ? Long.MAX_VALUE : ord - cursorOrd;
                if (behind < 0 || behind > CHECKPOINT_INTERVAL) {
                    // Behind us, or far ahead: re-seek to the nearest checkpoint.
                    cursorEnum = terms.iterator();
                    int checkpoint = ord / CHECKPOINT_INTERVAL;
                    cursorEnum.seekCeil(checkpoints[checkpoint]);
                    cursorOrd = (long) checkpoint * CHECKPOINT_INTERVAL;
                }
                while (cursorOrd < ord) {
                    cursorEnum.next();
                    cursorOrd++;
                }
                return cursorEnum.term();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The ordinal of {@code key}, or {@code -insertionPoint - 1} (the lookupTerm contract). */
    public int rank(BytesRef key) {
        try {
            // Binary search over checkpoints, then a bounded linear scan with the enum.
            int low = 0;
            int high = checkpoints.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = checkpoints[mid].compareTo(key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid * CHECKPOINT_INTERVAL;
                }
            }
            int checkpoint = Math.max(low - 1, 0);
            TermsEnum termsEnum = terms.iterator();
            termsEnum.seekCeil(checkpoints[checkpoint]);
            int ord = checkpoint * CHECKPOINT_INTERVAL;
            BytesRef term = termsEnum.term();
            while (term != null) {
                int cmp = term.compareTo(key);
                if (cmp == 0) {
                    return ord;
                }
                if (cmp > 0) {
                    return -(ord + 1);
                }
                term = termsEnum.next();
                ord++;
            }
            return -(ord + 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        input.close();
        directory.close();
    }
}
