/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A per-segment sorted term dictionary for one keyword field, loaded from the composite index's
 * Lucene sidecar — the inverted index already stores every distinct term in sorted order, so
 * reading it costs O(distinct terms) and never scans rows.
 *
 * <p>Backs dictionary-rank ordinals: a document's ordinal is computed on access by binary
 * search of its value against this dictionary, giving fully contract-compliant
 * segment-global ordinals for fields whose cardinality fits the configured budget. Fields
 * above the budget (or whose term count is unknown) are not eligible and stay on the
 * streaming fail-fast path.
 *
 * <p>Instances are immutable and cached per (segment core key, field) — see
 * {@link TermDictionaryCache}.
 */
public final class TermDictionary {

    private final BytesRef[] terms;
    private final long sizeInBytes;

    private TermDictionary(BytesRef[] terms, long sizeInBytes) {
        this.terms = terms;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * Loads the sorted dictionary, or returns {@code null} when the field is not eligible:
     * no terms, unknown term count, or cardinality above {@code maxTerms}.
     */
    public static TermDictionary load(Terms terms, int maxTerms) throws IOException {
        if (terms == null) {
            return null;
        }
        long size = terms.size();
        if (size < 0 || size > maxTerms) {
            return null;
        }
        List<BytesRef> collected = new ArrayList<>((int) size);
        long bytes = 0;
        TermsEnum termsEnum = terms.iterator();
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            BytesRef copy = BytesRef.deepCopyOf(term);
            collected.add(copy);
            bytes += copy.length + 32; // value bytes + object/array overhead estimate
        }
        return new TermDictionary(collected.toArray(new BytesRef[0]), bytes);
    }

    /** Cache sentinel marking a field as checked-and-ineligible. */
    static TermDictionary sentinel() {
        return new TermDictionary(new BytesRef[0], 0);
    }

    /** Number of distinct terms. */
    public int size() {
        return terms.length;
    }

    /** Estimated heap footprint, for cache accounting. */
    public long sizeInBytes() {
        return sizeInBytes;
    }

    /** The term for a segment ordinal. */
    public BytesRef term(int ord) {
        return terms[ord];
    }

    /**
     * The segment ordinal of {@code value}, or {@code -insertionPoint - 1} when absent
     * (the {@code lookupTerm} contract).
     */
    public int rank(BytesRef value) {
        int low = 0;
        int high = terms.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = terms[mid].compareTo(value);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }
}
