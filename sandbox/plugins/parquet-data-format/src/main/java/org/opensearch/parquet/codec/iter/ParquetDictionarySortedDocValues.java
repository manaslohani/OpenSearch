/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.codec.TermDictionary;

import java.io.IOException;

/**
 * Fully contract-compliant {@link SortedDocValues} for keyword fields whose cardinality fits
 * the dictionary budget: segment-global ordinals are computed on access as the rank of the
 * document's value in the segment's sorted {@link TermDictionary} (loaded O(distinct) from the
 * composite index's Lucene sidecar — never from a row scan).
 *
 * <p>Values come from the streaming iterator's zero-copy page path; the only added per-document
 * cost is one binary search over the heap-resident dictionary. All global operations —
 * cross-document ordinal comparison (sorting), {@link #getValueCount()} (cardinality,
 * composite, global-ordinals terms aggregations), {@link #lookupTerm} — are exact.
 */
public final class ParquetDictionarySortedDocValues extends SortedDocValues {

    private final ParquetSortedDocValues stream;
    private final TermDictionary dictionary;

    private int currentOrd = -1;

    public ParquetDictionarySortedDocValues(ParquetSortedDocValues stream, TermDictionary dictionary) {
        this.stream = stream;
        this.dictionary = dictionary;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (stream.advanceExact(target) == false) {
            currentOrd = -1;
            return false;
        }
        computeOrd();
        return true;
    }

    /**
     * Ranks the positioned document's value. The streaming iterator's ord is transient and
     * immediately resolved — the exact access pattern it supports; the dictionary rank is the
     * real segment ordinal.
     */
    private void computeOrd() {
        BytesRef value = stream.lookupOrd(stream.ordValue());
        int ord = dictionary.rank(value);
        if (ord < 0) {
            throw new IllegalStateException(
                "value ["
                    + value.utf8ToString()
                    + "] present in doc values but absent from the field's terms index; "
                    + "dictionary ordinals require every stored value to be indexed (no ignore_above)"
            );
        }
        currentOrd = ord;
    }

    @Override
    public int ordValue() {
        return currentOrd;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
        return dictionary.term(ord);
    }

    @Override
    public int getValueCount() {
        return dictionary.size();
    }

    @Override
    public int lookupTerm(BytesRef key) {
        return dictionary.rank(key);
    }

    @Override
    public int docID() {
        return stream.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        int doc = stream.nextDoc();
        if (doc != NO_MORE_DOCS) {
            computeOrd();
        }
        return doc;
    }

    @Override
    public int advance(int target) throws IOException {
        int doc = stream.advance(target);
        if (doc != NO_MORE_DOCS) {
            computeOrd();
        }
        return doc;
    }

    @Override
    public long cost() {
        return stream.cost();
    }
}
