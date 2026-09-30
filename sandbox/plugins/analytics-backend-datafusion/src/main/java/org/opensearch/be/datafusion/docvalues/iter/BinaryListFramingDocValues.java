/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListValueReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * {@link BinaryDocValues} over a repeated Parquet {@code BYTE_ARRAY} column, one Parquet row per
 * document, framed as OpenSearch {@code binary} multi-valued doc values.
 *
 * <p>{@code BinaryFieldMapper.CustomBinaryDocValuesField.binaryValue()} sorts a document's values
 * UNSIGNED and dedups them, then writes {@code vInt(count) [vInt(length) bytes]*}. This iterator
 * reproduces that exactly at read time: for each document it collects the child byte slices of the
 * row's list, sorts them with {@link Arrays#compareUnsigned} and drops adjacent duplicates, then
 * frames them into one buffer. An empty list, an all-null list, or a list of only null elements
 * carries no value, reported as {@code advanceExact == false} - the same absence a
 * {@code FieldExistsQuery} sees for an unset field.
 *
 * <p>The single-valued sibling is {@link BinaryFramingDocValues}, which frames one delegate value as
 * {@code count == 1}; this class frames {@code count} values from a list child and owns the
 * sort/dedup the scalar path never needs.
 */
public final class BinaryListFramingDocValues extends BinaryDocValues {

    /** Widest vInt encoding of a non-negative int, i.e. the most bytes a length prefix can take. */
    private static final int MAX_VINT_BYTES = 5;

    private final ListValueReader reader;
    private final int maxDoc;

    private int doc = -1;

    /** Reused per-document collection of a row's non-null child byte slices; only {@code [0, count)} is live. */
    private byte[][] valuesScratch = new byte[8][];
    private int count;

    /** Reused framing buffer, grown as needed; the framed {@link BytesRef} points into it. */
    private byte[] buffer = new byte[0];
    private final BytesRef framed = new BytesRef();
    /**
     * Whether {@link #framed} holds the frame for the current doc. Framing is lazy so a presence-only
     * consumer (a {@code FieldExistsQuery}) that never asks for the value pays neither the copy nor the
     * sort: the frame is built on the first {@link #binaryValue()} after a positive advance.
     */
    private boolean framedValid;

    public BinaryListFramingDocValues(ListValueReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        framedValid = false;
        count = 0;
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        DecodedListBatch batch = reader.decodedListBatch();
        if (batch == null || batch.contains(target) == false) {
            reader.loadListBatchContaining(target);
            batch = reader.decodedListBatch();
        }
        if (batch == null || batch.contains(target) == false) {
            throw new IllegalStateException("list batch does not contain target doc " + target);
        }
        int start = batch.startOffset(target);
        int end = batch.endOffset(target);
        DecodedBatch children = batch.childValues();

        ensureCapacity(end - start);
        int n = 0;
        BytesRef slice = new BytesRef();
        for (int child = start; child < end; child++) {
            // Null elements carry no value, exactly as a null in the ingest array is dropped before framing.
            if (children.isPresent(child)) {
                children.bytesAt(child, slice);
                // bytesAt writes into a buffer it reuses across calls, so each element must be copied out
                // before the next bytesAt overwrites it.
                valuesScratch[n++] = ArrayUtil.copyOfSubArray(slice.bytes, slice.offset, slice.offset + slice.length);
            }
        }
        count = n;
        if (count == 0) {
            // Empty list, all-null list, or a list of only null elements: no value, like an unset field.
            return false;
        }
        return true;
    }

    @Override
    public BytesRef binaryValue() {
        if (framedValid == false) {
            frame();
            framedValid = true;
        }
        return framed;
    }

    /**
     * Sorts {@link #valuesScratch}{@code [0, count)} unsigned, drops adjacent duplicates, and writes the
     * survivors into {@link #buffer} as {@code vInt(uniqueCount) [vInt(length) bytes]*}, repointing
     * {@link #framed} at it. Byte-identical to {@code CustomBinaryDocValuesField.binaryValue()} for the
     * same set of values.
     */
    private void frame() {
        // sortAndDedup, unsigned: the exact order and dedup CustomBinaryDocValuesField applies.
        Arrays.sort(valuesScratch, 0, count, Arrays::compareUnsigned);
        int unique = dedup();

        int bytesTotal = 0;
        for (int i = 0; i < unique; i++) {
            bytesTotal += valuesScratch[i].length;
        }
        // count header + one length header per value + the value bytes.
        int upperBound = MAX_VINT_BYTES + unique * MAX_VINT_BYTES + bytesTotal;
        if (buffer.length < upperBound) {
            buffer = ArrayUtil.grow(buffer, upperBound);
        }
        int pos = writeVInt(buffer, 0, unique);
        for (int i = 0; i < unique; i++) {
            byte[] value = valuesScratch[i];
            pos = writeVInt(buffer, pos, value.length);
            System.arraycopy(value, 0, buffer, pos, value.length);
            pos += value.length;
        }
        framed.bytes = buffer;
        framed.offset = 0;
        framed.length = pos;
    }

    /** Drops adjacent duplicates in the already-sorted {@code [0, count)} prefix; returns the unique count. */
    private int dedup() {
        int unique = 0;
        for (int i = 0; i < count; i++) {
            if (unique == 0 || Arrays.compareUnsigned(valuesScratch[unique - 1], valuesScratch[i]) != 0) {
                valuesScratch[unique++] = valuesScratch[i];
            }
        }
        return unique;
    }

    /**
     * Writes {@code value} at {@code pos} in the variable length encoding {@code StreamOutput#writeVInt}
     * uses - seven bits per byte, least significant group first, high bit set on every byte but the last -
     * and returns the position just past it.
     */
    private static int writeVInt(byte[] dst, int pos, int value) {
        while ((value & ~0x7F) != 0) {
            dst[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        dst[pos++] = (byte) value;
        return pos;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    private void ensureCapacity(int needed) {
        if (needed > valuesScratch.length) {
            valuesScratch = new byte[Math.max(needed, valuesScratch.length * 2)][];
        }
    }

    /** Unsigned {@code byte[]} order, extracted so the sort and the dedup share one comparison rule. */
    private static final class Comparator {
        static java.util.Comparator<byte[]> wrap() {
            return Arrays::compareUnsigned;
        }
    }
}
