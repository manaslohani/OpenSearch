/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetUninvertedSortedDocValues;
import org.opensearch.common.lucene.index.PerDocumentValuesProvider;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

/**
 * A {@link SequentialStoredFieldsLeafReader} that serves doc values for Parquet-resident fields from
 * the segment core's shared {@link ParquetDocValuesProducer}, delegating everything else to the
 * underlying leaf.
 *
 * <p>Lucene's {@code PerFieldDocValuesFormat} cannot route to a Parquet-resident field: it either has
 * no {@link FieldInfo} in the segment at all, or one that declares no doc values. The
 * {@link ParquetSegmentResources} for the core carries a synthesized {@code FieldInfo} (with the DV
 * type from {@link FieldTypeMapping}) for every such field and the combined {@link FieldInfos} this
 * reader presents; the accessor matching that DV type serves the field from the shared producer, the
 * others return null. All other fields pass through unchanged.
 *
 * <p>It extends {@link SequentialStoredFieldsLeafReader} (not plain {@code FilterLeafReader}) so the
 * fetch phase can still retrieve stored fields: the derived-source layer above unwraps to this reader,
 * which passes the underlying segment's stored-fields reader straight through.
 *
 * <p>The resources are shared across requests over the same segment core. This wrapper is request-scoped:
 * numeric cursors are recorded on the request-wide {@link CursorRegistry}, which the wrapping directory
 * reader closes when the request ends; a keyword or ip cursor is closed by the iterator that opened it.
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader implements PerDocumentValuesProvider {

    private final ParquetSegmentResources resources;
    private final CursorRegistry cursors;
    /** Null when no Parquet field of this segment is served as a stored field. */
    private final ParquetStoredFields storedFields;

    ParquetDocValuesLeafReader(LeafReader in, ParquetSegmentResources resources, CursorRegistry cursors) {
        super(in);
        this.resources = resources;
        this.cursors = cursors;
        this.storedFields = resources.storedFields.isEmpty() ? null : new ParquetStoredFields(resources, cursors);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return resources.combinedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        if (resources.parquetFieldInfo(field) != null) {
            // Synthesized Parquet fields are SORTED_NUMERIC; like CodecReader, an accessor whose DV
            // type does not match the FieldInfo returns null rather than serving the field.
            return null;
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = resources.parquetFieldInfo(field);
        if (fi != null) {
            if (fi.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
                // A Parquet-resident field of another DV type (keyword, ip, binary) is served by its own
                // accessor; a mismatched accessor returns null per the CodecReader contract.
                return null;
            }
            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...). The producer serves scalar columns as a singleton and LIST
            // columns as the multi-valued iterator (docId == Parquet row, asserted here); the cursor closes with the request.
            assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return resources.producer.getSortedNumeric(fi, cursors);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        FieldInfo fi = resources.parquetFieldInfo(field);
        if (fi != null) {
            if (fi.getDocValuesType() != DocValuesType.BINARY) {
                return null;
            }
            // A single-valued binary field: docId == Parquet row (asserted). The cursor is recorded on
            // this request's registry and closed when the request ends.
            assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return resources.producer.getBinary(fi, cursors);
        }
        return in.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        if (resources.parquetFieldInfo(field) != null) {
            // Keyword and ip are synthesized as SORTED_SET, so this accessor never matches; like
            // CodecReader, a non-matching accessor returns null rather than serving the field.
            return null;
        }
        return in.getSortedDocValues(field);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        FieldInfo fi = resources.parquetFieldInfo(field);
        if (fi != null) {
            if (fi.docValuesSkipIndexType() == DocValuesSkipIndexType.NONE) {
                return null;
            }
            assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return resources.producer.getSkipper(fi);
        }
        return in.getDocValuesSkipper(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return sortedSetDocValues(field, true);
    }

    /**
     * @param buildOrdinals false for source derivation, which reads one document at a time and needs no
     *                      whole-segment ordinals, so no .ord file is built for it.
     */
    private SortedSetDocValues sortedSetDocValues(String field, boolean buildOrdinals) throws IOException {
        FieldInfo fi = resources.parquetFieldInfo(field);
        if (fi != null) {
            if (fi.getDocValuesType() != DocValuesType.SORTED_SET) {
                return null;
            }
            if (resources.isMultiValued(field)) {
                if (buildOrdinals == false) {
                    // TODO: source derivation omits the field entirely, so a rebuilt document silently
                    // loses it; serve it properly once the Rust layer decodes repeated columns.
                    return null;
                }
                // Multi-valued keyword columns exist on disk (LIST promotion) but no repeated-column
                // reader is wired yet: refuse aggregations and sorts with a clear client error.
                throw new IllegalArgumentException(
                    "cannot aggregate or sort on multi-valued keyword field [" + field + "] on a pluggable data format index"
                );
            }
            assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            SortedDocValues plain = DocValues.unwrapSingleton(resources.producer.getSortedSet(fi));
            return DocValues.singleton(buildOrdinals ? withSegmentOrdinals(field, plain) : plain);
        }
        return in.getSortedSetDocValues(field);
    }

    @Override
    public LeafReader perDocumentValuesReader() {
        return new FilterLeafReader(this) {
            @Override
            public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
                return sortedSetDocValues(field, false);
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return ParquetDocValuesLeafReader.this.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return ParquetDocValuesLeafReader.this.getReaderCacheHelper();
            }
        };
    }

    /**
     * Layers cached segment ordinals over the streaming Parquet reader when available; falls back to
     * the streaming reader unchanged when no lease can be acquired.
     */
    private SortedDocValues withSegmentOrdinals(String field, SortedDocValues sorted) throws IOException {
        if (sorted instanceof ParquetSortedDocValues streaming) {
            long expectedNonNull = resources.producer.nonNullRowCount(resources.parquetFieldInfo(field));
            if (expectedNonNull == 0) {
                // No document in this segment has a value: empty doc values ARE the correct ordinals
                // view (zero terms) -- nothing to uninvert, and the streaming tier would fail the
                // aggregation at getValueCount.
                return DocValues.emptySorted();
            }
            UninvertedOrdinalsCache.Lease lease = UninvertedOrdinalsCache.acquire(in, resources.segmentInfo, field, expectedNonNull);
            if (lease != null) {
                ParquetUninvertedSortedDocValues withOrdinals = new ParquetUninvertedSortedDocValues(lease.ordinals(), streaming, maxDoc());
                UninvertedOrdinalsCache.releaseWhenUnreachable(withOrdinals, lease);
                return withOrdinals;
            }
        }
        return sorted;
    }

    @Override
    public StoredFields storedFields() throws IOException {
        StoredFields lucene = in.storedFields();
        if (storedFields == null) {
            return lucene;
        }
        assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet stored-fields read path";
        return storedFields.wrap(lucene);
    }

    @Override
    public StoredFieldsReader getSequentialStoredFieldsReader() throws IOException {
        assert storedFields == null || resources.assertRowIdsAreIdentity(in)
            : "non-identity __row_id__ segment reached the Parquet stored-fields read path";
        return super.getSequentialStoredFieldsReader();
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return storedFields == null ? reader : storedFields.wrap(reader);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
