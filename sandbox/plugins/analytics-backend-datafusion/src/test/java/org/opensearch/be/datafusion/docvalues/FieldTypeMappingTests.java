/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Guards the codec's mapping-type allowlist.
 *
 * <p>The allowlist is what decides whether a field's doc values are served from Parquet at all, so a
 * type added here without a verified decode would silently return wrong values. These tests pin both
 * halves: the types that are readable, and the types that are not served.
 */
public class FieldTypeMappingTests extends OpenSearchTestCase {

    public void testSupportedNumericTypesResolveToSortedNumericDocValues() {
        for (String type : new String[] { "byte", "short", "integer", "long", "float", "double", "date", "date_nanos" }) {
            assertTrue(type + " must be supported", FieldTypeMapping.isSupported(type));
            assertEquals(type, DocValuesType.SORTED_NUMERIC, FieldTypeMapping.forType(type));
        }
    }

    /** Boolean is read through the bit-packed borrow path and stored as 0/1, like the numerics. */
    public void testBooleanIsSupportedAsSortedNumericDocValues() {
        assertTrue("boolean must be supported", FieldTypeMapping.isSupported("boolean"));
        assertEquals(DocValuesType.SORTED_NUMERIC, FieldTypeMapping.forType("boolean"));
    }

    /**
     * These three all resolve to sorted-numeric doc values, each for a different reason: unsigned_long
     * passes its raw 64-bit pattern through, scaled_float passes the already-scaled long that
     * ScaledFloatLeafFieldData later divides, and half_float is re-encoded to Lucene's sortable short.
     */
    public void testUnsignedLongScaledFloatAndHalfFloatAreSupported() {
        for (String type : new String[] { "unsigned_long", "scaled_float", "half_float" }) {
            assertTrue(type + " must be supported", FieldTypeMapping.isSupported(type));
            assertEquals(type, DocValuesType.SORTED_NUMERIC, FieldTypeMapping.forType(type));
        }
    }

    /**
     * token_count needs no entry of its own and must not gain one: its field type extends
     * NumberFieldType, so typeName() reports "integer" and the lookup resolves through that entry. A
     * "token_count" key would be dead code, because nothing ever looks the table up by that string.
     */
    public void testTokenCountResolvesThroughTheIntegerEntry() {
        assertFalse("a token_count key would never be looked up", FieldTypeMapping.isSupported("token_count"));
        assertTrue("the integer entry is what serves it", FieldTypeMapping.isSupported("integer"));
        FieldTypeMapping.validate("body_length", "integer", DocValuesType.SORTED_NUMERIC);
    }

    /**
     * Keyword and ip resolve to SORTED_SET because that is what KeywordFieldMapper and IpFieldMapper
     * index; a column holding one value per document is served as a singleton over it.
     */
    public void testKeywordAndIpResolveToSortedSetDocValues() {
        for (String type : new String[] { "keyword", "ip" }) {
            assertTrue(type + " must be supported", FieldTypeMapping.isSupported(type));
            assertEquals(type, DocValuesType.SORTED_SET, FieldTypeMapping.forType(type));
        }
        FieldTypeMapping.validate("city", "keyword", DocValuesType.SORTED_SET);
        FieldTypeMapping.validate("client_ip", "ip", DocValuesType.SORTED_SET);
    }

    /** {@code binary} is served through the variable-width borrow path as BINARY doc values. */
    public void testBinaryIsSupportedAsBinaryDocValues() {
        assertTrue("binary must be supported", FieldTypeMapping.isSupported("binary"));
        assertEquals(DocValuesType.BINARY, FieldTypeMapping.forType("binary"));
        FieldTypeMapping.validate("blob", "binary", DocValuesType.BINARY);
    }

    /**
     * Still deliberately out: text has no read path in the native cursor, so admitting it would fail at
     * read time.
     */
    public void testTypesWithoutAVerifiedDecodeAreNotSupported() {
        for (String type : new String[] { "text" }) {
            assertFalse(type + " must not be supported yet", FieldTypeMapping.isSupported(type));
            expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.forType(type));
        }
    }

    public void testAnUnknownTypeIsRejected() {
        assertFalse(FieldTypeMapping.isSupported("not_a_real_type"));
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.forType("not_a_real_type"));
    }

    /**
     * Every supported mapping type resolves to SORTED_NUMERIC, and the requested-type guard rejects
     * any other requested DocValues type for those mappings.
     */
    public void testEverySupportedTypeResolvesToSortedNumericAndRejectsOtherRequestedDvTypes() {
        for (String type : new String[] {
            "byte",
            "short",
            "integer",
            "long",
            "float",
            "double",
            "date",
            "date_nanos",
            "boolean",
            "unsigned_long",
            "scaled_float",
            "half_float" }) {
            assertEquals(type + " must resolve to SORTED_NUMERIC", DocValuesType.SORTED_NUMERIC, FieldTypeMapping.forType(type));
            // The requested-type guard admits only SORTED_NUMERIC; NUMERIC and every other DV type
            // must be rejected for an otherwise-supported mapping type.
            FieldTypeMapping.validate(type + "_field", type, DocValuesType.SORTED_NUMERIC);
            expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate(type + "_field", type, DocValuesType.NUMERIC));
            expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate(type + "_field", type, DocValuesType.SORTED_SET));
        }
    }

    /**
     * {@code validate} is the gate {@code ParquetDocValuesProducer.getSortedNumeric}
     * actually calls, once the field's {@code MappedFieldType} is known, so it is what keeps an
     * unsupported type from ever reaching the native cursor.
     */
    public void testValidateAcceptsSortedNumericForASupportedType() {
        FieldTypeMapping.validate("flag", "boolean", DocValuesType.SORTED_NUMERIC);
        FieldTypeMapping.validate("count", "long", DocValuesType.SORTED_NUMERIC);
    }

    public void testValidateRejectsAnUnsupportedMappingType() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FieldTypeMapping.validate("title", "text", DocValuesType.SORTED_SET)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("text"));
    }

    /** A supported mapping type still must not be served as a DocValues type it does not resolve to. */
    public void testValidateRejectsAMismatchedDocValuesType() {
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate("count", "long", DocValuesType.SORTED_SET));
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate("flag", "boolean", DocValuesType.BINARY));
        // The single-valued form is served as a SORTED_NUMERIC singleton, never as bare NUMERIC.
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate("count", "long", DocValuesType.NUMERIC));
        // Keyword resolves to SORTED_SET, so the bare SORTED form is not accepted for it either.
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate("city", "keyword", DocValuesType.SORTED));
    }
}
