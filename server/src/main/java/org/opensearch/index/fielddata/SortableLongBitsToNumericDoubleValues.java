/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * {@link NumericDoubleValues} instance that wraps a {@link NumericDocValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#sortableLongToDouble(long)}.
 *
 * @opensearch.internal
 */
final class SortableLongBitsToNumericDoubleValues extends NumericDoubleValues implements BulkDoubleValues {

    private final NumericDocValues values;
    /** Non-null when the wrapped values support a contiguous raw-long bulk read; enables fillDoubles. */
    private final BulkLongDocValues bulkLongs;
    private long[] longScratch = new long[0];

    SortableLongBitsToNumericDoubleValues(NumericDocValues values) {
        this.values = values;
        this.bulkLongs = values instanceof BulkLongDocValues ? (BulkLongDocValues) values : null;
    }

    @Override
    public double doubleValue() throws IOException {
        return NumericUtils.sortableLongToDouble(values.longValue());
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        return values.advanceExact(doc);
    }

    /** Return the wrapped values. */
    public NumericDocValues getLongValues() {
        return values;
    }

    @Override
    public int fillDoubles(int minDoc, int maxExclusive, double[] dst) throws IOException {
        if (bulkLongs == null) {
            return 0;
        }
        if (longScratch.length < dst.length) {
            longScratch = new long[dst.length];
        }
        int n = bulkLongs.fillLongs(minDoc, maxExclusive, longScratch);
        for (int i = 0; i < n; i++) {
            dst[i] = NumericUtils.sortableLongToDouble(longScratch[i]);
        }
        return n;
    }

    @Override
    public int advance(int target) throws IOException {
        return values.advance(target);
    }
}
