/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Segment-level codec wiring for the composite engine. Routes per-field doc values between the
 * Parquet-backed codec and Lucene's native format via {@code PerFieldDocValuesFormat}.
 */
package org.opensearch.composite.codec;
