/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetListColumnReader;

import java.nio.file.Path;
import java.util.List;

/**
 * Bookkeeping coverage for {@link CursorRegistry}: scalar and list cursors are tracked in separate
 * views, and {@link CursorRegistry#close()} releases and clears both.
 */
public class CursorRegistryTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";

    /** A registered list cursor is visible via listOpened() but not opened(), and close() releases and clears both views. */
    public void testListOpenedTracksListCursorsSeparatelyAndCloseClearsThem() throws Exception {
        Path file = createTempDir().resolve("registry.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, 8, -1);

        CursorRegistry registry = new CursorRegistry();
        ParquetColumnReader scalarCursor = ParquetColumnReader.open(file, COLUMN);
        ParquetListColumnReader listCursor = ParquetListColumnReader.open(file, COLUMN);
        registry.register(scalarCursor);
        registry.register(listCursor);

        assertEquals("listOpened() must expose only the list cursor", List.of(listCursor), registry.listOpened());
        assertEquals("opened() must expose only the scalar cursor", List.of(scalarCursor), registry.opened());
        assertFalse("scalar cursor must be open before close()", scalarCursor.isClosed());
        assertFalse("list cursor must be open before close()", listCursor.isClosed());

        registry.close();

        assertTrue("close() must release the scalar cursor", scalarCursor.isClosed());
        assertTrue("close() must release the list cursor", listCursor.isClosed());
        assertTrue("close() must clear the scalar view", registry.opened().isEmpty());
        assertTrue("close() must clear the list view", registry.listOpened().isEmpty());
    }
}
