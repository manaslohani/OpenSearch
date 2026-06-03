/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class SearchActionFilterTests extends OpenSearchTestCase {

    private final NodeClient client = mock(NodeClient.class);
    private final Task task = mock(Task.class);
    private final ActionListener<ActionResponse> listener = mock(ActionListener.class);
    private final ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
    private final ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);

    /**
     * Builds a filter whose interception gate is initialised from a cluster service backed by the
     * given setting value.
     */
    private SearchActionFilter newFilter(boolean interceptEnabled) {
        Settings settings = Settings.builder()
            .put(SearchActionFilter.INTERCEPT_SEARCH_ENABLED.getKey(), interceptEnabled)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(SearchActionFilter.INTERCEPT_SEARCH_ENABLED));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new SearchActionFilter(client, clusterService);
    }

    public void testOrderRunsAfterSecurityFilter() {
        assertEquals(SearchActionFilter.FILTER_ORDER, newFilter(true).order());
    }

    public void testPassesThroughNonSearchAction() {
        BulkRequest request = new BulkRequest();

        newFilter(true).apply(task, BulkAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testReroutesSearchActionWhenInterceptEnabled() {
        SearchRequest request = new SearchRequest("test-index");

        newFilter(true).apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    public void testPassesThroughSearchActionWhenInterceptDisabled() {
        SearchRequest request = new SearchRequest("test-index");

        newFilter(false).apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }
}
