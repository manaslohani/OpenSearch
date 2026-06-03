/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Intercepts all {@code _search} requests and dispatches them to {@link DslExecuteAction}
 * for execution through the Calcite pipeline. Non-search actions pass through unchanged.
 */
public class SearchActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(SearchActionFilter.class);

    /**
     * Controls whether this filter intercepts {@code _search} and routes it through the Calcite/DSL
     * pipeline. Default {@code true} preserves the plugin's original intercept behavior; set to
     * {@code false} to let {@code _search} reach the standard aggregation path (and thus aggregation
     * delegation on composite-engine indexes).
     */
    public static final Setting<Boolean> INTERCEPT_SEARCH_ENABLED = Setting.boolSetting(
        "dsl.query_executor.intercept_search.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

    private final NodeClient client;
    private volatile boolean interceptEnabled;

    /**
     * Creates a filter that dispatches intercepted searches via the given client. The interception
     * gate is initialised from the cluster settings and kept in sync with dynamic updates.
     *
     * @param client         node client for dispatching to {@link DslExecuteAction}
     * @param clusterService cluster service used to read and observe {@link #INTERCEPT_SEARCH_ENABLED}
     */
    public SearchActionFilter(NodeClient client, ClusterService clusterService) {
        this.client = client;
        this.interceptEnabled = INTERCEPT_SEARCH_ENABLED.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INTERCEPT_SEARCH_ENABLED, value -> this.interceptEnabled = value);
    }

    @Override
    public int order() {
        return FILTER_ORDER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        // TODO: add support for other search-related APIs (_msearch, _count, _search_shards, etc.).
        // Consider two categories: APIs that execute search vs APIs that only explain/validate.
        if (interceptEnabled && SearchAction.NAME.equals(action)) {
            SearchRequest searchRequest = (SearchRequest) request;
            logger.info("[AGG_DELEGATION_TRACE] SearchActionFilter: intercepting _search → DSL pipeline");
            client.execute(DslExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
        } else {
            if (SearchAction.NAME.equals(action)) {
                logger.info(
                    "[AGG_DELEGATION_TRACE] SearchActionFilter: _search passing through to standard QueryPhase path (intercept disabled)"
                );
            }
            chain.proceed(task, action, request, listener);
        }
    }
}
