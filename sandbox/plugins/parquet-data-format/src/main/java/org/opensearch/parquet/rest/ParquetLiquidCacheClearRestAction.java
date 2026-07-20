/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.rest;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST /_plugins/parquet/liquid_cache/_clear}.
 *
 * <p>Clears the codec-owned liquid decoded-page cache (in-memory index + spilled {@code t4}
 * entries) on the coordinating node, without disabling it — the next reads re-decode and
 * re-populate. Intended for cold-start benchmarking so a "cold" measurement no longer requires a
 * full node restart. A no-op when the cache is disabled or not yet built.
 *
 * <p>Node-local: the clear runs in-process against this node's cache static. On a multi-node
 * cluster, hit each node (or route via {@code _nodes}) to clear all — for the single-node bench
 * box this handler is sufficient.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetLiquidCacheClearRestAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "parquet_liquid_cache_clear_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_plugins/parquet/liquid_cache/_clear"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> {
            RustBridge.liquidCacheClear();
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("acknowledged", true);
                builder.field("cleared", "parquet_liquid_cache");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
