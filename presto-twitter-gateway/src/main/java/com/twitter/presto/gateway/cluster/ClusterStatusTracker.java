/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.presto.gateway.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ClusterStatusTracker
{
    private static final Logger log = Logger.get(ClusterStatusTracker.class);
    private static final String QUERY_INFO = "/v1/query";
    private static final String CLUSTER_INFO = "/v1/cluster";

    private final ClusterManager clusterManager;
    private final HttpClient httpClient;
    private final ScheduledExecutorService queryInfoUpdateExecutor;

    // Cluster status
    private final ConcurrentHashMap<URI, RemoteClusterInfo> remoteClusterInfos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, RemoteQueryInfo> remoteQueryInfos = new ConcurrentHashMap<>();

    @Inject
    public ClusterStatusTracker(
            ClusterManager clusterManager,
            @ForQueryTracker HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
        this.queryInfoUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("query-info-poller-%s"));
    }

    @PostConstruct
    public void startPollingQueryInfo()
    {
        clusterManager.getAllClusters().stream()
                .forEach(uri -> {
                    remoteClusterInfos.put(uri, createRemoteClusterInfo(uri));
                    remoteQueryInfos.put(uri, createRemoteQueryInfo(uri));
                });

        queryInfoUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollQueryInfos();
            }
            catch (Exception e) {
                log.error(e, "Error polling list of queries");
            }
        }, 5, 5, TimeUnit.SECONDS);

        pollQueryInfos();
    }

    private void pollQueryInfos()
    {
        Set<URI> allClusters = newHashSet(clusterManager.getAllClusters());
        Set<URI> inactiveClusters = difference(remoteQueryInfos.keySet(), allClusters).immutableCopy();
        remoteQueryInfos.keySet().removeAll(inactiveClusters);

        allClusters.stream()
                .forEach(uri -> {
                    remoteClusterInfos.putIfAbsent(uri, createRemoteClusterInfo(uri));
                    remoteQueryInfos.putIfAbsent(uri, createRemoteQueryInfo(uri));
                });

        remoteClusterInfos.values().forEach(RemoteClusterInfo::asyncRefresh);
        remoteQueryInfos.values().forEach(RemoteQueryInfo::asyncRefresh);
    }

    public long getRunningQueries()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getRunningQueries)
                .sum();
    }

    public long getBlockedQueries()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getBlockedQueries)
                .sum();
    }

    public long getQueuedQueries()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getQueuedQueries)
                .sum();
    }

    public long getClusterCount()
    {
        return remoteClusterInfos.entrySet().size();
    }

    public long getActiveWorkers()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getActiveWorkers)
                .sum();
    }

    public long getRunningDrivers()
    {
        return remoteClusterInfos.values().stream()
                .mapToLong(RemoteClusterInfo::getRunningDrivers)
                .sum();
    }

    public List<JsonNode> getAllQueryInfos()
    {
        ImmutableList.Builder<JsonNode> builder = ImmutableList.builder();
        remoteQueryInfos.forEach((coordinator, remoteQueryInfo) ->
                builder.addAll(remoteQueryInfo.getQueryList().orElse(ImmutableList.of()).stream()
                        .map(queryInfo -> ((ObjectNode) queryInfo).put("coordinatorUri", coordinator.toASCIIString()))
                        .collect(toImmutableList())));
        return builder.build();
    }

    private RemoteQueryInfo createRemoteQueryInfo(URI uri)
    {
        return new RemoteQueryInfo(httpClient, uriBuilderFrom(uri).appendPath(QUERY_INFO).build());
    }

    private RemoteClusterInfo createRemoteClusterInfo(URI uri)
    {
        return new RemoteClusterInfo(httpClient, uriBuilderFrom(uri).appendPath(CLUSTER_INFO).build());
    }
}
