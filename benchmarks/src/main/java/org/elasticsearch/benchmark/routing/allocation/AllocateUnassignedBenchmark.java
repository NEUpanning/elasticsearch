/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.routing.allocation;


import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class AllocateUnassignedBenchmark {
    @Param({
        // indices| shards| replicas| nodes | unassigned indices
//        "       100|      5|        1|     20|      1",
//        "       3000|      5|        1|     50|     1",
//        "       3000|      10|        1|     100|   1"
        "       5000|      10|        1|     100|   1"
    })
    public String indicesShardsReplicasNodes = "3000|10|1|50|1";

    private AllocationService strategy;
    private ClusterState initialClusterState;
    private Random random = new Random();

    @Setup
    public void setUp() throws Exception {
        final String[] params = indicesShardsReplicasNodes.split("\\|");
        Settings settings = Settings.builder()
            // 关闭rebalance避免此处耗时
            .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            // 日志中心线上集群的配置
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 5)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        int numIndices = toInt(params[0]);
        int numShards = toInt(params[1]);
        int numReplicas = toInt(params[2]);
        int numNodes = toInt(params[3]);
        int numUnassignedIndices = toInt(params[4]);

        strategy = Allocators.createAllocationService(
            Settings.builder().build()
        );

        Metadata.Builder mb = Metadata.builder();
        for (int i = 1; i <= numIndices; i++) {
            addIndice(numShards, numReplicas, mb, i);
        }
        Metadata metadata = mb.build();
        RoutingTable.Builder rb = RoutingTable.builder();
        for (int i = 1; i <= numIndices; i++) {
            rb.addAsNew(metadata.index("test_" + i));
        }
        RoutingTable routingTable = rb.build();
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= numNodes; i++) {
            nb.add(Allocators.newNode("node" + i, Collections.emptyMap()));
        }
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(nb)
            .build();
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Arrays.asList(
                new SameShardAllocationDecider(settings, clusterSettings)
            )), new RoutingNodes(state, false), state, ClusterInfo.EMPTY, System.nanoTime());
        allocate(allocation, numNodes);
        state = strategy.buildResult(state, allocation);
        mb = Metadata.builder(state.metadata());
        for (int i = numIndices+1; i <= numIndices+numUnassignedIndices; i++) {
            addIndice(numShards, numReplicas, mb, i);
        }
        metadata = mb.build();
        rb = RoutingTable.builder(state.routingTable());
        for (int i = numIndices+1; i <= numIndices+numUnassignedIndices; i++) {
            rb.addAsNew(metadata.index("test_" + i));
        }
        routingTable = rb.build();
        initialClusterState = ClusterState.builder(state).metadata(metadata).routingTable(routingTable).build();
        initialClusterState = strategy.applyStartedShards(
            initialClusterState,
            initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING)
        );
    }

    private void addIndice(int numShards, int numReplicas, Metadata.Builder mb, int i) {
        mb.put(
            IndexMetadata.builder("test_" + i)
                .settings(Settings.builder().put("index.version.created", Version.CURRENT))
                .numberOfShards(numShards)
                .numberOfReplicas(numReplicas)
        );
    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }

    @Benchmark
    public ClusterState measureAllocation() {
        int i = 1;
        ClusterState clusterState = initialClusterState;
        while (clusterState.getRoutingNodes().hasUnassignedShards()) {
            clusterState = strategy.applyStartedShards(
                clusterState,
                clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING)
            );
            clusterState = strategy.reroute(clusterState, "reroute");
        }
        return clusterState;
    }

    public static void main(String[] args) throws Exception {
        final AllocateUnassignedBenchmark allocationBenchmark = new AllocateUnassignedBenchmark();
        allocationBenchmark.setUp();
        final long start = System.nanoTime();
        allocationBenchmark.measureAllocation();
        final long end = System.nanoTime();
        System.out.println("cost time :" + (end - start) / 1000000 + "ms");
    }

    public void allocate(RoutingAllocation allocation, int nodeNum) {
        assert nodeNum > 2;
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        ShardRouting[] drain = unassigned.drain();
        ArrayUtil.timSort(drain, (a, b) -> {
            if (a.primary() ^ b.primary()) {
                return a.primary() ? -1 : 1;
            } else {
                int indexCmp;
                if ((indexCmp = a.getIndexName().compareTo(b.getIndexName())) == 0) {
                    return Integer.compare(a.id(), b.id());
                } else {
                    return indexCmp;
                }
            }
        }); // we have to allocate primaries first
        int primary_node = 1;
        int replica_node = 2;
        for (ShardRouting sr : drain) {
            if (primary_node > nodeNum) {
                primary_node = 1;
            }
            if (replica_node > nodeNum) {
                replica_node = 1;
            }
            if (sr.primary()) {
                allocation.routingNodes().initializeShard(sr, "node" + primary_node++, null, -1, allocation.changes());
            } else {
                allocation.routingNodes().initializeShard(sr, "node" + replica_node++, null, -1, allocation.changes());
            }
        }
    }
}
