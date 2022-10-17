package com.dexecr.kafka.manager.controller;

import java.util.Map;
import java.util.Set;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.ConsumerGroupInfo;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Put;
import io.micronaut.http.annotation.QueryValue;
import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.PartitionOffset;
import com.dexecr.kafka.manager.service.ConsumerService;


@RequiredArgsConstructor
@Controller("/consumers")
public class ConsumerController implements ClusterProviderController {
    private final ConsumerService consumerService;
    @Getter
    private final ClusterInfoProvider clusterInfoProvider;

    @Get("{?cluster}")
    public Mono<Set<String>> getConsumerGroups(@QueryValue String cluster) {
        return withCluster(cluster, consumerService::getConsumerGroups);
    }

    @Get("/{groupId}{?cluster}")
    public Mono<ConsumerGroupInfo> getConsumerGroupInfo(@QueryValue String cluster, @PathVariable String groupId) {
        return withCluster(cluster, c -> consumerService.getConsumerGroupInfo(c, groupId));
    }

    @Delete("/{groupId}{?cluster}")
    public Mono<Void> deleteConsumerGroup(@QueryValue String cluster, @PathVariable String groupId) {
        return withCluster(cluster, c -> consumerService.deleteConsumerGroup(c, groupId));
    }

    @Put("/{groupId}/offset/reset{?cluster}")
    public Mono<Void> resetOffset(@QueryValue String cluster, @PathVariable String groupId, @Body Map<String, Set<Integer>> topicPartitions) {
        return withCluster(cluster, c -> consumerService.resetOffset(c, groupId, topicPartitions));
    }

    @Put("/{groupId}/offset/alter{?cluster}")
    public Mono<Void> alterConsumerGroupOffsets(@QueryValue String cluster, @PathVariable String groupId, @Body Map<String, Set<PartitionOffset>> topicPartitionsOffset) {
        return withCluster(cluster, c -> consumerService.alterConsumerGroupOffsets(c, groupId, topicPartitionsOffset));
    }
}
