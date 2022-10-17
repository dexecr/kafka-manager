package com.dexecr.kafka.manager.service;


import java.util.Map;
import java.util.Set;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.ConsumerGroupInfo;
import com.dexecr.kafka.manager.model.PartitionOffset;
import reactor.core.publisher.Mono;

public interface ConsumerService {

    Mono<Set<String>> getConsumerGroups(Cluster cluster);

    Mono<ConsumerGroupInfo> getConsumerGroupInfo(Cluster cluster, String groupId);

    Mono<Void> deleteConsumerGroup(Cluster cluster, String groupId);

    Mono<Void> resetOffset(Cluster cluster, String groupId, Map<String, Set<Integer>> topicPartitions);

    Mono<Void> alterConsumerGroupOffsets(Cluster cluster, String groupId, Map<String, Set<PartitionOffset>> topicPartitionsOffsetMap);
}
