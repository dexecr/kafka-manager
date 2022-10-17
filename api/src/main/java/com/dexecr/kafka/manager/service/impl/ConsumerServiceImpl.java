package com.dexecr.kafka.manager.service.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Singleton;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.ConsumerGroupInfo;
import com.dexecr.kafka.manager.model.ConsumerGroupMember;
import com.dexecr.kafka.manager.model.PartitionOffset;
import com.dexecr.kafka.manager.service.ConsumerService;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class ConsumerServiceImpl implements ConsumerService {

    private final KafkaClientProvider kafkaClientProvider;
    @Override
    public Mono<Set<String>> getConsumerGroups(Cluster cluster) {
        return kafkaClientProvider.getClient(cluster).listConsumerGroups().all()
            .map(groups -> groups.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet()));
    }

    @Override
    public Mono<ConsumerGroupInfo> getConsumerGroupInfo(Cluster cluster, String groupId) {
        return topicPartitionOffset(cluster, groupId)
            .flatMap(topicOffsets -> retrieveConsumerGroupEndOffsets(cluster, groupId, topicOffsets.keySet())
                    .map(topicEndOffsets -> mergeOffsets(topicOffsets, topicEndOffsets))
            )
            .flatMap(topicOffsets -> retrieveConsumerGroupDescription(cluster, groupId)
            .map(description -> toConsumerGroupInfo(description, topicOffsets)));
    }

    @Override
    public Mono<Void> alterConsumerGroupOffsets(Cluster cluster, String groupId, Map<String, Set<PartitionOffset>> topicPartitionsOffsetMap) {

        var changedOffsetMap = topicPartitionsOffsetMap.entrySet().stream().flatMap(m ->
            m.getValue().stream().collect(Collectors.toMap(e -> new TopicPartition(m.getKey(), e.getPartitionId()), PartitionOffset::getOffset)).entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return topicPartitionOffset(cluster, groupId)
            .map(entry -> entry.entrySet().stream().filter(mapEntry -> changedOffsetMap.containsKey(mapEntry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, partition -> new OffsetAndMetadata(changedOffsetMap.get(partition.getKey()), partition.getValue().leaderEpoch(), partition.getValue().metadata())))
            )
            .doOnNext(map -> kafkaClientProvider.getClient(cluster).alterConsumerGroupOffsets(groupId, map)).then();

    }

    @Override
    public Mono<Void> deleteConsumerGroup(Cluster cluster, String groupId) {
        return kafkaClientProvider.getClient(cluster).deleteConsumerGroups(List.of(groupId)).all();
    }

    @Override
    public Mono<Void> resetOffset(Cluster cluster, String groupId, Map<String, Set<Integer>> topicPartitions) {
        var topicPartitionsSet = topicPartitions.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(partitionId -> new TopicPartition(entry.getKey(), partitionId)))
            .collect(Collectors.toSet());
        return kafkaClientProvider.getClient(cluster).deleteConsumerGroupOffsets(groupId, topicPartitionsSet).all();
    }

    private KafkaConsumer<String, Serializable> createKafkaConsumer(Cluster cluster, String groupId) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getServers(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "true",
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, cluster.getRequestTimeout(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));
    }

    private Mono<ConsumerGroupDescription> retrieveConsumerGroupDescription(Cluster cluster, String groupId) {
        return kafkaClientProvider.getClient(cluster).describeConsumerGroups(List.of(groupId)).all()
                .map(consumerGroups -> consumerGroups.get(groupId));
    }


    private Mono<Map<TopicPartition, Long>> retrieveConsumerGroupEndOffsets(Cluster cluster, String groupId, Set<TopicPartition> topicPartitions) {
        var kafkaConsumer = createKafkaConsumer(cluster, groupId);
        return Mono.fromSupplier(() -> kafkaConsumer.endOffsets(topicPartitions)).doFinally(signal -> kafkaConsumer.close());
    }

    private ConsumerGroupInfo toConsumerGroupInfo(ConsumerGroupDescription description, Map<TopicPartition, OffsetInfo> offsetInfo) {
        var topicPartitions = offsetInfo.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> entry.getKey().topic(),
                Collectors.mapping(entry -> toPartitionOffset(entry.getKey().partition(), entry.getValue(), member(entry.getKey(), description.members())), Collectors.toList())));
        return ConsumerGroupInfo.builder().groupId(description.groupId()).state(String.valueOf(description.state()))
            .coordinatorId(description.coordinator().id()).topicPartitions(topicPartitions).build();
    }

    private Mono<Map<TopicPartition, OffsetAndMetadata>> topicPartitionOffset(Cluster cluster, String groupId) {
        return kafkaClientProvider.getClient(cluster).listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata();
    }

    private Map<TopicPartition, OffsetInfo> mergeOffsets(Map<TopicPartition, OffsetAndMetadata> topicOffsets, Map<TopicPartition, Long> topicEndOffsets) {
        return Stream.concat(topicOffsets.keySet().stream(), topicEndOffsets.keySet().stream()).distinct()
            .collect(Collectors.toMap(Function.identity(), topicPartition ->
                new OffsetInfo(Optional.ofNullable(topicOffsets.get(topicPartition)).map(OffsetAndMetadata::offset).orElse(null), topicEndOffsets.get(topicPartition))
            ));
    }

    private PartitionOffset toPartitionOffset(int partition, OffsetInfo offsetInfo, ConsumerGroupMember member) {
        offsetInfo = offsetInfo == null ? new OffsetInfo() : offsetInfo;
        return new PartitionOffset(partition, offsetInfo.offset, offsetInfo.endOffset, member);
    }

    private static ConsumerGroupMember member(TopicPartition topicPartition, Collection<MemberDescription> memberDescriptions) {
        return memberDescriptions.stream()
            .filter(memberDescription -> memberDescription.assignment().topicPartitions().stream().anyMatch(topicPartition::equals))
            .findFirst()
            .map(memberDescription -> ConsumerGroupMember.builder()
                .id(memberDescription.consumerId())
                .clientId(memberDescription.clientId())
                .host(memberDescription.host()).build())
            .orElse(null);
    }

    @AllArgsConstructor
    @NoArgsConstructor
    private static class OffsetInfo {
        private Long offset;
        private Long endOffset;
    }

}
