package com.dexecr.kafka.manager.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.dexecr.kafka.manager.exceptions.InvalidAssignmentException;
import com.dexecr.kafka.manager.model.Broker;
import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.Reassignment;
import com.dexecr.kafka.manager.model.TopicReassignmentInfo;
import com.dexecr.kafka.manager.service.BrokerService;
import com.dexecr.kafka.manager.service.ReassignmentService;
import com.dexecr.kafka.manager.service.TopicService;
import com.dexecr.kafka.manager.util.ReassignmentUtil;
import com.dexecr.kafka.manager.util.TopicUtil;
import jakarta.inject.Singleton;

import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import io.micronaut.core.util.CollectionUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class ReassignmentServiceImpl implements ReassignmentService {

    private final KafkaClientProvider kafkaClientProvider;
    private final TopicService topicService;
    private final BrokerService brokerService;

    @Override
    public Mono<Void> addReassignment(Cluster cluster, Reassignment reassignment) {
        return getTopicsForReassignment(cluster, reassignment)
            .zipWith(getBrokersForReassignment(cluster, reassignment))
            .publishOn(Schedulers.single())
            .map(topicsAndBrokers -> getReassignmentsForTopicPartition(topicsAndBrokers.getT1(), topicsAndBrokers.getT2()))
            .flatMap(reassignments -> kafkaClientProvider.getClient(cluster).alterPartitionReassignments(reassignments).all());
    }

    @Override
    public Mono<List<TopicReassignmentInfo>> reassignmentStatus(Cluster cluster) {
        return kafkaClientProvider.getClient(cluster).listPartitionReassignments().reassignments().map(Map::entrySet)
            .map(entries -> entries.stream().collect(Collectors.groupingBy(entry -> entry.getKey().topic())).entrySet().stream()
                .map(entry -> new TopicReassignmentInfo(entry.getKey(), entry.getValue().stream()
                    .collect(Collectors.toMap(e -> e.getKey().partition(), e -> new HashSet<>(e.getValue().replicas()))))
                ).collect(Collectors.toList())
            );
    }

    @Override
    public Mono<Void> clearReassignment(Cluster cluster, Map<String, List<Integer>> topicPartitions) {
        var reassignments = topicPartitions.entrySet().stream().flatMap(entrySet -> getTopicPartitionsFromAssignment(entrySet)
            .stream()).collect(Collectors.toMap(Function.identity(), e -> Optional.<NewPartitionReassignment>empty()));
        return kafkaClientProvider.getClient(cluster).alterPartitionReassignments(reassignments).all()
            .onErrorResume(NoReassignmentInProgressException.class, error -> {
                log.error("Reassignment for one partition already has finished, {}", error.getMessage());
                return Mono.empty();
            });
    }

    private List<TopicPartition> getTopicPartitionsFromAssignment(Map.Entry<String, List<Integer>> assignmentEntry) {
        return assignmentEntry.getValue().stream().map(brokerId -> new TopicPartition(assignmentEntry.getKey(), brokerId))
            .collect(Collectors.toList());
    }

    private Map<TopicPartition, Optional<NewPartitionReassignment>> getReassignmentsForTopicPartition(Map<TopicDescription, Short> topicDescriptions, List<Integer> brokers) {
        return topicDescriptions.keySet().stream()
            .flatMap(description -> toReassignment(description, brokers, topicDescriptions.get(description)).entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<TopicPartition, Optional<NewPartitionReassignment>> toReassignment(TopicDescription description, List<Integer> brokers, Short baseRf) {
        var assignment = ReassignmentUtil.extractAssignment(description);
        return assignReplicasToBrokers(assignment, brokers, calculateReplicationFactor(assignment, baseRf))
            .entrySet().stream()
            .collect(Collectors.toMap(entry -> new TopicPartition(description.name(), entry.getKey()),
                entry -> Optional.of(new NewPartitionReassignment(entry.getValue()))));
    }


    private short calculateReplicationFactor(Map<Integer, List<Integer>> assignment, Short baseRF) {
        return Optional.ofNullable(baseRF).orElseGet(() -> assignment
            .values()
            .stream()
            .max(Comparator.comparingInt(List::size))
            .map(Collection::size)
            .map(Integer::shortValue)
            .orElse((short) 0));
    }

    private Mono<Map<TopicDescription, Short>> getTopicsForReassignment(Cluster cluster, Reassignment reassignment) {
        var topicDescriptions = topicService.getTopicDescriptions(cluster);
        if (CollectionUtils.isEmpty(reassignment.getTopicsAndReplication())) {
            return topicDescriptions.map(descriptions -> descriptions.stream()
                .collect(Collectors.toMap(Function.identity(), TopicUtil::extractReplicationFactor)));
        }
        var topicsForReassignment = reassignment.getTopicsAndReplication();
        return topicDescriptions.map(descriptions -> {
            var topicNames = descriptions.stream().map(TopicDescription::name).collect(Collectors.toSet());
            for (var topic : topicsForReassignment.keySet()) {
                if (!topicNames.contains(topic)) {
                    throw new InvalidAssignmentException(String.format("Invalid assignment. Topic (%s) is not found", topic));
                }
            }
            return descriptions.stream().collect(Collectors.toMap(Function.identity(), description -> topicsForReassignment.get(description.name())));
        });
    }

    private Mono<List<Integer>> getBrokersForReassignment(Cluster cluster, Reassignment reassignment) {
        var allBrokers = brokerService.getBrokers(cluster).map(brokers -> brokers.stream().map(Broker::getId).collect(Collectors.toList()))
            .flatMap(brokers -> {
                if (CollectionUtils.isEmpty(brokers))
                    return Mono.error(new InvalidAssignmentException("Brokers are unavailable"));
                Collections.sort(brokers);
                return Mono.just(brokers);
            });
        if (CollectionUtils.isEmpty(reassignment.getBrokers())) {
            return allBrokers;
        }
        return allBrokers.flatMap(brokers -> {
            for (var broker : reassignment.getBrokers()) {
                if (!brokers.contains(broker)) {
                    Mono.error(new InvalidAssignmentException(String.format("Invalid assignment. Broker (%s) not found", broker)));
                }
            }
            return Mono.just(reassignment.getBrokers().stream().sorted().toList());
        });
    }

    private Map<Integer, List<Integer>> assignReplicasToBrokers(Map<Integer, List<Integer>> topicAssignment,
                                                                List<Integer> brokers, int replicationFactor) {
        Map<Integer, List<Integer>> newPartitionAssignment = new HashMap<>();
        Random rand = ThreadLocalRandom.current();
        int startIndex = rand.nextInt(brokers.size());
        int nextReplicaShift = rand.nextInt(brokers.size());
        var replicaPartitionList = new ArrayList<>(topicAssignment.keySet());
        for (int currentPartitionId : replicaPartitionList) {
            if (currentPartitionId > 0 && (currentPartitionId % brokers.size() == 0))
                nextReplicaShift++;
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokers.size();
            List<Integer> replicaList = new ArrayList<>();
            replicaList.add(brokers.get(firstReplicaIndex));
            for (int j = 0; j < replicationFactor - 1; j++)
                replicaList.add(brokers.get(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokers.size())));
            newPartitionAssignment.put(currentPartitionId, replicaList);
        }
        return newPartitionAssignment;
    }

    private static int replicaIndex(int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int brokersCount) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (brokersCount - 1);
        return (firstReplicaIndex + shift) % brokersCount;
    }
}
