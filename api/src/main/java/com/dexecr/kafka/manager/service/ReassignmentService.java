package com.dexecr.kafka.manager.service;

import java.util.List;
import java.util.Map;

import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.Reassignment;
import com.dexecr.kafka.manager.model.TopicReassignmentInfo;


public interface ReassignmentService {

    Mono<Void> addReassignment(Cluster cluster, Reassignment reassignment);

    Mono<List<TopicReassignmentInfo>> reassignmentStatus(Cluster cluster);

    Mono<Void> clearReassignment(Cluster cluster, Map<String, List<Integer>> topicPartitions);
}
