package com.dexecr.kafka.manager.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dexecr.kafka.manager.exceptions.NotFoundException;
import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.TopicConfiguration;
import com.dexecr.kafka.manager.util.ReassignmentUtil;
import org.apache.kafka.clients.admin.TopicDescription;

import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Topic;
import com.dexecr.kafka.manager.util.TopicUtil;

public interface TopicService {
    Mono<Set<String>> getTopicNames(Cluster cluster);

    Mono<Topic> getTopic(Cluster cluster, String name);

    Mono<Void> deleteTopic(Cluster cluster, String name);

    Mono<Void> createTopic(Cluster cluster, Topic topic);

    Mono<TopicConfiguration> getTopicConfig(Cluster cluster, String topicName);

    Mono<Void> updateTopicConfig(Cluster cluster, String topicName, TopicConfiguration configuration);

    Mono<Void> increasePartitionsCount(Cluster cluster, String name, int newPartitionsCount);

    default Mono<Map<Integer, List<Integer>>> getTopicAssignment(Cluster cluster, String name) {
        return getTopicDescriptions(cluster, name)
            .map(topicDescriptions -> topicDescriptions.stream().filter(TopicUtil.byName(name)).findFirst().orElseThrow(() -> new NotFoundException(name)))
            .map(ReassignmentUtil::extractAssignment);
    }

    Mono<Collection<TopicDescription>> getTopicDescriptions(Cluster cluster, String... names);

    Mono<Map<String, String>> getTopicConfigTemplate();
}
