package com.dexecr.kafka.manager.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.dexecr.kafka.manager.exceptions.NotFoundException;
import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.model.Topic;
import com.dexecr.kafka.manager.model.TopicConfiguration;
import com.dexecr.kafka.manager.service.TopicService;
import com.dexecr.kafka.manager.util.TopicUtil;
import jakarta.inject.Singleton;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;

import lombok.RequiredArgsConstructor;

import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.CollectionUtils;
import reactor.core.publisher.Mono;


@RequiredArgsConstructor
@Singleton
public class TopicServiceImpl implements TopicService {

    private final KafkaClientProvider kafkaClientProvider;

    @Override
    public Mono<Set<String>> getTopicNames(Cluster cluster) {
        return kafkaClientProvider.getClient(cluster).listTopics().listings()
            .map(topics -> topics.stream().filter(Predicate.not(TopicListing::isInternal)).map(TopicListing::name)
                .collect(Collectors.toSet()));
    }

    @Override
    public Mono<Topic> getTopic(Cluster cluster, String name) {
        return getTopicDescriptions(cluster, name).map(descriptions -> {
            if (CollectionUtils.isEmpty(descriptions) || descriptions.size() > 1) {
                throw new NotFoundException("Topic %s is not found", name);
            }
            return descriptions.iterator().next();
        }).map(this::fromDescription);
    }

    @Override
    public Mono<Void> deleteTopic(Cluster cluster, String name) {
        return kafkaClientProvider.getClient(cluster).deleteTopics(List.of(Objects.requireNonNull(name))).all();
    }

    @Override
    public Mono<Void> createTopic(Cluster cluster, Topic topic) {
        NewTopic newTopic = new NewTopic(topic.getName(), Optional.ofNullable(topic.getNumPartitions()), Optional.ofNullable(topic.getReplicationFactor()));
        Optional.ofNullable(topic.getConfig()).ifPresent(topicConfiguration -> newTopic.configs(TopicConfiguration.toMap(topicConfiguration)));
        return kafkaClientProvider.getClient(cluster).createTopics(List.of(newTopic)).all();
    }

    @Override
    public Mono<TopicConfiguration> getTopicConfig(Cluster cluster, String topicName) {
        return getTopicConfigurationMap(cluster, topicName).map(TopicConfiguration::getTopicConfigurationFromMap);
    }

    @Override
    public Mono<Void> updateTopicConfig(Cluster cluster, String topicName, TopicConfiguration topicConfiguration) {
        var topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        return getTopicConfigurationMap(cluster, topicName).flatMap(oldTopicsConfigs -> {
            var currentTopicsConfig = TopicConfiguration.toMap(topicConfiguration);
            var changedTopicsOptions = currentTopicsConfig.keySet().stream().filter(oldTopicsConfigs::containsKey)
                    .filter(key -> !Objects.equals(oldTopicsConfigs.get(key), currentTopicsConfig.get(key)))
                    .map(configKey -> new AlterConfigOp(new ConfigEntry(configKey, currentTopicsConfig.get(configKey)), AlterConfigOp.OpType.SET))
                    .toList();
            return kafkaClientProvider.getClient(cluster).incrementalAlterConfigs(Map.of(topicConfigResource, changedTopicsOptions)).all();
        });
    }

    @Override
    public Mono<Void> increasePartitionsCount(Cluster cluster, String name, int newPartitionsCount) {
        var newPartitions = NewPartitions.increaseTo(newPartitionsCount);
        return kafkaClientProvider.getClient(cluster).createPartitions(Map.of(name, newPartitions)).all();
    }

    @Override
    public Mono<Collection<TopicDescription>> getTopicDescriptions(Cluster cluster, String... names) {
        return Mono.just(names).filter(ArrayUtils::isNotEmpty).map(Set::of).switchIfEmpty(getTopicNames(cluster))
            .flatMap(topics -> kafkaClientProvider.getClient(cluster).describeTopics(topics).all())
            .map(Map::values)
            .map(descriptions -> descriptions.stream().filter(Predicate.not(TopicDescription::isInternal)).collect(Collectors.toList()));
    }

    @Override
    public Mono<Map<String, String>> getTopicConfigTemplate() {
        return Mono.just(TopicConfiguration.getConfigDescription());
    }

    private Mono<Map<String, String>> getTopicConfigurationMap(Cluster cluster, String topicName) {
        return kafkaClientProvider.getClient(cluster)
                .describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topicName))).all()
                .map(Map::values)
                .map(configs -> configs.stream().map(Config::entries).flatMap(Collection::stream).collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));
    }

    private Topic fromDescription(TopicDescription description) {
        return Topic.builder().name(description.name()).numPartitions(description.partitions().size())
            .replicationFactor(TopicUtil.extractReplicationFactor(description)).build();
    }

}
