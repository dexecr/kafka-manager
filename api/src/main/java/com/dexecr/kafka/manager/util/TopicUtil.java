package com.dexecr.kafka.manager.util;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import static java.util.Optional.ofNullable;

public class TopicUtil {

    public static Predicate<TopicDescription> byName(String name) {
        return description -> description != null && Objects.equals(description.name(), name);
    }

    public static short extractReplicationFactor(TopicDescription topicDescription) {
        return (short) ofNullable(topicDescription).map(TopicDescription::partitions).stream()
            .flatMap(Collection::stream).filter(Objects::nonNull).map(TopicPartitionInfo::replicas).filter(Objects::nonNull)
            .map(List::size).mapToInt(Integer::intValue).max().orElse(0);
    }
}
