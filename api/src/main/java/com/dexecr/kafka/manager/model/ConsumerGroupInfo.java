package com.dexecr.kafka.manager.model;

import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import io.micronaut.core.annotation.Introspected;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor
@Builder
@Introspected
public class ConsumerGroupInfo {
    private String groupId;
    private Integer coordinatorId;
    private String state;
    private Map<String, List<PartitionOffset>> topicPartitions;
}
