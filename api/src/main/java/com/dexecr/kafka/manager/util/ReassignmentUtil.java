package com.dexecr.kafka.manager.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

public class ReassignmentUtil {

    public static Map<Integer, List<Integer>> extractAssignment(TopicDescription description) {
        return description.partitions().stream()
            .collect(Collectors.toMap(TopicPartitionInfo::partition, partition -> partition.replicas().stream().map(Node::id).collect(Collectors.toList())));
    }
}
