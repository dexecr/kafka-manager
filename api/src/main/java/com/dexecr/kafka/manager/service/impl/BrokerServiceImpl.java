package com.dexecr.kafka.manager.service.impl;

import java.util.Set;
import java.util.stream.Collectors;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.BrokerService;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import jakarta.inject.Singleton;

import org.apache.kafka.common.Node;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Broker;


@Slf4j
@Singleton
@RequiredArgsConstructor
public class BrokerServiceImpl implements BrokerService {

    private final KafkaClientProvider kafkaClientProvider;

    @Override
    public Mono<Set<Broker>> getBrokers(Cluster cluster) {
        return kafkaClientProvider.getClient(cluster).describeCluster().nodes()
                .map(brokers -> brokers.stream().map(this::toBroker).collect(Collectors.toSet()));
    }

    private Broker toBroker(Node node) {
        return Broker.builder().id(node.id()).host(node.host()).port(node.port()).build();
    }

}
