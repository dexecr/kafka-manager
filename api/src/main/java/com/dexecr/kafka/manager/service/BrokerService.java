package com.dexecr.kafka.manager.service;

import java.util.Set;

import reactor.core.publisher.Mono;
import com.dexecr.kafka.manager.model.Broker;
import com.dexecr.kafka.manager.model.Cluster;

public interface BrokerService {

    /**
     * Get all broker configs
     *
     * @return broker configs
     */
    Mono<Set<Broker>> getBrokers(Cluster cluster);

}
