package com.dexecr.kafka.manager.service;

import com.dexecr.kafka.manager.model.Cluster;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface ClusterInfoProvider {

    Mono<Cluster> getCluster(String name);

    Collection<Cluster> list();
}
