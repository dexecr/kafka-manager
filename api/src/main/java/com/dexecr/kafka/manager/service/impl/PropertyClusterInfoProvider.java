package com.dexecr.kafka.manager.service.impl;

import com.dexecr.kafka.manager.exceptions.NotFoundException;
import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PropertyClusterInfoProvider implements ClusterInfoProvider {

    private final Map<String, Cluster> clusters;

    public PropertyClusterInfoProvider(List<Cluster> clusters) {
        this.clusters = clusters.stream().collect(Collectors.toUnmodifiableMap(Cluster::getName, Function.identity()));
    }

    @Override
    public Mono<Cluster> getCluster(String name) {
        return Optional.ofNullable(clusters.get(name)).map(Mono::just)
                .orElse(Mono.error(new NotFoundException("Cluster %s not found", name)));
    }

    @Override
    public Collection<Cluster> list() {
        return clusters.values();
    }
}
