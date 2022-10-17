package com.dexecr.kafka.manager.controller;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import io.micronaut.core.bind.annotation.Bindable;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface ClusterProviderController {

    @Bindable
    ClusterInfoProvider getClusterInfoProvider();

    default <T> Mono<T> withCluster(String name, Function<Cluster, Mono<T>> cluster) {
        return getClusterInfoProvider().getCluster(name).flatMap(cluster);
    }
}
