package com.dexecr.kafka.manager.controller;

import com.dexecr.kafka.manager.model.Cluster;
import com.dexecr.kafka.manager.service.ClusterInfoProvider;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

import java.util.Collection;

@RequiredArgsConstructor
@Controller("/clusters")
public class ClusterController implements ClusterProviderController {

    @Getter
    private final ClusterInfoProvider clusterInfoProvider;

    @Get("/{name}")
    public Mono<Cluster> get(@PathVariable String name) {
        return withCluster(name, Mono::just);
    }

    @Get
    public Mono<Collection<Cluster>> list() {
        return Mono.just(clusterInfoProvider.list());
    }
}
